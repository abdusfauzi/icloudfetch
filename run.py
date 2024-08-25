from pyicloud import PyiCloudService
import os
import platform
import subprocess
import hashlib
import time
import keyring
import logging
import pytz
from tqdm import tqdm
from datetime import datetime, timezone

# Set up logging without milliseconds
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Function to calculate file hash
def calculate_file_hash(file_path, chunk_size=1024 * 1024):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as file:
        while chunk := file.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

# Function to download and save a file with progress bar and calculate hash
def download_file_with_progress(photo, file_path, chunk_size=1024 * 1024):
    total_size = photo.size  # Total size of the file in bytes
    hasher = hashlib.md5()

    with open(file_path, 'wb') as file, tqdm(
        total=total_size, unit='B', unit_scale=True, desc=file_path
    ) as pbar:
        for chunk in photo.download().iter_content(chunk_size):
            if chunk:  # filter out keep-alive new chunks
                file.write(chunk)
                hasher.update(chunk)
                pbar.update(len(chunk))

    return hasher.hexdigest()

def reset_file_timestamp(file_path, timestamp, timezone='UTC'):
    try:
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = pytz.UTC.localize(timestamp)
        
        # Convert to the desired timezone
        local_tz = pytz.timezone(timezone)
        local_timestamp = timestamp.astimezone(local_tz)

        mod_time = local_timestamp.timestamp()  # Convert to Unix timestamp
        os.utime(file_path, (mod_time, mod_time))  # Set access and modified times

        # Set creation time (birth time)
        if platform.system() == 'Darwin':  # macOS
            # Format the date string as required by SetFile, in the local timezone
            date_string = local_timestamp.strftime("%m/%d/%Y %H:%M:%S")
            
            # Set TZ environment variable for subprocess
            env = os.environ.copy()
            env['TZ'] = timezone

            subprocess.run(['SetFile', '-d', date_string, file_path], check=True, env=env)
        elif platform.system() == 'Windows':
            from win32_setctime import setctime
            setctime(file_path, mod_time)

        # logging.info(f"Set all timestamps for {file_path} to {local_timestamp} ({timezone})")
    except Exception as e:
        logging.error(f"Failed to set timestamps for {file_path}: {e}")

# Rate limiting variables
RATE_LIMIT = 100  # Maximum number of requests
TIME_WINDOW = 0  # Time window in seconds

# Exponential backoff settings
MAX_RETRIES = 5
INITIAL_WAIT = 1  # Initial wait time in seconds
MAX_WAIT = 60  # Maximum wait time in seconds

# File to track last downloaded photo
TRACK_FILE = "last_downloaded.txt"

# Load or create iCloud session
def authenticate():
    try:
        session_dir = os.path.expanduser("~/.icloud-session")
        if not os.path.exists(session_dir):
            os.makedirs(session_dir)
        
        apple_id = input(f"Enter the email for Apple ID: ")
        
        # Retrieve password securely using keyring
        # keyring.delete_password("icloud", apple_id)
        password = keyring.get_password("icloud", apple_id)
        
        if not password:
            password = input(f"Enter the password for {apple_id}: ")
            keyring.set_password("icloud", apple_id, password)

        icloud = PyiCloudService(apple_id, password, cookie_directory=session_dir)

        logging.info("Checking for 2FA")
        
        if icloud.requires_2fa:
            logging.info("Two-factor authentication required.")
            code = input("Enter the code you received on one of your approved devices: ")
            result = icloud.validate_2fa_code(code)
            if result:
                logging.info("Two-factor authentication successful.")
            else:
                logging.error("Failed to verify two-factor authentication code.")
                exit()

        logging.info("Checking for Trusted Session")

        if icloud.is_trusted_session:
            logging.info("Successfully authenticated.")
        else:
            logging.error("Failed to authenticate. Check your credentials and try again.")
            exit()

        return icloud

    except Exception as e:
        logging.error(f"An error occurred during authentication: {e}")
        exit()

def exponential_backoff(attempt):
    return min(INITIAL_WAIT * (2 ** attempt), MAX_WAIT)

def load_last_downloaded():
    if os.path.exists(TRACK_FILE):
        with open(TRACK_FILE, 'r') as f:
            return f.read().strip()
    return None

def save_last_downloaded(photo_name):
    with open(TRACK_FILE, 'w') as f:
        f.write(photo_name)

# Get the date range from the user (optional)
# start_date_str = input("Enter the start date (YYYY-MM-DD) or leave blank for no start date: ")
# end_date_str = input("Enter the end date (YYYY-MM-DD) or leave blank for no end date: ")

# Parse the dates if provided and make them timezone-aware
# start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) if start_date_str else None
# end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date_str else None

icloud = authenticate()

# Set the directory where you want to download the photos/videos
# download_directory = 'icloud_photos'
download_directory = 'downloaded_files'
os.makedirs(download_directory, exist_ok=True)

# Filter photos based on the provided date range, or get all photos
# if start_date and end_date:
#     photos = [photo for photo in icloud.photos.all if start_date <= photo.added_date <= end_date]
#     logging.info(f"Found {len(photos)} items in iCloud within the date range.")
# elif start_date:
#     photos = [photo for photo in icloud.photos.all if photo.added_date >= start_date]
#     logging.info(f"Found {len(photos)} items in iCloud from {start_date_str} onwards.")
# elif end_date:
#     photos = [photo for photo in icloud.photos.all if photo.added_date <= end_date]
#     logging.info(f"Found {len(photos)} items in iCloud up to {end_date_str}.")
# else:
photos = icloud.photos.all
logging.info(f"Found {len(photos)} items in iCloud.")

# Load the last downloaded photo
last_downloaded = load_last_downloaded()
skip = last_downloaded is not None

requests_made = 0
for photo in photos:
    if skip:
        if photo.filename == last_downloaded:
            skip = False
        continue

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            photo_name = photo.filename
            photo_path = os.path.join(download_directory, photo_name)

            # Log the file size
            # logging.info(f"Processing {photo_name} ({photo.size / (1024 * 1024):.2f} MB)")

            # Check if the file already exists and is identical
            if os.path.exists(photo_path):
                local_file_hash = calculate_file_hash(photo_path)
                temp_file_path = photo_path + ".temp"
                icloud_file_hash = download_file_with_progress(photo, temp_file_path)
                
                if local_file_hash == icloud_file_hash:
                    logging.info(f"{photo_name} is already up-to-date, skipping download.")
                    # Reset the file's created/modified timestamps to the original photo timestamp
                    reset_file_timestamp(photo_path, photo.added_date)
                    os.remove(temp_file_path)
                    break
                else:
                    logging.info(f"{photo_name} exists but differs from the iCloud version. Updating file.")
                    os.rename(temp_file_path, photo_path)
            else:
                # Download the file with progress bar
                # logging.info(f"Downloading {photo_name} ({photo.asset_date})...")
                download_file_with_progress(photo, photo_path)
                # logging.info(f"Downloaded {photo_name} to {photo_path}.")

            # Reset the file's created/modified timestamps to the original photo timestamp
            reset_file_timestamp(photo_path, photo.added_date)

            # Save the last downloaded photo name
            save_last_downloaded(photo_name)

            requests_made += 1

            # Rate limiting logic
            if requests_made >= RATE_LIMIT:
                if TIME_WINDOW > 0:
                    logging.info(f"Rate limit reached. Sleeping for {TIME_WINDOW} seconds...")
                    time.sleep(TIME_WINDOW)
                requests_made = 0

            break  # Break out of the retry loop if successful

        except Exception as e:
            logging.error(f"An error occurred while processing {photo_name}: {e}")
            attempt += 1
            if attempt < MAX_RETRIES:
                wait_time = exponential_backoff(attempt)
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Max retries reached for {photo_name}. Skipping file.")
                break

logging.info("Sync process complete.")