from pyicloud import PyiCloudService
import os
import hashlib
import time
import keyring
import logging
from tqdm import tqdm
from datetime import datetime

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

# Function to reset file's created and modified timestamps
def reset_file_timestamp(file_path, timestamp):
    try:
        mod_time = timestamp.timestamp()  # Convert to Unix timestamp
        os.utime(file_path, (mod_time, mod_time))  # Set both access and modified times
        # logging.info(f"Set file timestamp for {file_path} to {timestamp}")
    except Exception as e:
        logging.error(f"Failed to set file timestamp for {file_path}: {e}")

# Rate limiting variables
RATE_LIMIT = 100  # Maximum number of requests
TIME_WINDOW = 0  # Time window in seconds

# Exponential backoff settings
MAX_RETRIES = 5
INITIAL_WAIT = 1  # Initial wait time in seconds
MAX_WAIT = 60  # Maximum wait time in seconds

# Load or create iCloud session
def authenticate():
    try:
        session_dir = os.path.expanduser("~/.icloud-session")
        if not os.path.exists(session_dir):
            os.makedirs(session_dir)
        
        apple_id = input(f"Enter the email for Apple ID: ")

        # Retrieve password securely using keyring
        password = keyring.get_password("icloud", apple_id)

        if not password:
            password = input(f"Enter the password for {apple_id}: ")
            keyring.set_password("icloud", apple_id, password)

        icloud = PyiCloudService(apple_id, password, cookie_directory=session_dir)
        
        if icloud.requires_2fa:
            logging.info("Two-factor authentication required.")
            code = input("Enter the code you received on one of your approved devices: ")
            result = icloud.validate_2fa_code(code)
            if result:
                logging.info("Two-factor authentication successful.")
            else:
                logging.error("Failed to verify two-factor authentication code.")
                exit()

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

try:
    icloud = authenticate()

    # Set the directory where you want to download the photos/videos
    download_directory = 'icloud_photos'
    os.makedirs(download_directory, exist_ok=True)

    # Download all photos/videos from the iCloud Photo Library
    photos = icloud.photos.all
    logging.info(f"Found {len(photos)} items in iCloud.")

    requests_made = 0
    for photo in photos:
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
                        # logging.info(f"{photo_name} is already up-to-date, skipping download.")
                        # Reset the file's created/modified timestamps to the original photo timestamp
                        reset_file_timestamp(photo_path, photo.created)
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
                reset_file_timestamp(photo_path, photo.created)

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

except KeyboardInterrupt:
    logging.info("Process interrupted by user. Exiting gracefully...")

logging.info("Sync process complete.")