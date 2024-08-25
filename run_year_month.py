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

# Constants
RATE_LIMIT = 100  # Maximum number of requests
TIME_WINDOW = 0  # Time window in seconds
MAX_RETRIES = 5
INITIAL_WAIT = 1  # Initial wait time in seconds
MAX_WAIT = 60  # Maximum wait time in seconds
TRACK_FILE = "last_downloaded.txt"
BASE_DOWNLOAD_DIRECTORY = 'downloaded_files'

def calculate_file_hash(file_path, chunk_size=1024 * 1024):
    """Calculate the MD5 hash of a file."""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as file:
        while chunk := file.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

def download_file_with_progress(photo, file_path, chunk_size=1024 * 1024):
    """Download a file with a progress bar and calculate its hash."""
    total_size = photo.size
    hasher = hashlib.md5()

    with open(file_path, 'wb') as file, tqdm(
        total=total_size, unit='B', unit_scale=True, desc=file_path
    ) as pbar:
        for chunk in photo.download().iter_content(chunk_size):
            if chunk:
                file.write(chunk)
                hasher.update(chunk)
                pbar.update(len(chunk))

    return hasher.hexdigest()

def reset_file_timestamp(file_path, timestamp, timezone='UTC'):
    """Set the file's timestamp to the specified time in the given timezone."""
    try:
        if timestamp.tzinfo is None:
            timestamp = pytz.UTC.localize(timestamp)

        local_tz = pytz.timezone(timezone)
        local_timestamp = timestamp.astimezone(local_tz)
        mod_time = local_timestamp.timestamp()

        os.utime(file_path, (mod_time, mod_time))

        if platform.system() == 'Darwin':  # macOS
            date_string = local_timestamp.strftime("%m/%d/%Y %H:%M:%S")
            env = os.environ.copy()
            env['TZ'] = timezone
            subprocess.run(['SetFile', '-d', date_string, file_path], check=True, env=env)
        elif platform.system() == 'Windows':
            from win32_setctime import setctime
            setctime(file_path, mod_time)

    except Exception as e:
        logging.error(f"Failed to set timestamps for {file_path}: {e}")

def exponential_backoff(attempt):
    """Calculate the exponential backoff time."""
    return min(INITIAL_WAIT * (2 ** attempt), MAX_WAIT)

def load_last_downloaded():
    """Load the name of the last downloaded file."""
    if os.path.exists(TRACK_FILE):
        with open(TRACK_FILE, 'r') as f:
            return f.read().strip()
    return None

def save_last_downloaded(photo_name):
    """Save the name of the last downloaded file."""
    with open(TRACK_FILE, 'w') as f:
        f.write(photo_name)

def authenticate():
    """Authenticate with iCloud."""
    try:
        session_dir = os.path.expanduser("~/.icloud-session")
        os.makedirs(session_dir, exist_ok=True)
        
        apple_id = input("Enter the email for Apple ID: ")
        password = keyring.get_password("icloud", apple_id)

        if not password:
            password = input(f"Enter the password for {apple_id}: ")
            keyring.set_password("icloud", apple_id, password)

        icloud = PyiCloudService(apple_id, password, cookie_directory=session_dir)

        if icloud.requires_2fa:
            logging.info("Two-factor authentication required.")
            code = input("Enter the code you received on one of your approved devices: ")
            if icloud.validate_2fa_code(code):
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

def create_directory_for_photo(photo):
    """Create a directory structure based on the year and month of the photo."""
    date_taken = photo.added_date
    year = date_taken.strftime('%Y')
    month = date_taken.strftime('%m')
    directory = os.path.join(BASE_DOWNLOAD_DIRECTORY, year, month)
    os.makedirs(directory, exist_ok=True)
    return directory

def process_photos(icloud):
    """Process and download photos from iCloud."""
    photos = icloud.photos.all
    logging.info(f"Found {len(photos)} items in iCloud.")

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
                directory = create_directory_for_photo(photo)
                photo_path = os.path.join(directory, photo_name)

                if os.path.exists(photo_path):
                    local_file_hash = calculate_file_hash(photo_path)
                    temp_file_path = photo_path + ".temp"
                    icloud_file_hash = download_file_with_progress(photo, temp_file_path)
                    
                    if local_file_hash == icloud_file_hash:
                        logging.info(f"{photo_name} is already up-to-date, skipping download.")
                        reset_file_timestamp(photo_path, photo.added_date)
                        os.remove(temp_file_path)
                        break
                    else:
                        logging.info(f"{photo_name} differs from iCloud version. Updating file.")
                        os.rename(temp_file_path, photo_path)
                else:
                    download_file_with_progress(photo, photo_path)

                reset_file_timestamp(photo_path, photo.added_date)
                save_last_downloaded(photo_name)

                requests_made += 1
                if requests_made >= RATE_LIMIT:
                    if TIME_WINDOW > 0:
                        logging.info(f"Rate limit reached. Sleeping for {TIME_WINDOW} seconds...")
                        time.sleep(TIME_WINDOW)
                    requests_made = 0

                break

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

if __name__ == "__main__":
    icloud = authenticate()
    process_photos(icloud)
    logging.info("Sync process complete.")
