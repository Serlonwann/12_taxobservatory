import dropbox
from dropbox.oauth import DropboxOAuth2FlowNoRedirect
from dropbox import exceptions
from dropbox import files
from loguru import logger
import os
from dotenv import load_dotenv
import io
import pandas as pd

# Load environment variables
load_dotenv()

APP_KEY = os.getenv("DROPBOX_APP_KEY")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BLACKLIST_PATH = "/CbCRs/blacklist.csv"

# Obtain access token using refresh token
try:
    dbx = dropbox.Dropbox(oauth2_refresh_token=REFRESH_TOKEN, app_key=APP_KEY, app_secret=APP_SECRET)
    logger.info("Connected to Dropbox successfully using refresh token.")
except Exception as e:
    logger.error(f"Error connecting to Dropbox: {e}")
    raise

def _load_blacklist_from_dropbox():
    """
    Load the blacklist from Dropbox and return a set of blacklisted URLs.
    """
    try:
        _, blacklist_res = dbx.files_download(BLACKLIST_PATH)
        file_like = io.BytesIO(blacklist_res.content)
        return pd.read_csv(file_like)

    except exceptions.ApiError as e:
        logger.error(f"Dropbox API error: request_id={e.request_id}, error={e.error}, message={e.user_message_text}")
        # If file not found, create an empty CSV
        if isinstance(e.error, files.DownloadError) and isinstance(e.error.get_path(), files.LookupError):
            if e.error.get_path().is_not_found():
                blacklist_df = pd.DataFrame(columns=["Blacklisted URLs"])
                file_like = io.BytesIO()
                blacklist_df.to_csv(file_like, index=False)
                file_like.seek(0)
                try:
                    dbx.files_upload(file_like.read(), BLACKLIST_PATH, mode=dropbox.files.WriteMode.overwrite)
                    logger.info("Blacklist file created successfully.")
                except Exception as upload_error:
                    logger.error(f"Error creating blacklist file: {upload_error}")
                    raise
            else:
                logger.error(f"Error loading blacklist file: {e}")
                raise

def _save_blacklist_to_dropbox(blacklist_df: pd.DataFrame):
    """
    Save the given DataFrame to blacklist.csv in Dropbox (overwrite if exists).
    """
    try:
        csv_buffer = io.StringIO()
        blacklist_df.to_csv(csv_buffer, index=False)
        dbx.files_upload(
            csv_buffer.getvalue().encode("utf-8"),
            BLACKLIST_PATH,
            mode=files.WriteMode("overwrite")
        )
        logger.debug(f"blacklist.csv saved to Dropbox with {len(blacklist_df)} rows.")
        return True
    except Exception as e:
        logger.error(f"Error saving blacklist to Dropbox: {e}")


blacklist_df = _load_blacklist_from_dropbox()
blacklist_urls = set(blacklist_df["Blacklisted URLs"].str.strip())
