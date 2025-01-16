import dropbox
from dropbox.oauth import DropboxOAuth2FlowNoRedirect
from dropbox import exceptions
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

# Obtain access token using refresh token
try:
    dbx = dropbox.Dropbox(oauth2_refresh_token=REFRESH_TOKEN, app_key=APP_KEY, app_secret=APP_SECRET)
    logger.info("Connected to Dropbox successfully using refresh token.")
except Exception as e:
    logger.error(f"Error connecting to Dropbox: {e}")
    raise

# Load blacklist from CSV
blacklist_file = f"/CbCRs/blacklist.csv"
blacklist_urls = set()

try:
    _, blacklist_res = dbx.files_download(blacklist_file)
    file_like = io.BytesIO(blacklist_res.content)
    blacklist_df = pd.read_csv(file_like)
except exceptions.ApiError:
    logger.warning("No blacklist file found. Proceeding without URL blacklisting.")
