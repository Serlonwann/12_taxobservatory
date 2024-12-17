import dropbox
from dropbox import exceptions
from loguru import logger
import pandas as pd
import io
import os

from dotenv import load_dotenv
load_dotenv()
# Set your Dropbox Access Token (create app via https://www.dropbox.com/developers/apps/)
DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN")
# Initialize Dropbox client
try:
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    logger.info("Connected to Dropbox successfully.")
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
