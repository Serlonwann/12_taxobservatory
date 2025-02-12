import os
import io
import threading
import requests
import pandas as pd
from dropbox_client import dbx  # from the same directory
from dropbox.exceptions import ApiError
from dropbox import files
from urllib.parse import urlparse
from loguru import logger

def find_and_download_pdfs(
    csv_df: pd.DataFrame,
    company_name: str,
    api_key: str,
    cse_id: str,
    keywords: str,
    years: list,
    fetch_timeout_s: int,
    date_restrict: str,
    blacklist_urls: list,
    restrict_url: bool,
    stop_event: threading.Event,
    subfolder: str,
) -> None:
    metadata_df = _load_metadata_from_dropbox(subfolder)
    if csv_df is not None:
        # Process multiple companies from CSV
        logger.info(f"Received CSV with {len(csv_df)} rows.")
        for index, row in csv_df.iterrows():
            company = str(row.get("CompanyName", "")).strip()
            if not company:
                logger.warning(f"Row {index} has no valid company name. Skipping.")
                continue
            logger.info(f"Starting PDF search for company: {company}")
            _search_and_download(
                company=company,
                api_key=api_key,
                cse_id=cse_id,
                keywords=keywords,
                years=years,
                fetch_timeout_s=fetch_timeout_s,
                date_restrict=date_restrict,
                blacklist_urls=blacklist_urls,
                restrict_url=restrict_url,
                metadata_df=metadata_df,
                stop_event=stop_event,
                subfolder=subfolder,
            )
    else:
        # Process a single company name
        if not company_name:
            logger.error("No CSV and no single company name provided.")
            return
        logger.info(f"Starting PDF search for single company: {company_name}")
        _search_and_download(
            company=company_name,
            api_key=api_key,
            cse_id=cse_id,
            keywords=keywords,
            years=years,
            fetch_timeout_s=fetch_timeout_s,
            date_restrict=date_restrict,
            blacklist_urls=blacklist_urls,
            restrict_url=restrict_url,
            metadata_df=metadata_df,
            stop_event=stop_event,
            subfolder=subfolder,
        )


def _search_and_download(
    company: str,
    api_key: str,
    cse_id: str,
    keywords: str,
    years: list,
    fetch_timeout_s: int,
    date_restrict: str,
    blacklist_urls: list,
    restrict_url: bool,
    metadata_df: pd.DataFrame,
    stop_event: threading.Event,
    subfolder: str,
):
    # For each year, build the query and fetch results
    for year in years:
        # Construct search query with filetype:pdf
        query = f"{company} {keywords} {year} filetype:pdf"

        logger.info(f"Searching for: {query}")
        # We'll paginate through results (up to 100 results, 10 per page).
        for start_index in range(1, 100, 10):
            search_url = (
                "https://www.googleapis.com/customsearch/v1?"
                f"key={api_key}&cx={cse_id}"
                f"&q={requests.utils.quote(query)}"
                f"&start={start_index}"
                f"&dateRestrict={date_restrict}"  # e.g. y1, y2, y3
            )
            logger.debug(f"Requesting URL: {search_url}")

            try:
                response = requests.get(search_url, timeout=fetch_timeout_s)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Error fetching results for query {query}: {e}")
                break

            data = response.json()
            items = data.get("items", [])
            if not items:
                logger.info("No more results for this query.")
                break

            for item in items:
                if stop_event and stop_event.is_set():
                    logger.warning("Stop event received. Stopping further downloads.")
                    return
                
                link = item.get("link", "")
                # Check for blacklist
                if any(bad_url.lower() in link.lower() for bad_url in blacklist_urls):
                    logger.debug(f"Skipping blacklisted URL: {link}")
                    continue

                # If restrict_url is True, only download if the company is in the link
                if restrict_url and company.lower() not in link.lower():
                    logger.debug(f"Skipping URL (restrict_url=True) that doesn't contain company: {link}")
                    continue

                if link in metadata_df["url"].values:
                    logger.debug(f"Skipping URL already in metadata: {link}")
                    continue
                
                # Else, extract the other company name from the URL
                company = urlparse(link).netloc.split(".")[1]

                # Attempt to download PDF
                original_filename, status = _download_pdf_to_dropbox(link, company, year, subfolder)
                # Add a new row to metadata
                new_row = pd.DataFrame(
                    [[company, year, link, original_filename, subfolder, query, status]],
                    columns=["company", "year", "url", "filename", "folder", "query", "status"]
                )
                metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
                _download_pdf_to_dropbox(link, company, year, subfolder)
                _save_metadata_to_dropbox(metadata_df, subfolder)

def _download_pdf_to_dropbox(pdf_url: str, company: str, year: str, subfolder: str):
    logger.info(f"Attempting PDF download from {pdf_url}")
    try:
        pdf_response = requests.get(pdf_url, stream=True, timeout=60)
        pdf_response.raise_for_status()
    except Exception as e:
        logger.error(f"Error downloading PDF from {pdf_url}: {e}")
        return None, e
    
    # Determine the original filename from Content-Disposition or fallback to last part of URL
    original_filename = _extract_original_filename(pdf_response, pdf_url)

    # Clean up company name for a valid file path or use your own normalization
    safe_company = "".join(c for c in company if c.isalnum() or c in (' ', '-', '_')).replace(" ", "_")
    file_name = f"{safe_company}_{year}.pdf"
    if subfolder:
        dropbox_path = f"/CbCRs/{subfolder}/{company}/{file_name}"
    else:
        dropbox_path = f"/CbCRs/{company}/{original_filename}"

    # Upload to Dropbox
    file_bytes = io.BytesIO(pdf_response.content)
    try:
        dbx.files_upload(file_bytes.getvalue(), dropbox_path, mode=files.WriteMode("overwrite"))
        logger.info(f"Uploaded PDF to Dropbox: {dropbox_path}")
        status = "OK"
    except Exception as e:
        logger.error(f"Error uploading to Dropbox ({dropbox_path}): {e}")
        status = e

    return original_filename, status

def _extract_original_filename(response: requests.Response, pdf_url: str) -> str:
    content_disp = response.headers.get("Content-Disposition", "")
    if "filename=" in content_disp.lower():
        # Attempt to parse filename from the header
        # e.g. Content-Disposition: attachment; filename="example.pdf"
        try:
            parts = content_disp.split("filename=")
            if len(parts) > 1:
                filename_part = parts[1].strip().strip('";\'')
                # In case there's a trailing semicolon or quotes
                filename_part = filename_part.replace('"', '').replace("'", "")
                logger.debug(f"Extracted filename from Content-Disposition: {filename_part}")
                return filename_part
        except Exception as e:
            logger.warning(f"Error parsing Content-Disposition header: {e}")

    # Fallback: last segment of the PDF URL (remove query params, anchors, etc.)
    parsed = urlparse(pdf_url)
    filename = os.path.basename(parsed.path)
    if not filename:
        filename = "unknown.pdf"
    # Attempt to remove ?query=... or #anchor if not parsed by urlparse
    filename = filename.split("?")[0].split("#")[0]
    logger.debug(f"Extracted filename from URL: {filename}")
    return filename


def _load_metadata_from_dropbox(subfolder) -> pd.DataFrame:
    try:
        if subfolder:
            METADATA_PATH = f"/CbCRs/{subfolder}/metadata.csv"
        else:
            METADATA_PATH = "/CbCRs/metadata.csv"
        md, res = dbx.files_download(METADATA_PATH)
        data = res.content
        df = pd.read_csv(io.BytesIO(data))
        logger.info(f"Loaded metadata with {len(df)} entries.")
        return df
    except ApiError as e:
        logger.error(f"Dropbox API error: request_id={e.request_id}, error={e.error}, message={e.user_message_text}")
        # If file not found, create an empty CSV
        if isinstance(e.error, files.DownloadError) and isinstance(e.error.get_path(), files.LookupError):
            if e.error.get_path().is_not_found():
                logger.warning("metadata.csv not found on Dropbox. A new file will be created.")
                columns = ["company", "year", "url", "filename", "folder", "query", "status"]
                return pd.DataFrame(columns=columns)
        else:
            logger.error("Error loading metadata from Dropbox: %s", e)
            raise

def _save_metadata_to_dropbox(metadata_df: pd.DataFrame, subfolder):
    try:
        if subfolder:
            METADATA_PATH = f"/CbCRs/{subfolder}/metadata.csv"
        else:
            METADATA_PATH = "/CbCRs/metadata.csv"
        csv_buffer = io.StringIO()
        metadata_df.to_csv(csv_buffer, index=False)
        dbx.files_upload(
            csv_buffer.getvalue().encode("utf-8"),
            METADATA_PATH,
            mode=files.WriteMode("overwrite")
        )
        logger.debug(f"metadata.csv saved to Dropbox with {len(metadata_df)} rows.")
    except Exception as e:
        logger.error(f"Error saving metadata to Dropbox: {e}")
