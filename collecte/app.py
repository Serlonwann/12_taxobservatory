import streamlit as st
import streamlit_authenticator as stauth
from loguru import logger
import threading
import os
from datetime import datetime
import yaml
from yaml.loader import SafeLoader

# Streamlit page configuration
st.set_page_config(
    page_title="PDF Downloader Tool",
    layout="wide",
    page_icon="📃"
)

with open('/credentials/secret.yaml') as file: #TODO: change to the correct path
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

authenticator.login()

if st.session_state['authentication_status']:

    from pdf_downloader import find_and_download_pdfs
    from dropbox_client import *

    # Get environment variables from the .env file
    from dotenv import load_dotenv
    load_dotenv()

    st.title("📃 CbCR PDFs Finder")
    st.subheader("Search, Find, and Download CbCR PDFs")

    if "stop_event" not in st.session_state:
        st.session_state.stop_event = threading.Event()
    if "download_thread" not in st.session_state:
        st.session_state.download_thread = None

    # Tabs for navigation
    tab1, tab2 = st.tabs(["🔍 Search Options", "⚙️ Advanced Settings"])

    # Search Options
    with tab1:
        st.markdown("### 1️⃣ Provide Input")
        input_option = st.radio(
            "Choose how to provide company details:",
            options=["Upload CSV File", "Manually Enter Details"],
            index=0,
            horizontal=True,
        )
        
        if input_option == "Upload CSV File":
            src_file = st.file_uploader("Upload a CSV File with Company Names", type=["csv"])
            if not src_file:
                st.warning("Please upload a CSV file to proceed.")
            company_name = None
        else:
            company_name = st.text_input("Company Name")

        st.markdown("### 2️⃣ Configure Search Keywords")
        search_keywords = st.text_input(
            "Search Keywords",
            value="tax country by country reporting GRI 207-4",
            help="Enter keywords to be used for the search query.",
        )

        ## the user can choose one or several year since 2016
        st.markdown("### 3️⃣ Select Year")
        years = st.multiselect(
            "Select Year",
            options=[str(i) for i in range(2016, datetime.now().year)],
            default=[str(datetime.now().year - 1)],
            help="Select the year to search for CbCR PDFs.",
        )

    # Advanced Settings
    with tab2:
        st.markdown("### Advanced Configuration")

        restrict_url = st.checkbox("⛓️ Restrict downloads to urls that contain company name")

        # Subfolder in which the whole search will be saved (default value is CbCRs_<current_date>_<current_time>)
        if st.checkbox("🗂️ Dropbox Subfolder"):
            subfolder = st.text_input(
                "Subfolder Name",
                value=f"CbCRs_{datetime.now().strftime('%Y%m%d')}",
                help="Enter the name of the subfolder where the PDFs will be saved.",
            )
        else:
            subfolder = None

        ## the user can choose to restrict the search to a specific time period
        if st.checkbox("📅 Restrict Search to a Specific Time Period"):
            date_restrict = st.selectbox(
                "Date Restriction",
                options=["y5", "y4", "y3", "y2", "y1"],
                index=0,
                help="Restrict results to a specific time period. ex: y5 = 5 previous years to date.",
            )
        else:
            date_restrict = None

        st.markdown("🚫 Edit the blacklist of URLs to exclude during PDF downloading.")
        
        # Load and display blacklist
        updated_blacklist_df = st.data_editor(blacklist_df, num_rows="dynamic", use_container_width=True)

        if st.button("Save Blacklist"):
            from dropbox_client import _save_blacklist_to_dropbox
            res = _save_blacklist_to_dropbox(updated_blacklist_df)
            if res:
                st.success("Blacklist updated successfully!")
            else:
                st.error("Error updating blacklist. Please try again.")

        fetch_timeout_s = st.number_input(
            "⏳ Fetch Timeout (in seconds)",
            min_value=1,
            max_value=300,
            value=60,
            step=1,
            help="Set timeout threshold for downloading a PDF.",
        )

    with tab1:
        # Start Button
        st.markdown("### 🚀 Start PDF Download")
        if st.button("Start Downloading"):
            if not os.getenv("CX_API_KEY") or not os.getenv("GOOGLE_CX"):
                st.error("Google API Key and CSE ID are required in environment variables.")
            def start_download():
                # Clear any previous stop request
                st.session_state.stop_event.clear()
                
                # Prepare the CSV DataFrame if applicable
                csv_df = pd.read_csv(src_file) if input_option == "Upload CSV File" else None

                # Start the download in a separate thread
                st.session_state.download_thread = threading.Thread(
                    target=find_and_download_pdfs,
                    kwargs={
                        "csv_df": csv_df,
                        "company_name": company_name,
                        "api_key": os.getenv("CX_API_KEY"),
                        "cse_id": os.getenv("GOOGLE_CX"),
                        "keywords": search_keywords,
                        "years": years,
                        "fetch_timeout_s": fetch_timeout_s,
                        "date_restrict": date_restrict,
                        "blacklist_urls": blacklist_urls,
                        "restrict_url": restrict_url,
                        "stop_event": st.session_state.stop_event,
                        "subfolder": subfolder,
                    }
                )
                st.session_state.download_thread.start()
            try:
                start_download()
                # Wait for the thread to finish
                st.session_state.download_thread.join()
                st.success("🎉 PDF downloading process completed, go check dropbox for results")
            except Exception as e:
                logger.error(e)
                st.error(f"❌ An error occurred: {e}")

        if st.button("Stop Downloading"):
            st.session_state.stop_event.set()
            st.success("🛑 PDF downloading process stopped")

elif st.session_state['authentication_status'] is False:
    st.error('Username/password is incorrect')
elif st.session_state['authentication_status'] is None:
    st.warning('Please enter your username and password')