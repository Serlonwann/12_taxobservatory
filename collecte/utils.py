from dropbox.oauth import DropboxOAuth2FlowNoRedirect

APP_KEY = ""
APP_SECRET = ""

# Start OAuth2 Flow
auth_flow = DropboxOAuth2FlowNoRedirect(APP_KEY, APP_SECRET, token_access_type='offline')
authorize_url = auth_flow.start()
print("1. Go to:", authorize_url)
print("2. Click 'Allow' (you might have to log in first).")
print("3. Copy the authorization code.")

auth_code = input("Enter the authorization code here: ").strip()
oauth_result = auth_flow.finish(auth_code)
print("Access Token:", oauth_result.access_token)
