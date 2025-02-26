import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

# Define the scopes we need for access
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def generate_token():
    print("Starting authentication process...")
    # Path to your client secret file
    client_secret_file = 'client_secret.json'
    
    if not os.path.exists(client_secret_file):
        print(f"Error: {client_secret_file} not found. Please download it from Google Cloud Console.")
        return
    
    try:
        # Create the flow
        flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
        # Run the OAuth flow
        creds = flow.run_local_server(port=0)
        
        # Save the credentials
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
        print("Success! Token has been generated and saved to 'token.json'")
        print("Please copy the contents of this file to your GitHub repository secret.")
    except Exception as e:
        print(f"Error during authentication: {str(e)}")

if __name__ == "__main__":
    generate_token()
