# UPDATED SHAREPOINT CONFIGURATION FOR ONEDRIVE PERSONAL ACCESS
SHAREPOINT_CONFIG = {
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Files.ReadWrite.All', 'https://graph.microsoft.com/Sites.ReadWrite.All'],
    'site_name': 'MCH.MMB.QA',
    'base_url': 'masangroup.sharepoint.com',
    # NEW: OneDrive personal settings
    'onedrive_user_email': 'hanpt@mml.masangroup.com',  # User email from the URL
    'onedrive_base_url': 'masangroup-my.sharepoint.com'
}

# Updated SharePoint File IDs with OneDrive source
SHAREPOINT_FILE_IDS = {
    'sample_id': '8220CAEA-0CD9-585B-D483-DE0A82A98564',  # Sample ID.xlsx (SharePoint site)
    'knkh_data': '69AE13C5-76D7-4061-90E2-CE48F965C33A',  # BÁO CÁO KNKH.xlsx (OneDrive Personal)
    'data_knkh_output': '3E86CA4D-3F41-5C10-666B-5A51F8D9C911'  # Data KNKH.xlsx output (SharePoint site)
}

class SharePointProcessor:
    """SharePoint integration class for authentication and data processing"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.site_id = None
        self.msal_app = None
        
        # Initialize MSAL app with updated scopes
        self.msal_app = msal.PublicClientApplication(
            SHAREPOINT_CONFIG['client_id'],
            authority=SHAREPOINT_CONFIG['authority']
        )
        
        # Authenticate on initialization
        if not self.authenticate():
            raise Exception("SharePoint authentication failed during initialization")

    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        sys.stdout.flush()

    def authenticate(self):
        """Authenticate using delegation flow with pre-generated tokens"""
        try:
            self.log("🔐 Authenticating with SharePoint and OneDrive...")

            # Get tokens from environment variables
            access_token = os.environ.get('SHAREPOINT_ACCESS_TOKEN')
            refresh_token = os.environ.get('SHAREPOINT_REFRESH_TOKEN')

            if not access_token and not refresh_token:
                self.log("❌ No SharePoint tokens found in environment variables")
                self.log("💡 Please run generate_tokens.py locally and add tokens to GitHub Secrets")
                return False

            self.access_token = access_token
            self.refresh_token = refresh_token
            
            if access_token:
                self.log(f"✅ Found access token: {access_token[:30]}...")
                
                # Test token validity
                if self.test_token_validity():
                    self.log("✅ SharePoint/OneDrive access token is valid")
                    return True
                else:
                    self.log("⚠️ SharePoint/OneDrive access token expired, attempting refresh...")
                    
            # Try to refresh token
            if refresh_token:
                if self.refresh_access_token():
                    self.log("✅ SharePoint/OneDrive token refreshed successfully")
                    self.update_github_secrets()
                    return True
                else:
                    self.log("❌ SharePoint/OneDrive token refresh failed")
                    return False
            else:
                self.log("❌ No SharePoint/OneDrive refresh token available")
                return False

        except Exception as e:
            self.log(f"❌ SharePoint/OneDrive authentication error: {str(e)}")
            return False

    def test_token_validity(self):
        """Test if current access token is valid"""
        try:
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/me", headers=headers, timeout=30)

            if response.status_code == 200:
                user_info = response.json()
                self.log(f"✅ Authenticated to Microsoft Graph as: {user_info.get('displayName', 'Unknown')}")
                return True
            elif response.status_code == 401:
                return False
            else:
                self.log(f"Warning: Unexpected response code: {response.status_code}")
                return False

        except Exception as e:
            self.log(f"Error testing token validity: {str(e)}")
            return False

    def refresh_access_token(self):
        """Refresh access token using refresh token with MSAL"""
        try:
            if not self.refresh_token:
                self.log("❌ No refresh token available")
                return False

            self.log("🔄 Attempting to refresh token using MSAL...")

            # Use MSAL to refresh token with updated scopes
            result = self.msal_app.acquire_token_by_refresh_token(
                self.refresh_token,
                scopes=SHAREPOINT_CONFIG['scopes']
            )

            if result and "access_token" in result:
                self.access_token = result['access_token']
                if 'refresh_token' in result:
                    self.refresh_token = result['refresh_token']
                    self.log("✅ Got new refresh token")
                
                self.log("✅ Token refreshed successfully")
                return True
            else:
                error = result.get('error_description', 'Unknown error') if result else 'No result'
                self.log(f"❌ Token refresh failed: {error}")
                return False

        except Exception as e:
            self.log(f"❌ Error refreshing token: {str(e)}")
            return False

    def download_excel_file_by_id(self, file_id, description="", source_type="sharepoint"):
        """
        Download Excel file from SharePoint or OneDrive by file ID
        source_type: "sharepoint" for SharePoint site, "onedrive" for OneDrive personal
        """
        try:
            self.log(f"📥 Downloading {description} from {source_type.upper()}...")

            if source_type == "onedrive":
                # For OneDrive personal files, use /me/drive/items/{file_id}
                url = f"{self.base_url}/me/drive/items/{file_id}"
                self.log(f"Using OneDrive endpoint: /me/drive/items/{file_id}")
            else:
                # For SharePoint site files
                url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}"
                self.log(f"Using SharePoint endpoint: /sites/{self.get_site_id()}/drive/items/{file_id}")

            response = requests.get(url, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                file_info = response.json()
                download_url = file_info.get('@microsoft.graph.downloadUrl')
                file_name = file_info.get('name', 'Unknown')
                
                self.log(f"✅ Found file: {file_name}")

                if download_url:
                    # Download file content
                    self.log(f"✅ Got download URL, downloading content...")
                    file_response = requests.get(download_url, timeout=60)

                    if file_response.status_code == 200:
                        # Read Excel from memory
                        excel_data = io.BytesIO(file_response.content)
                        self.log(f"✅ Downloaded {len(file_response.content)} bytes")
                        
                        try:
                            excel_file = pd.ExcelFile(excel_data)
                            sheets_data = {}
                            
                            self.log(f"Excel sheets found: {excel_file.sheet_names}")
                            
                            for sheet_name in excel_file.sheet_names:
                                excel_data.seek(0)
                                df = pd.read_excel(excel_data, sheet_name=sheet_name)
                                sheets_data[sheet_name] = df
                                self.log(f"✅ Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
                            
                            self.log(f"✅ Successfully downloaded {description}")
                            return sheets_data
                            
                        except Exception as e:
                            self.log(f"❌ Error reading Excel file: {str(e)}")
                            return None
                    else:
                        self.log(f"❌ Error downloading file content: {file_response.status_code}")
                else:
                    self.log(f"❌ No download URL found for {description}")
            else:
                self.log(f"❌ Error getting file info: {response.status_code}")
                if response.status_code == 404:
                    if source_type == "onedrive":
                        self.log("💡 Tip: Make sure the file is in the authenticated user's OneDrive")
                    else:
                        self.log("💡 Tip: Check if the file ID is correct and accessible")
                self.log(f"Response: {response.text[:500]}")

        except Exception as e:
            self.log(f"❌ Error downloading {description}: {str(e)}")

        return None

    # Rest of the methods remain the same...
    def get_headers(self):
        """Get headers for API requests"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_site_id(self):
        """Get SharePoint site ID"""
        try:
            if self.site_id:
                return self.site_id

            url = f"{self.base_url}/sites/{SHAREPOINT_CONFIG['base_url']}:/sites/{SHAREPOINT_CONFIG['site_name']}"
            response = requests.get(url, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                site_data = response.json()
                self.site_id = site_data['id']
                self.log(f"✅ Found SharePoint site ID: {self.site_id}")
                return self.site_id
            else:
                self.log(f"❌ Error getting SharePoint site ID: {response.status_code}")
                return None

        except Exception as e:
            self.log(f"❌ Error getting SharePoint site ID: {str(e)}")
            return None

    def upload_excel_to_sharepoint(self, df, file_id, sheet_name="Sheet1"):
        """Upload processed data to SharePoint Excel file"""
        try:
            self.log(f"📤 Uploading data to SharePoint...")

            # Create Excel file in memory
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

            excel_buffer.seek(0)
            excel_content = excel_buffer.getvalue()
            self.log(f"Created Excel file with {len(excel_content)} bytes")

            # Upload to SharePoint (output always goes to SharePoint site, not OneDrive)
            upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}/content"

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)

            if response.status_code in [200, 201]:
                self.log(f"✅ Successfully uploaded {len(df)} rows to SharePoint")
                return True
            else:
                self.log(f"❌ Error uploading to SharePoint: {response.status_code}")
                self.log(f"Response: {response.text[:500]}")
                return False

        except Exception as e:
            self.log(f"❌ Error uploading to SharePoint: {str(e)}")
            return False

    def update_github_secrets(self):
        """Update GitHub Secrets with new tokens"""
        try:
            github_token = os.environ.get('GITHUB_TOKEN')
            if not github_token:
                self.log("⚠️ No GITHUB_TOKEN found, cannot update secrets")
                return False
            
            repo = os.environ.get('GITHUB_REPOSITORY', '')
            if '/' not in repo:
                self.log("⚠️ Invalid GITHUB_REPOSITORY format")
                return False
            
            repo_owner, repo_name = repo.split('/')
            updater = GitHubSecretsUpdater(repo_owner, repo_name, github_token)
            
            # Update access token
            if self.access_token:
                updater.update_secret('SHAREPOINT_ACCESS_TOKEN', self.access_token)
            
            # Update refresh token
            if self.refresh_token:
                updater.update_secret('SHAREPOINT_REFRESH_TOKEN', self.refresh_token)
            
            return True
            
        except Exception as e:
            self.log(f"⚠️ Error updating GitHub Secrets: {str(e)}")
            return False


# UPDATE THE MAIN FUNCTION TO USE CORRECT SOURCE TYPES
def main():
    print("="*80)
    print("🔄 SHAREPOINT + ONEDRIVE INTEGRATION - WITH MMB FILTER")
    print("="*80)

    # Initialize SharePoint processor
    print("\n🔗 Initializing SharePoint/OneDrive connection...")
    try:
        sp_processor = SharePointProcessor()
        print("✅ SharePoint/OneDrive connection established")
    except Exception as e:
        print(f"❌ SharePoint/OneDrive initialization failed: {str(e)}")
        sys.exit(1)

    print("\n📥 Loading data from multiple sources...")

    # 1. Get AQL data from SharePoint site
    print("📋 Loading AQL data from SharePoint site...")
    try:
        aql_sheets_data = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['sample_id'], 
            "Sample ID.xlsx (AQL Data)",
            source_type="sharepoint"  # SharePoint site
        )

        if not aql_sheets_data:
            print("❌ Failed to download AQL data from SharePoint")
            sys.exit(1)

        # Extract ID AQL sheet
        aql_df = aql_sheets_data.get('ID AQL', pd.DataFrame())
        if aql_df.empty:
            print("❌ ID AQL sheet not found or empty")
            sys.exit(1)

        print(f"✅ AQL data loaded: {len(aql_df)} records")

    except Exception as e:
        print(f"❌ Error loading AQL data from SharePoint: {str(e)}")
        sys.exit(1)

    # 2. Get KNKH data from OneDrive Personal ⭐ NEW SOURCE TYPE
    print("📋 Loading KNKH data from OneDrive Personal...")
    try:
        knkh_sheets_data = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['knkh_data'], 
            "BÁO CÁO KNKH.xlsx (OneDrive Personal)",
            source_type="onedrive"  # OneDrive Personal
        )

        if not knkh_sheets_data:
            print("❌ Failed to download KNKH data from OneDrive")
            sys.exit(1)

        # Find the Data sheet
        knkh_df = None
        data_sheet_name = None
        
        # First try exact match
        if 'Data' in knkh_sheets_data:
            knkh_df = knkh_sheets_data['Data']
            data_sheet_name = 'Data'
        else:
            # Look for sheets with "Data" in the name
            for sheet_name in knkh_sheets_data.keys():
                if 'data' in sheet_name.lower().strip():
                    knkh_df = knkh_sheets_data[sheet_name]
                    data_sheet_name = sheet_name
                    print(f"✅ Found data sheet: '{sheet_name}' (with {len(knkh_df)} rows)")
                    break
        
        if knkh_df is None or knkh_df.empty:
            print("❌ 'Data' sheet not found, trying alternatives...")
            print(f"Available sheets: {list(knkh_sheets_data.keys())}")
            
            # Try other possible sheet names
            possible_sheet_names = ['Sheet1', 'BÁO CÁO KNKH', 'MMB', 'Chi tiết4', 'Trang_tính1']
            for sheet_name in possible_sheet_names:
                if sheet_name in knkh_sheets_data:
                    temp_df = knkh_sheets_data[sheet_name]
                    if not temp_df.empty and len(temp_df) > 10:  # Use sheet with data
                        knkh_df = temp_df
                        data_sheet_name = sheet_name
                        print(f"✅ Using fallback sheet '{sheet_name}' with {len(knkh_df)} rows")
                        break

        if knkh_df is None or knkh_df.empty:
            print("❌ No valid KNKH data found in OneDrive file")
            sys.exit(1)

        print(f"✅ KNKH data loaded from OneDrive: {len(knkh_df)} records")
        knkh_df = knkh_df.copy()

    except Exception as e:
        print(f"❌ Error loading KNKH data from OneDrive: {str(e)}")
        sys.exit(1)

    # Continue with the rest of the processing...
    # (The rest of the main function remains the same)

if __name__ == "__main__":
    main()
