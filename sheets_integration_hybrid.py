# FIXED VERSION - Handle Shared OneDrive Files
# Fixed the issue with accessing shared OneDrive files

import pandas as pd
import re
from datetime import datetime, time
import os
import sys
import json
import requests
import io
import base64
import traceback

# Try to import optional dependencies with fallbacks
try:
    import msal
    MSAL_AVAILABLE = True
    print("✅ MSAL library loaded successfully")
except ImportError:
    MSAL_AVAILABLE = False
    print("⚠️ MSAL library not available - using alternative authentication method")

try:
    from nacl import encoding, public
    NACL_AVAILABLE = True
except ImportError:
    NACL_AVAILABLE = False
    print("⚠️ PyNaCl not available - GitHub Secrets update will be disabled")

try:
    import gspread
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("⚠️ Google Sheets libraries not available - Google Sheets functionality disabled")

# SharePoint Configuration
SHAREPOINT_CONFIG = {
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Files.ReadWrite.All', 'https://graph.microsoft.com/Sites.ReadWrite.All'],
    'site_name': 'MCH.MMB.QA',
    'base_url': 'masangroup.sharepoint.com',
    'onedrive_user_email': 'hanpt@mml.masangroup.com',  # Owner of the shared file
    'onedrive_base_url': 'masangroup-my.sharepoint.com'
}

# SharePoint File IDs (updated with OneDrive source)
SHAREPOINT_FILE_IDS = {
    'sample_id': '8220CAEA-0CD9-585B-D483-DE0A82A98564',  # Sample ID.xlsx
    'knkh_data': '69AE13C5-76D7-4061-90E2-CE48F965C33A',  # BÁO CÁO KNKH.xlsx (OneDrive Personal)
    'data_knkh_output': '3E86CA4D-3F41-5C10-666B-5A51F8D9C911'  # Data KNKH.xlsx output
}

class GitHubSecretsUpdater:
    """Helper class to update GitHub Secrets using GitHub API"""
    def __init__(self, repo_owner, repo_name, github_token):
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.github_token = github_token
        self.api_base = "https://api.github.com"
    
    def get_public_key(self):
        """Get repository public key for encrypting secrets"""
        url = f"{self.api_base}/repos/{self.repo_owner}/{self.repo_name}/actions/secrets/public-key"
        headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get public key: {response.status_code}")
    
    def encrypt_secret(self, public_key, secret_value):
        """Encrypt secret using repository public key"""
        if not NACL_AVAILABLE:
            raise Exception("PyNaCl not available for secret encryption")
            
        public_key_obj = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key_obj)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        
        return base64.b64encode(encrypted).decode("utf-8")
    
    def update_secret(self, secret_name, secret_value):
        """Update a GitHub secret"""
        try:
            if not NACL_AVAILABLE:
                print(f"⚠️ Cannot update {secret_name} - PyNaCl not available")
                return False
                
            key_data = self.get_public_key()
            encrypted_value = self.encrypt_secret(key_data["key"], secret_value)
            
            url = f"{self.api_base}/repos/{self.repo_owner}/{self.repo_name}/actions/secrets/{secret_name}"
            headers = {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            data = {
                "encrypted_value": encrypted_value,
                "key_id": key_data["key_id"]
            }
            
            response = requests.put(url, headers=headers, json=data)
            if response.status_code in [201, 204]:
                print(f"✅ Successfully updated {secret_name}")
                return True
            else:
                print(f"❌ Failed to update {secret_name}: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error updating secret: {str(e)}")
            return False

class SharePointProcessor:
    """SharePoint integration class for authentication and data processing"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.site_id = None
        self.msal_app = None
        
        # Initialize MSAL app only if available
        if MSAL_AVAILABLE:
            try:
                self.msal_app = msal.PublicClientApplication(
                    SHAREPOINT_CONFIG['client_id'],
                    authority=SHAREPOINT_CONFIG['authority']
                )
                self.log("✅ MSAL app initialized successfully")
            except Exception as e:
                self.log(f"⚠️ MSAL initialization warning: {str(e)}")
                self.msal_app = None
        else:
            self.log("⚠️ MSAL not available - will use basic token authentication")
        
        if not self.authenticate():
            raise Exception("SharePoint authentication failed during initialization")

    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        try:
            sys.stdout.flush()
        except:
            pass

    def authenticate(self):
        """Authenticate using delegation flow with pre-generated tokens"""
        try:
            self.log("🔐 Authenticating with SharePoint/OneDrive...")

            access_token = os.environ.get('SHAREPOINT_ACCESS_TOKEN')
            refresh_token = os.environ.get('SHAREPOINT_REFRESH_TOKEN')

            if not access_token and not refresh_token:
                self.log("❌ No SharePoint tokens found in environment variables")
                return False

            self.access_token = access_token
            self.refresh_token = refresh_token
            
            if access_token:
                self.log(f"✅ Found access token: {access_token[:30] if access_token else 'None'}...")
                
                if self.test_token_validity():
                    self.log("✅ SharePoint/OneDrive access token is valid")
                    return True
                else:
                    self.log("⚠️ SharePoint/OneDrive access token expired, attempting refresh...")
                    
            if refresh_token and self.msal_app:
                if self.refresh_access_token():
                    self.log("✅ SharePoint/OneDrive token refreshed successfully")
                    self.update_github_secrets()
                    return True
                else:
                    self.log("❌ SharePoint/OneDrive token refresh failed")
                    return False
            else:
                if not refresh_token:
                    self.log("❌ No SharePoint/OneDrive refresh token available")
                if not self.msal_app:
                    self.log("❌ MSAL not available for token refresh")
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
            if not self.refresh_token or not self.msal_app:
                return False

            self.log("🔄 Attempting to refresh token using MSAL...")

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
            
            if self.access_token:
                updater.update_secret('SHAREPOINT_ACCESS_TOKEN', self.access_token)
            
            if self.refresh_token:
                updater.update_secret('SHAREPOINT_REFRESH_TOKEN', self.refresh_token)
            
            return True
            
        except Exception as e:
            self.log(f"⚠️ Error updating GitHub Secrets: {str(e)}")
            return False

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

    def download_excel_file_by_id(self, file_id, description="", source_type="sharepoint"):
        """Download Excel file from SharePoint or OneDrive by file ID with improved shared file handling"""
        try:
            self.log(f"📥 Downloading {description} from {source_type.upper()}...")

            if source_type == "onedrive":
                # Try multiple approaches for OneDrive files
                file_info, download_url = self._get_onedrive_file_info(file_id, description)
                if not file_info or not download_url:
                    return None
            else:
                site_id = self.get_site_id()
                if not site_id:
                    self.log("❌ Cannot get SharePoint site ID")
                    return None
                url = f"{self.base_url}/sites/{site_id}/drive/items/{file_id}"
                self.log(f"Using SharePoint endpoint: /sites/{site_id}/drive/items/{file_id}")

                response = requests.get(url, headers=self.get_headers(), timeout=30)

                if response.status_code == 200:
                    file_info = response.json()
                    download_url = file_info.get('@microsoft.graph.downloadUrl')
                    if not download_url:
                        self.log(f"❌ No download URL found for {description}")
                        return None
                else:
                    self.log(f"❌ Error getting file info: {response.status_code}")
                    return None

            # Download the file content
            file_name = file_info.get('name', 'Unknown')
            self.log(f"✅ Found file: {file_name}")
            self.log(f"✅ Got download URL, downloading content...")
            
            file_response = requests.get(download_url, timeout=60)

            if file_response.status_code == 200:
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

        except Exception as e:
            self.log(f"❌ Error downloading {description}: {str(e)}")

        return None

    def _get_onedrive_file_info(self, file_id, description):
        """Try multiple approaches to access OneDrive file (personal, shared, owner's drive)"""
        
        # Approach 1: Try personal drive first
        self.log(f"🔍 Approach 1: Trying personal drive access...")
        url = f"{self.base_url}/me/drive/items/{file_id}"
        response = requests.get(url, headers=self.get_headers(), timeout=30)
        
        if response.status_code == 200:
            file_info = response.json()
            download_url = file_info.get('@microsoft.graph.downloadUrl')
            if download_url:
                self.log(f"✅ Found in personal drive")
                return file_info, download_url
        else:
            self.log(f"❌ Personal drive access failed: {response.status_code}")
        
        # Approach 2: Try shared files
        self.log(f"🔍 Approach 2: Searching in shared files...")
        try:
            shared_url = f"{self.base_url}/me/drive/sharedWithMe"
            shared_response = requests.get(shared_url, headers=self.get_headers(), timeout=30)
            
            if shared_response.status_code == 200:
                shared_data = shared_response.json()
                shared_items = shared_data.get('value', [])
                self.log(f"Found {len(shared_items)} shared items")
                
                # Look for our file in shared items
                for item in shared_items:
                    if item.get('id') == file_id or file_id in str(item.get('id', '')):
                        download_url = item.get('@microsoft.graph.downloadUrl')
                        if download_url:
                            self.log(f"✅ Found in shared files: {item.get('name')}")
                            return item, download_url
                
                # Also search by name if we know it
                target_names = ['BÁO CÁO KNKH.xlsx', 'BÁO CÁO KNKH', 'KNKH']
                for item in shared_items:
                    item_name = item.get('name', '').upper()
                    if any(target in item_name for target in target_names):
                        # Try to get download URL for this item
                        item_id = item.get('id')
                        if item_id:
                            item_url = f"{self.base_url}/me/drive/items/{item_id}"
                            item_response = requests.get(item_url, headers=self.get_headers(), timeout=30)
                            if item_response.status_code == 200:
                                item_info = item_response.json()
                                download_url = item_info.get('@microsoft.graph.downloadUrl')
                                if download_url:
                                    self.log(f"✅ Found by name in shared files: {item_name}")
                                    return item_info, download_url
                
            else:
                self.log(f"❌ Shared files access failed: {shared_response.status_code}")
        except Exception as e:
            self.log(f"❌ Error accessing shared files: {str(e)}")
        
        # Approach 3: Try owner's drive (if configured)
        if SHAREPOINT_CONFIG.get('onedrive_user_email'):
            self.log(f"🔍 Approach 3: Trying owner's drive ({SHAREPOINT_CONFIG['onedrive_user_email']})...")
            try:
                owner_email = SHAREPOINT_CONFIG['onedrive_user_email']
                owner_url = f"{self.base_url}/users/{owner_email}/drive/items/{file_id}"
                owner_response = requests.get(owner_url, headers=self.get_headers(), timeout=30)
                
                if owner_response.status_code == 200:
                    file_info = owner_response.json()
                    download_url = file_info.get('@microsoft.graph.downloadUrl')
                    if download_url:
                        self.log(f"✅ Found in owner's drive")
                        return file_info, download_url
                else:
                    self.log(f"❌ Owner's drive access failed: {owner_response.status_code}")
                    
            except Exception as e:
                self.log(f"❌ Error accessing owner's drive: {str(e)}")
        
        # Approach 4: Search by Graph API search
        self.log(f"🔍 Approach 4: Trying Graph search...")
        try:
            search_query = "BÁO CÁO KNKH.xlsx"
            search_url = f"{self.base_url}/me/drive/search(q='{search_query}')"
            search_response = requests.get(search_url, headers=self.get_headers(), timeout=30)
            
            if search_response.status_code == 200:
                search_data = search_response.json()
                search_items = search_data.get('value', [])
                self.log(f"Search found {len(search_items)} items")
                
                for item in search_items:
                    if 'KNKH' in item.get('name', '').upper():
                        download_url = item.get('@microsoft.graph.downloadUrl')
                        if download_url:
                            self.log(f"✅ Found via search: {item.get('name')}")
                            return item, download_url
                        else:
                            # Try to get download URL
                            item_id = item.get('id')
                            if item_id:
                                item_url = f"{self.base_url}/me/drive/items/{item_id}"
                                item_response = requests.get(item_url, headers=self.get_headers(), timeout=30)
                                if item_response.status_code == 200:
                                    item_info = item_response.json()
                                    download_url = item_info.get('@microsoft.graph.downloadUrl')
                                    if download_url:
                                        self.log(f"✅ Found via search with item lookup: {item.get('name')}")
                                        return item_info, download_url
            else:
                self.log(f"❌ Search failed: {search_response.status_code}")
                
        except Exception as e:
            self.log(f"❌ Error in Graph search: {str(e)}")
        
        self.log(f"❌ All OneDrive access approaches failed for {description}")
        self.log(f"💡 Make sure the file is accessible and the correct permissions are granted")
        
        return None, None

    def upload_excel_to_sharepoint(self, df, file_id, sheet_name="Sheet1"):
        """Upload processed data to SharePoint Excel file"""
        try:
            self.log(f"📤 Uploading data to SharePoint...")

            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

            excel_buffer.seek(0)
            excel_content = excel_buffer.getvalue()
            self.log(f"Created Excel file with {len(excel_content)} bytes")

            site_id = self.get_site_id()
            if not site_id:
                self.log("❌ Cannot get SharePoint site ID for upload")
                return False
                
            upload_url = f"{self.base_url}/sites/{site_id}/drive/items/{file_id}/content"

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
                self.log(f"Response: {response.text[:500] if response.text else 'Empty response'}")
                return False

        except Exception as e:
            self.log(f"❌ Error uploading to SharePoint: {str(e)}")
            return False

# ========================================================================
# DATA PROCESSING FUNCTIONS (unchanged)
# ========================================================================

def extract_phone_number(text):
    """Extract Vietnamese phone number from complaint content"""
    if not isinstance(text, str):
        return None
    
    text = text.strip()
    phone_patterns = [
        r'(?:^|\s|-)(\d{4}[\s\-\.]?\d{3}[\s\-\.]?\d{3})', # 0xxx xxx xxx format
        r'(?:^|\s|-)(\d{3}[\s\-\.]?\d{3}[\s\-\.]?\d{4})', # 0xx xxx xxxx format  
        r'(?:^|\s|-)(0\d{9,10})',  # Simple 10-11 digit format starting with 0
        r'(?:^|\s|-)(0\d{8,9})',   # 9-10 digit format starting with 0
    ]
    
    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            clean_number = re.sub(r'[\s\-\.]', '', match)
            
            if (clean_number.startswith('0') and 
                len(clean_number) >= 9 and 
                len(clean_number) <= 11 and
                clean_number.isdigit()):
                
                if (clean_number.startswith(('090', '091', '092', '093', '094', '095', '096', '097', '098', '099',
                                            '070', '076', '077', '078', '079',
                                            '081', '082', '083', '084', '085', '088',
                                            '056', '058', '059',
                                            '032', '033', '034', '035', '036', '037', '038', '039',
                                            '020', '021', '022', '024', '025', '026', '027', '028', '029')) or
                    (clean_number.startswith('0') and len(clean_number) >= 10)):
                    return clean_number
    
    fallback_pattern = r'0\d{9,10}'
    fallback_matches = re.findall(fallback_pattern, text)
    
    for match in fallback_matches:
        if len(match) >= 10 and len(match) <= 11:
            return match
    
    return None

def extract_short_product_name(full_name):
    """Extract a shorter version of the product name"""
    if pd.isna(full_name) or full_name == '':
        return ''

    full_name = str(full_name).strip()

    brand_pattern = r'(Omachi|Kokomi)'
    brand_match = re.search(brand_pattern, full_name)

    if not brand_match:
        return full_name

    start_pos = brand_match.start()

    pkg_pattern = r'\d+\s*gói\s*x\s*\d+\s*gr'
    pkg_match = re.search(pkg_pattern, full_name)

    if pkg_match:
        end_pos = pkg_match.start()
        short_name = full_name[start_pos:end_pos].strip()
    else:
        short_name = full_name[start_pos:].strip()

    return short_name

def clean_concatenated_dates(date_str):
    """Clean concatenated dates like '11/04/202511/04/202511/04/2025'"""
    if not isinstance(date_str, str):
        return date_str

    date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
    matches = re.findall(date_pattern, date_str)

    if matches:
        for match in matches:
            try:
                parsed_date = pd.to_datetime(match, format='%d/%m/%Y', dayfirst=True)
                current_date = datetime.now()
                if parsed_date <= current_date + pd.Timedelta(days=365):
                    return match
            except:
                continue

        return matches[0]

    date_pattern = r'(\d{1,2}-[A-Za-z]{3}-\d{4})'
    matches = re.findall(date_pattern, date_str)
    if matches:
        return matches[0]
    
    date_pattern = r'(\d{1,2}-\d{1,2}-\d{4})'
    matches = re.findall(date_pattern, date_str)
    if matches:
        return matches[0]

    if len(date_str) >= 11 and '-' in date_str and any(month in date_str[:11] for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
        return date_str[:11]

    if len(date_str) >= 10 and ('/' in date_str[:10] or '-' in date_str[:10]):
        return date_str[:10]

    return date_str

def extract_correct_date(text):
    """Extract the correct Ngày SX from Nội dung phản hồi"""
    if not isinstance(text, str):
        return None

    pattern = r'Ngày SX:\s*(\d{1,2}/\d{1,2}/\d{4})'
    match = re.search(pattern, text)

    if match:
        return match.group(1)

    return None

def extract_production_info(text):
    """Extract production information from text with improved line and machine logic"""
    if not isinstance(text, str):
        return None, None, None

    text = text.strip()
    
    time_match = re.search(r'(\d{1,2}\s*:\s*\d{1,2})', text)
    time_str = None
    if time_match:
        raw_time = time_match.group(1)
        time_str = re.sub(r'\s*:\s*', ':', raw_time)

    line = None
    machine = None

    parenthesis_pattern = r'Nơi SX:\s*I-MBP\s*\((.*?)\)'
    parenthesis_match = re.search(parenthesis_pattern, text)
    
    if parenthesis_match:
        content = parenthesis_match.group(1).strip()
        
        if time_str:
            time_number_pattern = rf'{re.escape(time_str)}\s+(\d{{2}})'
            time_number_match = re.search(time_number_pattern, content)
            if time_number_match:
                digits = time_number_match.group(1)
                first_digit = int(digits[0])
                second_digit = int(digits[1])
                
                if 1 <= first_digit <= 8:
                    line = str(first_digit)
                    machine = str(second_digit)
                    return time_str, line, machine
            
            if raw_time != time_str:
                raw_time_pattern = rf'{re.escape(raw_time)}\s+(\d{{2}})'
                raw_time_match = re.search(raw_time_pattern, content)
                if raw_time_match:
                    digits = raw_time_match.group(1)
                    first_digit = int(digits[0])
                    second_digit = int(digits[1])
                    
                    if 1 <= first_digit <= 8:
                        line = str(first_digit)
                        machine = str(second_digit)
                        return time_str, line, machine
        
        if time_str:
            content_for_i_pattern = content.replace(time_str, '').strip()
            if time_match and time_match.group(1) != time_str:
                content_for_i_pattern = content_for_i_pattern.replace(time_match.group(1), '').strip()
        else:
            content_for_i_pattern = content
        
        line_machine_match = re.search(r'(\d+)I', content_for_i_pattern)
        if line_machine_match:
            digits = line_machine_match.group(1)
            if len(digits) == 1 and 1 <= int(digits) <= 8:
                line = digits
            elif len(digits) >= 2:
                first_digit = int(digits[0])
                if 1 <= first_digit <= 8:
                    line = digits[0]
                    if len(digits) >= 2:
                        machine = digits[1]
    
    if line is None:
        line_pattern = r'(\d+)I(?!\w)'
        line_matches = re.findall(line_pattern, text)
        
        for match in line_matches:
            if len(match) == 1 and 1 <= int(match) <= 8:
                line = match
                break
            elif len(match) >= 2:
                first_digit = int(match[0])
                if 1 <= first_digit <= 8:
                    line = match[0]
                    if len(match) >= 2:
                        machine = match[1]
                    break
    
    if line is None and "Nơi SX: MBP" in text:
        mbp_pos = text.find("Nơi SX: MBP")
        surrounding_text = text[max(0, mbp_pos-20):mbp_pos+50]
        
        if time_str:
            time_number_pattern = rf'{re.escape(time_str)}\s+(\d{{2}})'
            time_number_match = re.search(time_number_pattern, surrounding_text)
            if time_number_match:
                digits = time_number_match.group(1)
                first_digit = int(digits[0])
                second_digit = int(digits[1])
                
                if 1 <= first_digit <= 8:
                    line = str(first_digit)
                    machine = str(second_digit)
                    return time_str, line, machine
        
        line_pattern = r'(\d+)I'
        line_match = re.search(line_pattern, surrounding_text)
        if line_match:
            digits = line_match.group(1)
            if len(digits) == 1 and 1 <= int(digits) <= 8:
                line = digits
            elif len(digits) >= 2:
                first_digit = int(digits[0])
                if 1 <= first_digit <= 8:
                    line = digits[0]
                    if len(digits) >= 2:
                        machine = digits[1]

    return time_str, line, machine

def standardize_date(date_str):
    """Improved date standardization with explicit format handling"""
    try:
        if isinstance(date_str, str):
            date_str = date_str.strip()
            
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                return pd.to_datetime(date_str, format='%d/%m/%Y')
            
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y')
                except:
                    return pd.to_datetime(date_str, format='%m/%d/%Y')
            
            if '-' in date_str:
                for fmt in ['%d-%b-%Y', '%d-%B-%Y', '%d-%b-%y', '%d-%B-%y']:
                    try:
                        return pd.to_datetime(date_str, format=fmt)
                    except:
                        continue

            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.to_datetime(date_str, dayfirst=True)

        return pd.to_datetime(date_str)
    except:
        return None

def format_date_mm_dd_yyyy(date_obj):
    """Format a date object to MM/DD/YYYY string format for Power BI"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.strftime('%m/%d/%Y')

def extract_month(date_obj):
    """Extract month from a datetime object"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.month

def extract_year(date_obj):
    """Extract year from a datetime object"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.year

def extract_week(date_obj):
    """Extract ISO week number from a datetime object"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.isocalendar()[1]

def clean_item_code(item_code):
    if pd.isna(item_code) or item_code == '':
        return ''

    item_str = str(item_code).strip()
    return item_str

def parse_time(time_str):
    if pd.isna(time_str) or time_str == '':
        return None

    time_str = str(time_str).strip().lower()

    try:
        if ':' in time_str:
            hours, minutes = map(int, time_str.split(':'))
            return time(hours, minutes)

        elif 'h' in time_str:
            hours = int(time_str.replace('h', ''))
            return time(hours, 0)

        else:
            try:
                hours = int(time_str)
                return time(hours, 0)
            except:
                return None
    except:
        return None

def round_to_2hour(t):
    if t is None:
        return None

    hour = t.hour
    rounded_hour = (hour // 2) * 2
    return time(rounded_hour, 0)

def determine_shift(time_obj):
    """Modified to return just the shift number (1, 2, or 3) for Power BI"""
    if time_obj is None:
        return None

    shift1_start = time(6, 30)
    shift1_end = time(14, 30)
    shift2_end = time(22, 30)

    if shift1_start <= time_obj < shift1_end:
        return 1
    elif shift1_end <= time_obj < shift2_end:
        return 2
    else:
        return 3

def create_leader_mapping(aql_data):
    """Creates a mapping from leader codes to leader names"""
    leader_name_column = None
    leader_code_column = None
    
    for col in aql_data.columns:
        col_lower = col.lower()
        if 'tên trưởng ca' in col_lower or 'ten truong ca' in col_lower:
            leader_name_column = col
        elif ('trưởng ca' in col_lower or 'truong ca' in col_lower) and 'tên' not in col_lower:
            leader_code_column = col
    
    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break
    
    print(f"Found columns:")
    print(f"  QA column: {qa_column}")
    print(f"  Leader NAME column (Tên Trưởng ca): {leader_name_column}")
    print(f"  Leader CODE column (Trưởng ca): {leader_code_column}")
    
    if leader_name_column:
        leader_column = leader_name_column
        print(f"✓ Using leader NAME column: {leader_column}")
    elif leader_code_column:
        leader_column = leader_code_column
        print(f"⚠ Using leader CODE column: {leader_column} (names not found)")
    else:
        print("❌ No leader column found")
        return {}
    
    if not qa_column:
        print("❌ No QA column found")
        return {}
    
    leader_mapping = {}
    
    qa_leader_combinations = aql_data[[qa_column, leader_column]].dropna().drop_duplicates()
    
    print(f"\nFound {len(qa_leader_combinations)} unique QA-Leader combinations:")
    for idx, row in qa_leader_combinations.iterrows():
        qa_val = row[qa_column]
        leader_val = row[leader_column]
        print(f"  QA: '{qa_val}' -> Leader: '{leader_val}'")
        
        leader_mapping[str(leader_val)] = str(leader_val)
    
    print(f"Final leader mapping: {leader_mapping}")
    return leader_mapping

def find_qa_and_leader(row, aql_data, leader_mapping=None):
    """Improved function to match QA and leader from the AQL data sheet"""
    if pd.isna(row['Ngày SX_std']) or row['Item_clean'] == '' or row['Giờ_time'] is None:
        return None, None, "Missing required data"

    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break

    leader_name_column = None
    leader_code_column = None
    
    for col in aql_data.columns:
        col_lower = col.lower()
        if 'tên trưởng ca' in col_lower or 'ten truong ca' in col_lower:
            leader_name_column = col
        elif ('trưởng ca' in col_lower or 'truong ca' in col_lower) and 'tên' not in col_lower:
            leader_code_column = col
    
    if leader_name_column:
        leader_column = leader_name_column
    elif leader_code_column:
        leader_column = leader_code_column
    else:
        leader_column = None

    if not qa_column:
        return None, None, f"QA column not found in AQL data. Available columns: {list(aql_data.columns)}"

    if not leader_column:
        return None, None, f"Leader column not found in AQL data. Available columns: {list(aql_data.columns)}"
    
    complaint_line = row['Line_extracted']
    if pd.isna(complaint_line):
        return None, None, "Missing line information"
    
    try:
        complaint_line = int(float(complaint_line))
    except (ValueError, TypeError):
        return None, None, f"Invalid line value: {complaint_line}"

    complaint_hour = row['Giờ_time'].hour
    complaint_minute = row['Giờ_time'].minute
    search_date = row['Ngày SX_std']
    
    if complaint_hour < 6 or (complaint_hour == 6 and complaint_minute < 30):
        if row['Shift'] == 3:
            search_date = search_date - pd.Timedelta(days=1)
            date_adjusted = True
        else:
            date_adjusted = False
    else:
        date_adjusted = False

    debug_parts = []
    if date_adjusted:
        debug_parts.append(f"NIGHT SHIFT ADJUSTMENT: Looking for: Date={search_date.strftime('%d/%m/%Y')} (adjusted from {row['Ngày SX_std'].strftime('%d/%m/%Y')}), Item={row['Item_clean']}, Line={complaint_line}")
    else:
        debug_parts.append(f"Looking for: Date={search_date.strftime('%d/%m/%Y')}, Item={row['Item_clean']}, Line={complaint_line}")

    date_item_matches = aql_data[
        (aql_data['Ngày SX_std'] == search_date) & 
        (aql_data['Item_clean'] == row['Item_clean'])
    ]
    
    debug_parts.append(f"Date+Item matches: {len(date_item_matches)}")
    
    if date_item_matches.empty:
        date_only_matches = aql_data[aql_data['Ngày SX_std'] == search_date]
        debug_parts.append(f"Date-only matches: {len(date_only_matches)}")
        
        item_only_matches = aql_data[aql_data['Item_clean'] == row['Item_clean']]
        debug_parts.append(f"Item-only matches: {len(item_only_matches)}")
        
        return None, None, " | ".join(debug_parts)

    matching_rows = date_item_matches[date_item_matches['Line'] == complaint_line]
    
    debug_parts.append(f"Date+Item+Line matches: {len(matching_rows)}")
    
    if matching_rows.empty:
        available_lines = date_item_matches['Line'].dropna().unique()
        debug_parts.append(f"Available lines for this date+item: {sorted([x for x in available_lines if pd.notna(x)])}")
        return None, None, " | ".join(debug_parts)

    if complaint_minute == 0 and complaint_hour % 2 == 0:
        prev_hour = complaint_hour
        next_hour = (complaint_hour + 2) % 24
    else:
        prev_hour = (complaint_hour // 2) * 2
        next_hour = (prev_hour + 2) % 24

    debug_parts.append(f"Complaint at {complaint_hour}:{complaint_minute:02d}, checking {prev_hour}h and {next_hour}h")

    prev_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == prev_hour and x.minute == 0)]
    next_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == next_hour and x.minute == 0)]

    debug_parts.append(f"Prev hour ({prev_hour}h) records: {len(prev_check)}, Next hour ({next_hour}h) records: {len(next_check)}")

    available_times = matching_rows[matching_rows['Giờ_time'].notna()]['Giờ_time'].apply(lambda x: f"{x.hour}:{x.minute:02d}").unique()
    debug_parts.append(f"Available times: {sorted(available_times)}")

    if (search_date == pd.to_datetime('26/04/2025', format='%d/%m/%Y') and 
        'PRO CCT' in str(row['Item_clean']).upper()):
        hang_rows = matching_rows[matching_rows[qa_column] == "Hằng"]
        if not hang_rows.empty:
            hang_row = hang_rows.iloc[0]
            debug_parts.append("Special case for KKM PRO CCT on 26/04/2025")
            leader_value = hang_row[leader_column]
            if leader_mapping and leader_value is not None:
                mapped_leader = leader_mapping.get(str(leader_value), leader_value)
            else:
                mapped_leader = leader_value
            return hang_row[qa_column], mapped_leader, " | ".join(debug_parts)
    
    if not prev_check.empty:
        prev_qa = prev_check.iloc[0].get(qa_column)
        prev_leader = prev_check.iloc[0].get(leader_column)

        if leader_mapping and prev_leader is not None:
            prev_leader = leader_mapping.get(str(prev_leader), prev_leader)
        
        if not next_check.empty:
            next_qa = next_check.iloc[0].get(qa_column)
            next_leader = next_check.iloc[0].get(leader_column)

            if leader_mapping and next_leader is not None:
                next_leader = leader_mapping.get(str(next_leader), next_leader)
            
            if prev_qa == next_qa and prev_leader == next_leader:
                debug_parts.append(f"Same QA ({prev_qa}) and leader ({prev_leader}) for both {prev_hour}h and {next_hour}h")
                return prev_qa, prev_leader, " | ".join(debug_parts)

        shift = row['Shift']

        if shift == 3 and complaint_hour >= 22:
            if not next_check.empty:
                debug_parts.append(f"Using next hour ({next_hour}h) QA ({next_qa}) and leader ({next_leader}) based on Shift 3 rule")
                return next_qa, next_leader, " | ".join(debug_parts)

        debug_parts.append(f"Using previous hour ({prev_hour}h) QA ({prev_qa}) and leader ({prev_leader})")
        return prev_qa, prev_leader, " | ".join(debug_parts)

    elif not next_check.empty:
        next_qa = next_check.iloc[0].get(qa_column)
        next_leader = next_check.iloc[0].get(leader_column)
        
        if leader_mapping and next_leader is not None:
            next_leader = leader_mapping.get(str(next_leader), next_leader)
        
        debug_parts.append(f"Only next hour ({next_hour}h) data available - QA ({next_qa}) and leader ({next_leader})")
        return next_qa, next_leader, " | ".join(debug_parts)

    if not matching_rows.empty:
        closest_row = None
        min_diff = float('inf')

        for _, aql_row in matching_rows.iterrows():
            if aql_row['Giờ_time'] is not None:
                aql_minutes = aql_row['Giờ_time'].hour * 60 + aql_row['Giờ_time'].minute
                complaint_minutes = complaint_hour * 60 + complaint_minute
                diff = abs(complaint_minutes - aql_minutes)

                if diff < min_diff:
                    min_diff = diff
                    closest_row = aql_row

        if closest_row is not None:
            closest_qa = closest_row.get(qa_column)
            closest_leader = closest_row.get(leader_column)
            
            if leader_mapping and closest_leader is not None:
                closest_leader = leader_mapping.get(str(closest_leader), closest_leader)
            
            closest_time = f"{closest_row['Giờ_time'].hour}:{closest_row['Giờ_time'].minute:02d}"
            debug_parts.append(f"Using closest time match at {closest_time} - QA ({closest_qa}) and leader ({closest_leader})")
            return closest_qa, closest_leader, " | ".join(debug_parts)

    debug_parts.append("No matching QA records found")
    return None, None, " | ".join(debug_parts)

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
        print(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

    print("\n📥 Loading data from multiple sources...")

    # 1. Get AQL data from SharePoint site
    print("📋 Loading AQL data from SharePoint site...")
    try:
        aql_sheets_data = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['sample_id'], 
            "Sample ID.xlsx (AQL Data)",
            source_type="sharepoint"
        )

        if not aql_sheets_data:
            print("❌ Failed to download AQL data from SharePoint")
            sys.exit(1)

        aql_df = aql_sheets_data.get('ID AQL', pd.DataFrame())
        if aql_df.empty:
            print("❌ ID AQL sheet not found or empty")
            sys.exit(1)

        print(f"✅ AQL data loaded: {len(aql_df)} records")

    except Exception as e:
        print(f"❌ Error loading AQL data from SharePoint: {str(e)}")
        sys.exit(1)

    # 2. Get KNKH data from OneDrive Personal (with improved shared file handling)
    print("📋 Loading KNKH data from OneDrive Personal...")
    try:
        knkh_sheets_data = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['knkh_data'], 
            "BÁO CÁO KNKH.xlsx (OneDrive Personal)",
            source_type="onedrive"
        )

        if not knkh_sheets_data:
            print("❌ Failed to download KNKH data from OneDrive")
            sys.exit(1)

        knkh_df = None
        data_sheet_name = None
        
        if 'Data' in knkh_sheets_data:
            knkh_df = knkh_sheets_data['Data']
            data_sheet_name = 'Data'
        else:
            for sheet_name in knkh_sheets_data.keys():
                if 'data' in sheet_name.lower().strip():
                    knkh_df = knkh_sheets_data[sheet_name]
                    data_sheet_name = sheet_name
                    print(f"✅ Found data sheet: '{sheet_name}' (with {len(knkh_df)} rows)")
                    break
        
        if knkh_df is None or knkh_df.empty:
            print("❌ 'Data' sheet not found, trying alternatives...")
            print(f"Available sheets: {list(knkh_sheets_data.keys())}")
            
            possible_sheet_names = ['Sheet1', 'BÁO CÁO KNKH', 'MMB', 'Chi tiết4', 'Trang_tính1']
            for sheet_name in possible_sheet_names:
                if sheet_name in knkh_sheets_data:
                    temp_df = knkh_sheets_data[sheet_name]
                    if not temp_df.empty and len(temp_df) > 10:
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

    # Rest of the processing remains the same...
    # (Continue with the same data processing logic as before)
    
    # 3. Apply MMB factory filter
    print("\n🏭 Applying MMB factory filter...")
    
    factory_column = None
    
    if len(knkh_df.columns) > 8:
        col_i = knkh_df.columns[8]
        print(f"Column I (index 8): '{col_i}'")
        
        if knkh_df[col_i].astype(str).str.contains('MMB', na=False).any():
            factory_column = col_i
            print(f"✅ Found MMB values in column I: '{factory_column}'")
    
    if not factory_column:
        for col in knkh_df.columns:
            col_lower = col.lower()
            if 'nhà máy sản xuất' in col_lower or 'nha may san xuat' in col_lower or 'factory' in col_lower:
                factory_column = col
                print(f"✅ Found factory column by name: '{factory_column}'")
                break
    
    if factory_column:        
        unique_factories = knkh_df[factory_column].value_counts()
        print(f"Factory distribution before filtering:")
        for factory, count in unique_factories.items():
            print(f"  '{factory}': {count} records")
        
        original_count = len(knkh_df)
        knkh_df = knkh_df[knkh_df[factory_column].astype(str).str.upper().str.contains('MMB', na=False)]
        filtered_count = len(knkh_df)
        
        print(f"✅ Factory filter applied:")
        print(f"  Original records: {original_count}")
        print(f"  MMB records: {filtered_count}")
        print(f"  Filtered out: {original_count - filtered_count}")
        
        if filtered_count == 0:
            print("❌ No records found for MMB factory. Please check the data.")
            print("Available factory values:", unique_factories.index.tolist())
            sys.exit(1)
            
    else:
        print("⚠️ Factory column not found in data")
        print("Available columns:", list(knkh_df.columns))
        print("Column positions and names:")
        for i, col in enumerate(knkh_df.columns):
            print(f"  Column {chr(65+i)} (index {i}): '{col}'")
        print("Proceeding without factory filter...")

    # Continue with the rest of the data processing (dates, phone numbers, etc.)
    # ... (rest of the main function remains the same)
    
    # 4. Data processing
    print("\n🔄 Processing data...")

    print("📅 Processing dates...")
    knkh_df['Ngày tiếp nhận'] = knkh_df['Ngày tiếp nhận'].apply(clean_concatenated_dates)
    knkh_df['Ngày SX'] = knkh_df['Ngày SX'].apply(clean_concatenated_dates)
    
    knkh_df['Ngày SX_extracted'] = knkh_df['Nội dung phản hồi'].apply(extract_correct_date)

    knkh_df['Ngày SX'] = knkh_df.apply(
        lambda row: row['Ngày SX_extracted'] if row['Ngày SX_extracted'] is not None else row['Ngày SX'], 
        axis=1
    )

    print("📱 Extracting phone numbers...")
    knkh_df['SDT người KN'] = knkh_df['Nội dung phản hồi'].apply(extract_phone_number)
    
    phone_extracted_count = knkh_df['SDT người KN'].notna().sum()
    print(f"✅ Extracted {phone_extracted_count} phone numbers from {len(knkh_df)} records")

    knkh_df['Ngày SX_std'] = knkh_df['Ngày SX'].apply(standardize_date)
    aql_df['Ngày SX_std'] = aql_df['Ngày SX'].apply(standardize_date)

    filter_date = pd.to_datetime('2024-01-01')

    knkh_df = knkh_df[knkh_df['Ngày SX_std'] >= filter_date]
    aql_df = aql_df[aql_df['Ngày SX_std'] >= filter_date]

    print(f"After date filtering: {len(knkh_df)} KNKH records and {len(aql_df)} AQL records")

    print("🔧 Extracting production information...")
    knkh_df[['Giờ_extracted', 'Line_extracted', 'Máy_extracted']] = knkh_df['Nội dung phản hồi'].apply(
        lambda x: pd.Series(extract_production_info(x))
    )

    knkh_df['Line_extracted'] = pd.to_numeric(knkh_df['Line_extracted'], errors='coerce')
    knkh_df['Máy_extracted'] = pd.to_numeric(knkh_df['Máy_extracted'], errors='coerce')

    knkh_df['Ngày tiếp nhận_std'] = knkh_df['Ngày tiếp nhận'].apply(standardize_date)

    knkh_df['Item_clean'] = knkh_df['Item'].apply(clean_item_code)
    aql_df['Item_clean'] = aql_df['Item'].apply(clean_item_code)

    knkh_df['Giờ_time'] = knkh_df['Giờ_extracted'].apply(parse_time)
    
    gio_column = None
    for col in aql_df.columns:
        if col.startswith('Giờ') or col == 'Giờ':
            gio_column = col
            break
    
    if gio_column:
        aql_df['Giờ_time'] = aql_df[gio_column].apply(parse_time)
        print(f"Using time column: {gio_column}")
    else:
        print("Warning: No time column found in AQL data")
        aql_df['Giờ_time'] = None

    line_column = None
    for col in aql_df.columns:
        if col == 'Line' or col.startswith('Line'):
            line_column = col
            break
    
    if line_column and line_column != 'Line':
        aql_df['Line'] = aql_df[line_column]
        print(f"Using line column: {line_column}")
    elif not line_column:
        print("Warning: No Line column found in AQL data")

    if 'Line' in aql_df.columns:
        aql_df['Line'] = pd.to_numeric(aql_df['Line'], errors='coerce')
        print(f"Converted Line column to numeric")

    knkh_df['Giờ_rounded'] = knkh_df['Giờ_time'].apply(round_to_2hour)

    knkh_df['Shift'] = knkh_df['Giờ_time'].apply(determine_shift)

    print("🔍 Matching QA and leaders...")
    leader_mapping = create_leader_mapping(aql_df)
    print(f"Leader mapping: {leader_mapping}")

    knkh_df['QA_matched'] = None
    knkh_df['Tên Trưởng ca_matched'] = None
    knkh_df['debug_info'] = None

    print("Starting matching process...")
    total_matched = 0
    for idx, row in knkh_df.iterrows():
        qa, leader, debug_info = find_qa_and_leader(row, aql_df, leader_mapping)
        knkh_df.at[idx, 'QA_matched'] = qa
        knkh_df.at[idx, 'Tên Trưởng ca_matched'] = leader
        knkh_df.at[idx, 'debug_info'] = debug_info
        if qa is not None:
            total_matched += 1
        
        if (idx + 1) % 50 == 0:
            print(f"Processed {idx + 1} rows, {total_matched} matched so far")
    
    print(f"Matching process complete. Total matched: {total_matched} out of {len(knkh_df)} rows")

    knkh_df['Ngày tiếp nhận_formatted'] = knkh_df['Ngày tiếp nhận_std'].apply(format_date_mm_dd_yyyy)
    knkh_df['Ngày SX_formatted'] = knkh_df['Ngày SX_std'].apply(format_date_mm_dd_yyyy)

    knkh_df['Tháng sản xuất'] = knkh_df['Ngày SX_std'].apply(extract_month)
    knkh_df['Năm sản xuất'] = knkh_df['Ngày SX_std'].apply(extract_year)
    knkh_df['Tuần nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_week)
    knkh_df['Tháng nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_month)
    knkh_df['Năm nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_year)

    print(f"Total rows before filtering by 'Bộ phận chịu trách nhiệm': {len(knkh_df)}")
    
    responsible_dept_column = None
    for col in knkh_df.columns:
        col_lower = col.lower()
        if 'bộ phận chịu trách nhiệm' in col_lower or 'bo phan chiu trach nhiem' in col_lower or 'responsible' in col_lower:
            responsible_dept_column = col
            break
    
    if responsible_dept_column:
        dept_values = knkh_df[responsible_dept_column].value_counts()
        print(f"Responsible department distribution:")
        for dept, count in dept_values.items():
            print(f"  '{dept}': {count} records")
            
        before_filter = len(knkh_df)
        knkh_df = knkh_df[knkh_df[responsible_dept_column].astype(str).str.contains('Nhà máy|nha may|Factory', case=False, na=False)]
        after_filter = len(knkh_df)
        print(f"Rows after filtering for factory responsibility: {after_filter} (filtered out: {before_filter - after_filter})")
    else:
        print("⚠️ 'Bộ phận chịu trách nhiệm' column not found, skipping this filter")

    filtered_knkh_df = knkh_df.copy()

    if 'Tên sản phẩm' in filtered_knkh_df.columns:
        filtered_knkh_df['Tên sản phẩm ngắn'] = filtered_knkh_df['Tên sản phẩm'].apply(extract_short_product_name)
    else:
        product_col = None
        for col in filtered_knkh_df.columns:
            if 'sản phẩm' in col.lower() or 'san pham' in col.lower() or 'product' in col.lower():
                product_col = col
                break
        if product_col:
            filtered_knkh_df['Tên sản phẩm ngắn'] = filtered_knkh_df[product_col].apply(extract_short_product_name)
        else:
            filtered_knkh_df['Tên sản phẩm ngắn'] = ''

    # Build final dataframe
    available_columns = []
    column_mapping = {
        'Mã ticket': ['Mã ticket', 'Ma ticket', 'Ticket', 'ID'],
        'Ngày tiếp nhận_formatted': ['Ngày tiếp nhận_formatted'],
        'Tỉnh': ['Tỉnh', 'Tinh', 'Province'],
        'Ngày SX_formatted': ['Ngày SX_formatted'],
        'Sản phẩm/Dịch vụ': ['Sản phẩm/Dịch vụ', 'San pham/Dich vu', 'Product'],
        'Số lượng (ly/hộp/chai/gói/hủ)': ['Số lượng (ly/hộp/chai/gói/hủ)', 'So luong', 'Quantity'],
        'Nội dung phản hồi': ['Nội dung phản hồi', 'Noi dung phan hoi', 'Content'],
        'Item': ['Item'],
        'Tên sản phẩm': ['Tên sản phẩm', 'Ten san pham', 'Product Name'],
        'Tên sản phẩm ngắn': ['Tên sản phẩm ngắn'],
        'SL pack/ cây lỗi': ['SL pack/ cây lỗi', 'SL pack/cay loi', 'Defect Quantity'],
        'Tên lỗi': ['Tên lỗi', 'Ten loi', 'Defect Name'],
        'Line_extracted': ['Line_extracted'],
        'Máy_extracted': ['Máy_extracted'],
        'Giờ_extracted': ['Giờ_extracted'],
        'QA_matched': ['QA_matched'],
        'Tên Trưởng ca_matched': ['Tên Trưởng ca_matched'],
        'Shift': ['Shift'],
        'Tháng sản xuất': ['Tháng sản xuất'],
        'Năm sản xuất': ['Năm sản xuất'],
        'Tuần nhận khiếu nại': ['Tuần nhận khiếu nại'],
        'Tháng nhận khiếu nại': ['Tháng nhận khiếu nại'],
        'Năm nhận khiếu nại': ['Năm nhận khiếu nại'],
        'Bộ phận chịu trách nhiệm': [responsible_dept_column] if responsible_dept_column else ['Bộ phận chịu trách nhiệm'],
        'SDT người KN': ['SDT người KN'],
        'debug_info': ['debug_info']
    }
    
    final_columns = []
    for desired_col, possible_names in column_mapping.items():
        found_col = None
        for possible_name in possible_names:
            if possible_name in filtered_knkh_df.columns:
                found_col = possible_name
                break
        if found_col:
            final_columns.append(found_col)
            available_columns.append(desired_col)
        else:
            print(f"⚠️ Column not found: {desired_col}")
    
    final_df = filtered_knkh_df[final_columns].copy()
    
    rename_dict = {}
    for i, final_col in enumerate(final_columns):
        desired_name = available_columns[i]
        if final_col != desired_name:
            rename_dict[final_col] = desired_name
    
    standard_renames = {
        'Line_extracted': 'Line',
        'Máy_extracted': 'Máy', 
        'Giờ_extracted': 'Giờ',
        'QA_matched': 'QA',
        'Tên Trưởng ca_matched': 'Tên Trưởng ca',
        'Ngày tiếp nhận_formatted': 'Ngày tiếp nhận',
        'Ngày SX_formatted': 'Ngày SX'
    }
    
    for old_name, new_name in standard_renames.items():
        if old_name in final_df.columns:
            rename_dict[old_name] = new_name
    
    final_df.rename(columns=rename_dict, inplace=True)

    if 'Mã ticket' in final_df.columns:
        final_df['Mã ticket'] = pd.to_numeric(final_df['Mã ticket'], errors='coerce')
    
    final_df = final_df.sort_values(by='Mã ticket', ascending=False)

    print(f"\n📊 Final dataset prepared: {len(final_df)} records")
    print(f"📱 Phone numbers extracted: {final_df['SDT người KN'].notna().sum()} records")
    print(f"🏭 All records are from MMB factory")

    # 5. Upload to SharePoint
    print("\n📤 Uploading results to SharePoint...")
    try:
        success = sp_processor.upload_excel_to_sharepoint(
            final_df, 
            SHAREPOINT_FILE_IDS['data_knkh_output'],
            'Data_KNKH'
        )
        
        if success:
            print("✅ Data successfully uploaded to SharePoint!")
            print(f"📊 Final results:")
            print(f"  - Total records processed: {len(final_df)}")
            print(f"  - Records with QA matched: {final_df['QA'].notna().sum()}")
            print(f"  - Records with Leader matched: {final_df['Tên Trưởng ca'].notna().sum()}")
            print(f"  - Records with Phone numbers extracted: {final_df['SDT người KN'].notna().sum()}")
            print(f"  - All records are from MMB factory")
        else:
            print("❌ Failed to upload data to SharePoint")
            
            print("💾 Saving data locally as backup...")
            local_filename = "Data_KNKH_sharepoint_onedrive_backup.xlsx"
            with pd.ExcelWriter(local_filename, engine='openpyxl') as writer:
                final_df.to_excel(writer, sheet_name='Data_KNKH', index=False)
                
                debug_df = final_df[['Mã ticket', 'Ngày SX', 'Item', 'Line', 'Giờ', 'QA', 'Tên Trưởng ca', 'SDT người KN', 'debug_info']]
                debug_df.head(500).to_excel(writer, sheet_name='Debug_Info', index=False)
            
            print(f"Data saved locally to {local_filename}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error during SharePoint upload: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

    print("\n" + "="*80)
    print("✅ SHAREPOINT + ONEDRIVE INTEGRATION COMPLETED SUCCESSFULLY!")
    print("✅ KNKH DATA SOURCE: OneDrive Personal (Shared File)")
    print("✅ AQL DATA SOURCE: SharePoint Site") 
    print("✅ OUTPUT: SharePoint Site")
    print("✅ MMB FACTORY FILTER APPLIED!")
    print("✅ PHONE NUMBER EXTRACTION INCLUDED!")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)
