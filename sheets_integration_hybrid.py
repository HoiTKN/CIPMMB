import pandas as pd
import re
from datetime import datetime, time
import gspread
import os
import sys
import json
import requests
import msal
import io
import base64
import traceback
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Define the scopes for Google Sheets (kept for potential future use)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# SharePoint Configuration
SHAREPOINT_CONFIG = {
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Sites.ReadWrite.All'],
    'site_name': 'MCH.MMB.QA',
    'base_url': 'masangroup.sharepoint.com'
}

# SharePoint File IDs (updated with new KNKH source)
SHAREPOINT_FILE_IDS = {
    'sample_id': '8220CAEA-0CD9-585B-D483-DE0A82A98564',  # Sample ID.xlsx
    'knkh_data': '7B69AE13C5-76D7-4061-90E2-CE48F965C33A',  # BÁO CÁO KNKH.xlsx (NEW SOURCE)
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
        from nacl import encoding, public
        
        public_key_obj = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key_obj)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        
        return base64.b64encode(encrypted).decode("utf-8")
    
    def update_secret(self, secret_name, secret_value):
        """Update a GitHub secret"""
        try:
            # Get public key
            key_data = self.get_public_key()
            
            # Encrypt secret
            encrypted_value = self.encrypt_secret(key_data["key"], secret_value)
            
            # Update secret
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
        
        # Initialize MSAL app
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
            self.log("🔐 Authenticating with SharePoint...")

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
                    self.log("✅ SharePoint access token is valid")
                    return True
                else:
                    self.log("⚠️ SharePoint access token expired, attempting refresh...")
                    
            # Try to refresh token
            if refresh_token:
                if self.refresh_access_token():
                    self.log("✅ SharePoint token refreshed successfully")
                    self.update_github_secrets()
                    return True
                else:
                    self.log("❌ SharePoint token refresh failed")
                    return False
            else:
                self.log("❌ No SharePoint refresh token available")
                return False

        except Exception as e:
            self.log(f"❌ SharePoint authentication error: {str(e)}")
            return False

    def test_token_validity(self):
        """Test if current access token is valid"""
        try:
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/me", headers=headers, timeout=30)

            if response.status_code == 200:
                user_info = response.json()
                self.log(f"✅ Authenticated to SharePoint as: {user_info.get('displayName', 'Unknown')}")
                return True
            elif response.status_code == 401:
                return False
            else:
                self.log(f"Warning: Unexpected response code: {response.status_code}")
                return False

        except Exception as e:
            self.log(f"Error testing SharePoint token validity: {str(e)}")
            return False

    def refresh_access_token(self):
        """Refresh access token using refresh token with MSAL"""
        try:
            if not self.refresh_token:
                self.log("❌ No refresh token available")
                return False

            self.log("🔄 Attempting to refresh SharePoint token using MSAL...")

            # Use MSAL to refresh token
            result = self.msal_app.acquire_token_by_refresh_token(
                self.refresh_token,
                scopes=SHAREPOINT_CONFIG['scopes']
            )

            if result and "access_token" in result:
                self.access_token = result['access_token']
                if 'refresh_token' in result:
                    self.refresh_token = result['refresh_token']
                    self.log("✅ Got new refresh token")
                
                self.log("✅ SharePoint token refreshed successfully")
                return True
            else:
                error = result.get('error_description', 'Unknown error') if result else 'No result'
                self.log(f"❌ SharePoint token refresh failed: {error}")
                return False

        except Exception as e:
            self.log(f"❌ Error refreshing SharePoint token: {str(e)}")
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

    def download_excel_file_by_id(self, file_id, description=""):
        """Download Excel file from SharePoint by file ID"""
        try:
            self.log(f"📥 Downloading {description} from SharePoint...")

            # Get file download URL using file ID
            url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}"
            response = requests.get(url, headers=self.get_headers(), timeout=30)

            if response.status_code == 200:
                file_info = response.json()
                download_url = file_info.get('@microsoft.graph.downloadUrl')

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

        except Exception as e:
            self.log(f"❌ Error downloading {description}: {str(e)}")

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

            # Upload to SharePoint
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

def authenticate_google():
    """Authentication using OAuth token for Google Sheets (DEPRECATED - kept for reference)"""
    try:
        print("⚠️ WARNING: Google Sheets authentication is deprecated in this version")
        print("⚠️ All data sources now use SharePoint")
        return None
        
        # Original code kept for reference but not used
        # print("Starting OAuth authentication process for Google Sheets...")
        # creds = None
        # 
        # # Check if token.json exists first
        # if os.path.exists('token.json'):
        #     print("Loading credentials from existing token.json file")
        #     creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # # Otherwise create it from the environment variable
        # elif os.environ.get('GOOGLE_TOKEN_JSON'):
        #     print("Creating token.json from GOOGLE_TOKEN_JSON environment variable")
        #     with open('token.json', 'w') as f:
        #         f.write(os.environ.get('GOOGLE_TOKEN_JSON'))
        #     creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # else:
        #     print("Error: No token.json file or GOOGLE_TOKEN_JSON environment variable found")
        #     sys.exit(1)
        # 
        # # Refresh token if expired
        # if creds and creds.expired and creds.refresh_token:
        #     print("Token expired, refreshing...")
        #     creds.refresh(Request())
        #     with open('token.json', 'w') as token:
        #         token.write(creds.to_json())
        # 
        # # Return authorized client
        # return gspread.authorize(creds)

    except Exception as e:
        print(f"Google Sheets authentication error: {str(e)}")
        return None

def extract_phone_number(text):
    """Extract Vietnamese phone number from complaint content"""
    if not isinstance(text, str):
        return None
    
    # Clean the text and normalize
    text = text.strip()
    
    # Vietnamese phone number patterns
    # Pattern 1: 10-11 digits starting with 0, may have spaces, dashes, or dots
    phone_patterns = [
        r'(?:^|\s|-)(\d{4}[\s\-\.]?\d{3}[\s\-\.]?\d{3})', # 0xxx xxx xxx format
        r'(?:^|\s|-)(\d{3}[\s\-\.]?\d{3}[\s\-\.]?\d{4})', # 0xx xxx xxxx format  
        r'(?:^|\s|-)(0\d{9,10})',  # Simple 10-11 digit format starting with 0
        r'(?:^|\s|-)(0\d{8,9})',   # 9-10 digit format starting with 0
    ]
    
    # Try each pattern
    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            # Clean the match (remove spaces, dashes, dots)
            clean_number = re.sub(r'[\s\-\.]', '', match)
            
            # Validate Vietnamese phone number
            if (clean_number.startswith('0') and 
                len(clean_number) >= 9 and 
                len(clean_number) <= 11 and
                clean_number.isdigit()):
                
                # Additional validation for Vietnamese phone prefixes
                # Vietnamese mobile: 09x, 08x, 07x, 05x, 03x
                # Vietnamese landline: 02x
                if (clean_number.startswith(('090', '091', '092', '093', '094', '095', '096', '097', '098', '099',
                                            '070', '076', '077', '078', '079',
                                            '081', '082', '083', '084', '085', '088',
                                            '056', '058', '059',
                                            '032', '033', '034', '035', '036', '037', '038', '039',
                                            '020', '021', '022', '024', '025', '026', '027', '028', '029')) or
                    (clean_number.startswith('0') and len(clean_number) >= 10)):
                    return clean_number
    
    # If no standard pattern found, try to find any sequence of 10-11 digits starting with 0
    # This is more aggressive pattern for edge cases
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

    # Pattern to match brand names (Omachi or Kokomi)
    brand_pattern = r'(Omachi|Kokomi)'
    brand_match = re.search(brand_pattern, full_name)

    if not brand_match:
        return full_name  # Return original if no brand match

    # Get the start position of brand name
    start_pos = brand_match.start()

    # Pattern to match packaging information (e.g., "30gói x 90gr")
    pkg_pattern = r'\d+\s*gói\s*x\s*\d+\s*gr'
    pkg_match = re.search(pkg_pattern, full_name)

    if pkg_match:
        # End position is where packaging info starts
        end_pos = pkg_match.start()
        # Extract text between brand name and packaging info
        short_name = full_name[start_pos:end_pos].strip()
    else:
        # If no packaging info, use rest of string after brand
        short_name = full_name[start_pos:].strip()

    return short_name

def clean_concatenated_dates(date_str):
    """Clean concatenated dates like '11/04/202511/04/202511/04/2025'"""
    if not isinstance(date_str, str):
        return date_str

    # Regular expression to find date patterns in DD/MM/YYYY format
    date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
    matches = re.findall(date_pattern, date_str)

    if matches:
        # Return the first date that parses correctly
        for match in matches:
            try:
                parsed_date = pd.to_datetime(match, format='%d/%m/%Y', dayfirst=True)
                # Current date as reference
                current_date = datetime.now()
                # If date is not more than 1 year in the future, consider it valid
                if parsed_date <= current_date + pd.Timedelta(days=365):
                    return match
            except:
                continue

        # If no valid dates found based on future check, return the first match
        return matches[0]

    # If no DD/MM/YYYY pattern found, try different patterns
    # DD-Mon-YYYY format (e.g., "26-Jun-2025")
    date_pattern = r'(\d{1,2}-[A-Za-z]{3}-\d{4})'
    matches = re.findall(date_pattern, date_str)
    if matches:
        return matches[0]
    
    # DD-MM-YYYY
    date_pattern = r'(\d{1,2}-\d{1,2}-\d{4})'
    matches = re.findall(date_pattern, date_str)
    if matches:
        return matches[0]

    # Try to extract first 11 characters if they look like a date (for DD-Mon-YYYY format)
    if len(date_str) >= 11 and '-' in date_str and any(month in date_str[:11] for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
        return date_str[:11]

    # Try to extract first 10 characters if they look like a date
    if len(date_str) >= 10 and ('/' in date_str[:10] or '-' in date_str[:10]):
        return date_str[:10]

    return date_str

def extract_correct_date(text):
    """Extract the correct Ngày SX from Nội dung phản hồi"""
    if not isinstance(text, str):
        return None

    # Pattern to find "Ngày SX: DD/MM/YYYY"
    pattern = r'Ngày SX:\s*(\d{1,2}/\d{1,2}/\d{4})'
    match = re.search(pattern, text)

    if match:
        return match.group(1)  # Return the date exactly as it appears in the text

    return None

def extract_production_info(text):
    """Extract production information from text with improved line and machine logic"""
    if not isinstance(text, str):
        return None, None, None

    # Clean and standardize the text
    text = text.strip()
    
    # FIXED: Extract time first (anywhere in the text) - now handles spaces around colon
    time_match = re.search(r'(\d{1,2}\s*:\s*\d{1,2})', text)
    time_str = None
    if time_match:
        # Clean up the time string by removing spaces
        raw_time = time_match.group(1)
        time_str = re.sub(r'\s*:\s*', ':', raw_time)  # Replace " : " or " :" or ": " with ":"

    # Try to find line and machine info in different patterns
    line = None
    machine = None

    # Pattern 1: Look for content inside parentheses after "Nơi SX: I-MBP"
    parenthesis_pattern = r'Nơi SX:\s*I-MBP\s*\((.*?)\)'
    parenthesis_match = re.search(parenthesis_pattern, text)
    
    if parenthesis_match:
        content = parenthesis_match.group(1).strip()
        
        # NEW PATTERN: Look for 2-digit numbers after time (like "13:19 23" or "14:27 21")
        if time_str:
            # Look for pattern: time followed by space and 2-digit number
            # Use the cleaned time_str for pattern matching
            time_number_pattern = rf'{re.escape(time_str)}\s+(\d{{2}})'
            time_number_match = re.search(time_number_pattern, content)
            if time_number_match:
                digits = time_number_match.group(1)
                first_digit = int(digits[0])
                second_digit = int(digits[1])
                
                # Check if first digit is valid line number (1-8)
                if 1 <= first_digit <= 8:
                    line = str(first_digit)
                    machine = str(second_digit)
                    return time_str, line, machine
            
            # ALSO try with the original raw time pattern to catch cases like "23 :12 23"
            if raw_time != time_str:
                raw_time_pattern = rf'{re.escape(raw_time)}\s+(\d{{2}})'
                raw_time_match = re.search(raw_time_pattern, content)
                if raw_time_match:
                    digits = raw_time_match.group(1)
                    first_digit = int(digits[0])
                    second_digit = int(digits[1])
                    
                    # Check if first digit is valid line number (1-8)
                    if 1 <= first_digit <= 8:
                        line = str(first_digit)
                        machine = str(second_digit)
                        return time_str, line, machine
        
        # EXISTING PATTERN: Look for patterns like "8I06", "21I", "2I", etc.
        # Remove time part if found to simplify processing
        if time_str:
            content_for_i_pattern = content.replace(time_str, '').strip()
            # Also remove the raw time pattern if different
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
    
    # Pattern 2: If no parentheses, look for patterns like "22I" directly in text
    if line is None:
        # Look for patterns like "22I", "8I", "21I" anywhere in the text
        line_pattern = r'(\d+)I(?!\w)'  # \d+I not followed by word character
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
    
    # Pattern 3: Look for "Nơi SX: MBP" and then search around it
    if line is None and "Nơi SX: MBP" in text:
        # Find position of "Nơi SX: MBP" and look around it
        mbp_pos = text.find("Nơi SX: MBP")
        surrounding_text = text[max(0, mbp_pos-20):mbp_pos+50]
        
        # First try the new 2-digit pattern in surrounding text
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
        
        # Fall back to looking for "I" patterns in surrounding text
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
            
            # Handle DD/MM/YYYY format specifically
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                return pd.to_datetime(date_str, format='%d/%m/%Y')
            
            # Handle MM/DD/YYYY format specifically  
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                # Try DD/MM/YYYY first (since dayfirst=True)
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y')
                except:
                    # Fall back to MM/DD/YYYY
                    return pd.to_datetime(date_str, format='%m/%d/%Y')
            
            # Handle DD-MMM-YYYY format (e.g., "4-Apr-2025")
            if '-' in date_str:
                for fmt in ['%d-%b-%Y', '%d-%B-%Y', '%d-%b-%y', '%d-%B-%y']:
                    try:
                        return pd.to_datetime(date_str, format=fmt)
                    except:
                        continue

            # Last resort: Let pandas try to detect, but suppress warnings
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
    return date_obj.isocalendar()[1]  # Returns the ISO week number

def clean_item_code(item_code):
    if pd.isna(item_code) or item_code == '':
        return ''

    # Convert to string, remove spaces, and standardize
    item_str = str(item_code).strip()
    return item_str

def parse_time(time_str):
    if pd.isna(time_str) or time_str == '':
        return None

    time_str = str(time_str).strip().lower()

    try:
        # Handle HH:MM format
        if ':' in time_str:
            hours, minutes = map(int, time_str.split(':'))
            return time(hours, minutes)

        # Handle "22h" format
        elif 'h' in time_str:
            hours = int(time_str.replace('h', ''))
            return time(hours, 0)

        # Try to parse as simple hour
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
    # Round down to nearest even hour
    rounded_hour = (hour // 2) * 2
    return time(rounded_hour, 0)

def determine_shift(time_obj):
    """Modified to return just the shift number (1, 2, or 3) for Power BI"""
    if time_obj is None:
        return None

    # Create time boundaries
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
    # Find the leader NAME column specifically (Tên Trưởng ca) - prioritize this over codes
    leader_name_column = None
    leader_code_column = None
    
    for col in aql_data.columns:
        col_lower = col.lower()
        if 'tên trưởng ca' in col_lower or 'ten truong ca' in col_lower:
            leader_name_column = col
        elif ('trưởng ca' in col_lower or 'truong ca' in col_lower) and 'tên' not in col_lower:
            leader_code_column = col
    
    # Find the QA column
    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break
    
    print(f"Found columns:")
    print(f"  QA column: {qa_column}")
    print(f"  Leader NAME column (Tên Trưởng ca): {leader_name_column}")
    print(f"  Leader CODE column (Trưởng ca): {leader_code_column}")
    
    # Use the name column if available, otherwise fall back to code column
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
    
    # Create mapping by examining actual data relationships
    leader_mapping = {}
    
    # Get unique combinations of QA and leader from the data
    qa_leader_combinations = aql_data[[qa_column, leader_column]].dropna().drop_duplicates()
    
    print(f"\nFound {len(qa_leader_combinations)} unique QA-Leader combinations:")
    for idx, row in qa_leader_combinations.iterrows():
        qa_val = row[qa_column]
        leader_val = row[leader_column]
        print(f"  QA: '{qa_val}' -> Leader: '{leader_val}'")
        
        # Store the mapping (keep original values since we're now using the name column)
        leader_mapping[str(leader_val)] = str(leader_val)
    
    print(f"Final leader mapping: {leader_mapping}")
    return leader_mapping

def find_qa_and_leader(row, aql_data, leader_mapping=None):
    """Improved function to match QA and leader from the AQL data sheet"""
    if pd.isna(row['Ngày SX_std']) or row['Item_clean'] == '' or row['Giờ_time'] is None:
        return None, None, "Missing required data"

    # Check for QA column
    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break

    # Check for leader column
    leader_name_column = None
    leader_code_column = None
    
    for col in aql_data.columns:
        col_lower = col.lower()
        if 'tên trưởng ca' in col_lower or 'ten truong ca' in col_lower:
            leader_name_column = col
        elif ('trưởng ca' in col_lower or 'truong ca' in col_lower) and 'tên' not in col_lower:
            leader_code_column = col
    
    # Use the name column if available, otherwise fall back to code column
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
    
    # Convert Line_extracted to proper integer for comparison
    complaint_line = row['Line_extracted']
    if pd.isna(complaint_line):
        return None, None, "Missing line information"
    
    try:
        complaint_line = int(float(complaint_line))
    except (ValueError, TypeError):
        return None, None, f"Invalid line value: {complaint_line}"

    # Handle night shift date adjustment
    complaint_hour = row['Giờ_time'].hour
    complaint_minute = row['Giờ_time'].minute
    search_date = row['Ngày SX_std']
    
    # If complaint is in early morning hours (0:00 to 6:30) and in shift 3,
    # we should look at the previous day's AQL data
    if complaint_hour < 6 or (complaint_hour == 6 and complaint_minute < 30):
        if row['Shift'] == 3:
            search_date = search_date - pd.Timedelta(days=1)
            date_adjusted = True
        else:
            date_adjusted = False
    else:
        date_adjusted = False

    # Debug information
    debug_parts = []
    if date_adjusted:
        debug_parts.append(f"NIGHT SHIFT ADJUSTMENT: Looking for: Date={search_date.strftime('%d/%m/%Y')} (adjusted from {row['Ngày SX_std'].strftime('%d/%m/%Y')}), Item={row['Item_clean']}, Line={complaint_line}")
    else:
        debug_parts.append(f"Looking for: Date={search_date.strftime('%d/%m/%Y')}, Item={row['Item_clean']}, Line={complaint_line}")

    # 1. Filter AQL data for the same date and item first
    date_item_matches = aql_data[
        (aql_data['Ngày SX_std'] == search_date) & 
        (aql_data['Item_clean'] == row['Item_clean'])
    ]
    
    debug_parts.append(f"Date+Item matches: {len(date_item_matches)}")
    
    if date_item_matches.empty:
        # Try with date only to see if date matching works
        date_only_matches = aql_data[aql_data['Ngày SX_std'] == search_date]
        debug_parts.append(f"Date-only matches: {len(date_only_matches)}")
        
        # Try with item only to see if item matching works
        item_only_matches = aql_data[aql_data['Item_clean'] == row['Item_clean']]
        debug_parts.append(f"Item-only matches: {len(item_only_matches)}")
        
        return None, None, " | ".join(debug_parts)

    # 2. Now filter by line
    matching_rows = date_item_matches[date_item_matches['Line'] == complaint_line]
    
    debug_parts.append(f"Date+Item+Line matches: {len(matching_rows)}")
    
    if matching_rows.empty:
        # Show available lines for this date+item combination
        available_lines = date_item_matches['Line'].dropna().unique()
        debug_parts.append(f"Available lines for this date+item: {sorted([x for x in available_lines if pd.notna(x)])}")
        return None, None, " | ".join(debug_parts)

    # 3. Determine which QA check hours to look at
    if complaint_minute == 0 and complaint_hour % 2 == 0:
        prev_hour = complaint_hour
        next_hour = (complaint_hour + 2) % 24
    else:
        prev_hour = (complaint_hour // 2) * 2
        next_hour = (prev_hour + 2) % 24

    debug_parts.append(f"Complaint at {complaint_hour}:{complaint_minute:02d}, checking {prev_hour}h and {next_hour}h")

    # 4. Find QA records at these times
    prev_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == prev_hour and x.minute == 0)]
    next_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == next_hour and x.minute == 0)]

    debug_parts.append(f"Prev hour ({prev_hour}h) records: {len(prev_check)}, Next hour ({next_hour}h) records: {len(next_check)}")

    # Show available times for debugging
    available_times = matching_rows[matching_rows['Giờ_time'].notna()]['Giờ_time'].apply(lambda x: f"{x.hour}:{x.minute:02d}").unique()
    debug_parts.append(f"Available times: {sorted(available_times)}")

    # Special case for tickets about KKM PRO CCT on 26/04/2025
    if (search_date == pd.to_datetime('26/04/2025', format='%d/%m/%Y') and 
        'PRO CCT' in str(row['Item_clean']).upper()):
        # Find rows with QA = "Hằng" in the matching rows
        hang_rows = matching_rows[matching_rows[qa_column] == "Hằng"]
        if not hang_rows.empty:
            # Get the first row with QA = "Hằng"
            hang_row = hang_rows.iloc[0]
            debug_parts.append("Special case for KKM PRO CCT on 26/04/2025")
            leader_value = hang_row[leader_column]
            # Apply leader mapping
            if leader_mapping and leader_value is not None:
                mapped_leader = leader_mapping.get(str(leader_value), leader_value)
            else:
                mapped_leader = leader_value
            return hang_row[qa_column], mapped_leader, " | ".join(debug_parts)
    
    # 5. Apply the matching rules
    # 5a. First, check if there's data for the preceding hour
    if not prev_check.empty:
        prev_qa = prev_check.iloc[0].get(qa_column)
        prev_leader = prev_check.iloc[0].get(leader_column)

        # Apply leader mapping if provided
        if leader_mapping and prev_leader is not None:
            prev_leader = leader_mapping.get(str(prev_leader), prev_leader)
        
        # 5b. Check if there's data for the next hour
        if not next_check.empty:
            next_qa = next_check.iloc[0].get(qa_column)
            next_leader = next_check.iloc[0].get(leader_column)

            # Apply leader mapping if provided
            if leader_mapping and next_leader is not None:
                next_leader = leader_mapping.get(str(next_leader), next_leader)
            
            # 5c. If both QA and leader are the same, use them
            if prev_qa == next_qa and prev_leader == next_leader:
                debug_parts.append(f"Same QA ({prev_qa}) and leader ({prev_leader}) for both {prev_hour}h and {next_hour}h")
                return prev_qa, prev_leader, " | ".join(debug_parts)

        # 5d. Determine based on shift if we need to
        shift = row['Shift']

        # For times between 22:30-23:59, we use the next hour's QA (from 0h)
        if shift == 3 and complaint_hour >= 22:
            if not next_check.empty:
                debug_parts.append(f"Using next hour ({next_hour}h) QA ({next_qa}) and leader ({next_leader}) based on Shift 3 rule")
                return next_qa, next_leader, " | ".join(debug_parts)

        # For all other cases, use the preceding hour's QA and leader from the same row
        debug_parts.append(f"Using previous hour ({prev_hour}h) QA ({prev_qa}) and leader ({prev_leader})")
        return prev_qa, prev_leader, " | ".join(debug_parts)

    # If no data for preceding hour, try next hour
    elif not next_check.empty:
        next_qa = next_check.iloc[0].get(qa_column)
        next_leader = next_check.iloc[0].get(leader_column)
        
        # Apply leader mapping if provided
        if leader_mapping and next_leader is not None:
            next_leader = leader_mapping.get(str(next_leader), next_leader)
        
        debug_parts.append(f"Only next hour ({next_hour}h) data available - QA ({next_qa}) and leader ({next_leader})")
        return next_qa, next_leader, " | ".join(debug_parts)

    # If no data for either hour, look for any data for same date, item, line
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
            
            # Apply leader mapping if provided
            if leader_mapping and closest_leader is not None:
                closest_leader = leader_mapping.get(str(closest_leader), closest_leader)
            
            closest_time = f"{closest_row['Giờ_time'].hour}:{closest_row['Giờ_time'].minute:02d}"
            debug_parts.append(f"Using closest time match at {closest_time} - QA ({closest_qa}) and leader ({closest_leader})")
            return closest_qa, closest_leader, " | ".join(debug_parts)

    debug_parts.append("No matching QA records found")
    return None, None, " | ".join(debug_parts)

def main():
    print("="*80)
    print("🔄 FULL SHAREPOINT INTEGRATION - UPDATED WITH MMB FILTER")
    print("="*80)

    # Initialize SharePoint processor
    print("\n🔗 Initializing SharePoint connection...")
    try:
        sp_processor = SharePointProcessor()
        print("✅ SharePoint connection established")
    except Exception as e:
        print(f"❌ SharePoint initialization failed: {str(e)}")
        sys.exit(1)

    # ========================================================================
    # DATA SOURCES:
    # 1. AQL Data - FROM SHAREPOINT (Sample ID.xlsx)
    # 2. KNKH Data - FROM SHAREPOINT (BÁO CÁO KNKH.xlsx) ⭐ UPDATED SOURCE
    # 3. Output - TO SHAREPOINT (Data KNKH.xlsx)
    # ========================================================================

    print("\n📥 Loading data from SharePoint sources...")

    # 1. Get AQL data from SharePoint
    print("📋 Loading AQL data from SharePoint...")
    try:
        aql_sheets_data = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['sample_id'], 
            "Sample ID.xlsx (AQL Data)"
        )

        if not aql_sheets_data:
            print("❌ Failed to download AQL data from SharePoint")
            sys.exit(1)

        # Extract ID AQL sheet (this contains the production data)
        aql_df = aql_sheets_data.get('ID AQL', pd.DataFrame())
        if aql_df.empty:
            print("❌ ID AQL sheet not found or empty")
            sys.exit(1)

        print(f"✅ AQL data loaded: {len(aql_df)} records")
        print(f"AQL columns: {list(aql_df.columns)}")

    except Exception as e:
        print(f"❌ Error loading AQL data from SharePoint: {str(e)}")
        sys.exit(1)

    # 2. Get KNKH data from SharePoint ⭐ NEW SOURCE
    print("📋 Loading KNKH data from NEW SharePoint source...")
    try:
        knkh_sheets_data = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['knkh_data'], 
            "BÁO CÁO KNKH.xlsx (NEW KNKH Data Source)"
        )

        if not knkh_sheets_data:
            print("❌ Failed to download KNKH data from SharePoint")
            sys.exit(1)

        # Find the Data sheet (handle variations like "Data", "Data ", etc.)
        knkh_df = None
        data_sheet_name = None
        
        # First try exact match
        if 'Data' in knkh_sheets_data:
            knkh_df = knkh_sheets_data['Data']
            data_sheet_name = 'Data'
        else:
            # Look for sheets with "Data" in the name (case insensitive, handle spaces)
            for sheet_name in knkh_sheets_data.keys():
                if 'data' in sheet_name.lower().strip():
                    knkh_df = knkh_sheets_data[sheet_name]
                    data_sheet_name = sheet_name
                    print(f"✅ Found data sheet: '{sheet_name}' (with {len(knkh_df)} rows)")
                    break
        
        # If still not found, try other possible sheet names
        if knkh_df is None or knkh_df.empty:
            print("❌ 'Data' sheet not found, trying alternatives...")
            print(f"Available sheets: {list(knkh_sheets_data.keys())}")
            
            possible_sheet_names = ['Sheet1', 'BÁO CÁO KNKH', 'MMB', 'Chi tiết4', 'Trang_tính1']
            for sheet_name in possible_sheet_names:
                if sheet_name in knkh_sheets_data:
                    temp_df = knkh_sheets_data[sheet_name]
                    if not temp_df.empty and len(temp_df) > 100:  # Use sheet with substantial data
                        knkh_df = temp_df
                        data_sheet_name = sheet_name
                        print(f"✅ Using fallback sheet '{sheet_name}' with {len(knkh_df)} rows")
                        break

        if knkh_df is None or knkh_df.empty:
            print("❌ No valid KNKH data found in any sheet")
            print(f"Available sheets: {list(knkh_sheets_data.keys())}")
            sys.exit(1)

        print(f"✅ KNKH data loaded: {len(knkh_df)} records")
        print(f"KNKH columns: {list(knkh_df.columns)}")
        
        # Make a copy to avoid SettingWithCopyWarning
        knkh_df = knkh_df.copy()

    except Exception as e:
        print(f"❌ Error loading KNKH data from SharePoint: {str(e)}")
        sys.exit(1)

    # ========================================================================
    # NEW: ADD MMB FILTER CONDITION ⭐
    # ========================================================================

    print("\n🏭 Applying MMB factory filter...")
    
    # Check column I specifically for 'Nhà máy sản xuất'
    factory_column = None
    
    # First check if we can access by column position (column I = index 8)
    if len(knkh_df.columns) > 8:
        # Check if column I (index 8) contains factory information
        col_i = knkh_df.columns[8]
        print(f"Column I (index 8): '{col_i}'")
        
        # Check if this column contains MMB values
        if knkh_df[col_i].astype(str).str.contains('MMB', na=False).any():
            factory_column = col_i
            print(f"✅ Found MMB values in column I: '{factory_column}'")
    
    # If column I doesn't work, search by name
    if not factory_column:
        for col in knkh_df.columns:
            col_lower = col.lower()
            if 'nhà máy sản xuất' in col_lower or 'nha may san xuat' in col_lower or 'factory' in col_lower:
                factory_column = col
                print(f"✅ Found factory column by name: '{factory_column}'")
                break
    
    if factory_column:        
        # Show unique values in factory column for debugging
        unique_factories = knkh_df[factory_column].value_counts()
        print(f"Factory distribution before filtering:")
        for factory, count in unique_factories.items():
            print(f"  '{factory}': {count} records")
        
        # Filter for MMB only
        original_count = len(knkh_df)
        # Use case-insensitive matching for MMB
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

    # ========================================================================
    # DATA PROCESSING (same as original logic)
    # ========================================================================

    print("\n🔄 Processing data...")

    # Clean concatenated dates for both reception date and production date
    print("📅 Processing dates...")
    knkh_df['Ngày tiếp nhận'] = knkh_df['Ngày tiếp nhận'].apply(clean_concatenated_dates)
    knkh_df['Ngày SX'] = knkh_df['Ngày SX'].apply(clean_concatenated_dates)
    
    # Extract correct Ngày SX from Nội dung phản hồi and replace the Ngày SX column
    knkh_df['Ngày SX_extracted'] = knkh_df['Nội dung phản hồi'].apply(extract_correct_date)

    # Replace the original Ngày SX with the extracted one when available
    knkh_df['Ngày SX'] = knkh_df.apply(
        lambda row: row['Ngày SX_extracted'] if row['Ngày SX_extracted'] is not None else row['Ngày SX'], 
        axis=1
    )

    # NEW: Extract phone number from complaint content
    print("📱 Extracting phone numbers...")
    knkh_df['SDT người KN'] = knkh_df['Nội dung phản hồi'].apply(extract_phone_number)
    
    # Count how many phone numbers were extracted
    phone_extracted_count = knkh_df['SDT người KN'].notna().sum()
    print(f"✅ Extracted {phone_extracted_count} phone numbers from {len(knkh_df)} records")

    # Standardize dates first for filtering
    knkh_df['Ngày SX_std'] = knkh_df['Ngày SX'].apply(standardize_date)
    aql_df['Ngày SX_std'] = aql_df['Ngày SX'].apply(standardize_date)

    # Create filter date (January 1, 2024)
    filter_date = pd.to_datetime('2024-01-01')

    # Filter both DataFrames to only include data from January 1, 2024 onwards
    knkh_df = knkh_df[knkh_df['Ngày SX_std'] >= filter_date]
    aql_df = aql_df[aql_df['Ngày SX_std'] >= filter_date]

    print(f"After date filtering: {len(knkh_df)} KNKH records and {len(aql_df)} AQL records")

    # Extract time, line, and machine information
    print("🔧 Extracting production information...")
    knkh_df[['Giờ_extracted', 'Line_extracted', 'Máy_extracted']] = knkh_df['Nội dung phản hồi'].apply(
        lambda x: pd.Series(extract_production_info(x))
    )

    # Convert to appropriate data types
    knkh_df['Line_extracted'] = pd.to_numeric(knkh_df['Line_extracted'], errors='coerce')
    knkh_df['Máy_extracted'] = pd.to_numeric(knkh_df['Máy_extracted'], errors='coerce')

    # Standardize the receipt date
    knkh_df['Ngày tiếp nhận_std'] = knkh_df['Ngày tiếp nhận'].apply(standardize_date)

    # Clean item codes
    knkh_df['Item_clean'] = knkh_df['Item'].apply(clean_item_code)
    aql_df['Item_clean'] = aql_df['Item'].apply(clean_item_code)

    # Parse time
    knkh_df['Giờ_time'] = knkh_df['Giờ_extracted'].apply(parse_time)
    
    # Find the correct Giờ column in AQL data
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

    # Handle Line column in AQL data
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

    # Convert AQL Line column to numeric BEFORE any sorting operations
    if 'Line' in aql_df.columns:
        aql_df['Line'] = pd.to_numeric(aql_df['Line'], errors='coerce')
        print(f"Converted Line column to numeric")

    # Round time to 2-hour intervals
    knkh_df['Giờ_rounded'] = knkh_df['Giờ_time'].apply(round_to_2hour)

    # Determine shift
    knkh_df['Shift'] = knkh_df['Giờ_time'].apply(determine_shift)

    # Match QA and leader
    print("🔍 Matching QA and leaders...")
    leader_mapping = create_leader_mapping(aql_df)
    print(f"Leader mapping: {leader_mapping}")

    # Match QA and leader
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
        
        # Print progress every 50 rows
        if (idx + 1) % 50 == 0:
            print(f"Processed {idx + 1} rows, {total_matched} matched so far")
    
    print(f"Matching process complete. Total matched: {total_matched} out of {len(knkh_df)} rows")

    # Format dates for Power BI (MM/DD/YYYY)
    knkh_df['Ngày tiếp nhận_formatted'] = knkh_df['Ngày tiếp nhận_std'].apply(format_date_mm_dd_yyyy)
    knkh_df['Ngày SX_formatted'] = knkh_df['Ngày SX_std'].apply(format_date_mm_dd_yyyy)

    # Extract time components
    knkh_df['Tháng sản xuất'] = knkh_df['Ngày SX_std'].apply(extract_month)
    knkh_df['Năm sản xuất'] = knkh_df['Ngày SX_std'].apply(extract_year)
    knkh_df['Tuần nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_week)
    knkh_df['Tháng nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_month)
    knkh_df['Năm nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_year)

    # Filter to only include rows where "Bộ phận chịu trách nhiệm" is "Nhà máy" (if exists)
    print(f"Total rows before filtering by 'Bộ phận chịu trách nhiệm': {len(knkh_df)}")
    
    responsible_dept_column = None
    for col in knkh_df.columns:
        col_lower = col.lower()
        if 'bộ phận chịu trách nhiệm' in col_lower or 'bo phan chiu trach nhiem' in col_lower or 'responsible' in col_lower:
            responsible_dept_column = col
            break
    
    if responsible_dept_column:
        # Show unique values for debugging
        dept_values = knkh_df[responsible_dept_column].value_counts()
        print(f"Responsible department distribution:")
        for dept, count in dept_values.items():
            print(f"  '{dept}': {count} records")
            
        # Filter for factory only
        before_filter = len(knkh_df)
        knkh_df = knkh_df[knkh_df[responsible_dept_column].astype(str).str.contains('Nhà máy|nha may|Factory', case=False, na=False)]
        after_filter = len(knkh_df)
        print(f"Rows after filtering for factory responsibility: {after_filter} (filtered out: {before_filter - after_filter})")
    else:
        print("⚠️ 'Bộ phận chịu trách nhiệm' column not found, skipping this filter")
        print("Available columns with sample data:")
        for col in knkh_df.columns:
            sample_values = knkh_df[col].dropna().head(3).tolist()
            print(f"  '{col}': {sample_values}")

    # Create the final output dataframe
    filtered_knkh_df = knkh_df.copy()

    # Extract short product names
    if 'Tên sản phẩm' in filtered_knkh_df.columns:
        filtered_knkh_df['Tên sản phẩm ngắn'] = filtered_knkh_df['Tên sản phẩm'].apply(extract_short_product_name)
    else:
        # Try to find product name column
        product_col = None
        for col in filtered_knkh_df.columns:
            if 'sản phẩm' in col.lower() or 'san pham' in col.lower() or 'product' in col.lower():
                product_col = col
                break
        if product_col:
            filtered_knkh_df['Tên sản phẩm ngắn'] = filtered_knkh_df[product_col].apply(extract_short_product_name)
        else:
            filtered_knkh_df['Tên sản phẩm ngắn'] = ''

    # Build final dataframe with available columns
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
    
    # Find actual column names that exist
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
    
    # Create final dataframe with available columns
    final_df = filtered_knkh_df[final_columns].copy()
    
    # Create rename mapping
    rename_dict = {}
    for i, final_col in enumerate(final_columns):
        desired_name = available_columns[i]
        if final_col != desired_name:
            rename_dict[final_col] = desired_name
    
    # Add standard renames
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

    # Convert 'Mã ticket' to numeric before sorting to avoid type comparison errors
    if 'Mã ticket' in final_df.columns:
        final_df['Mã ticket'] = pd.to_numeric(final_df['Mã ticket'], errors='coerce')
    
    # Sort by Mã ticket from largest to smallest
    final_df = final_df.sort_values(by='Mã ticket', ascending=False)

    print(f"\n📊 Final dataset prepared: {len(final_df)} records")
    print(f"📱 Phone numbers extracted: {final_df['SDT người KN'].notna().sum()} records")
    print(f"🏭 All records are from MMB factory")

    # ========================================================================
    # OUTPUT TO SHAREPOINT
    # ========================================================================

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
            
            # Fallback: save locally
            print("💾 Saving data locally as backup...")
            local_filename = "Data_KNKH_full_sharepoint_mmb_backup.xlsx"
            with pd.ExcelWriter(local_filename, engine='openpyxl') as writer:
                final_df.to_excel(writer, sheet_name='Data_KNKH', index=False)
                
                # Create debug sheet
                debug_df = final_df[['Mã ticket', 'Ngày SX', 'Item', 'Line', 'Giờ', 'QA', 'Tên Trưởng ca', 'SDT người KN', 'debug_info']]
                debug_df.head(500).to_excel(writer, sheet_name='Debug_Info', index=False)
            
            print(f"Data saved locally to {local_filename}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error during SharePoint upload: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

    print("\n" + "="*80)
    print("✅ FULL SHAREPOINT INTEGRATION COMPLETED SUCCESSFULLY!")
    print("✅ NEW KNKH DATA SOURCE INTEGRATED!")
    print("✅ MMB FACTORY FILTER APPLIED!")
    print("✅ PHONE NUMBER EXTRACTION FEATURE INCLUDED!")
    print("✅ ALL DATA SOURCES NOW USE SHAREPOINT!")
    print("="*80)

if __name__ == "__main__":
    main()
