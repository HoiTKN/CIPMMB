import os
import sys
import io
import requests
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
import msal
import base64
import traceback
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# SharePoint Configuration
SHAREPOINT_CONFIG = {
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Sites.ReadWrite.All'],
    'site_name': 'MCH.MMB.QA',
    'base_url': 'masangroup.sharepoint.com'
}

# SharePoint File ID for "CIP plan.xlsx"
CIP_PLAN_FILE_ID = '8C90FB38-DA8C-59CC-547D-53BEA1C8B16D'

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

class SharePointCIPProcessor:
    """SharePoint integration for CIP Cleaning automation"""
    
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

    def download_excel_file(self):
        """Download Excel file from SharePoint"""
        try:
            self.log(f"📥 Downloading CIP plan file from SharePoint...")

            # Get file download URL using file ID
            url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{CIP_PLAN_FILE_ID}"
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
                            
                            self.log(f"✅ Successfully downloaded CIP plan file")
                            return sheets_data
                            
                        except Exception as e:
                            self.log(f"❌ Error reading Excel file: {str(e)}")
                            return None
                    else:
                        self.log(f"❌ Error downloading file content: {file_response.status_code}")
                else:
                    self.log(f"❌ No download URL found for CIP plan file")
            else:
                self.log(f"❌ Error getting file info: {response.status_code}")

        except Exception as e:
            self.log(f"❌ Error downloading CIP plan file: {str(e)}")

        return None

    def upload_excel_file(self, sheets_data):
        """Upload updated Excel file back to SharePoint with retry logic for locked files"""
        max_retries = 5
        retry_delay = 30  # seconds
        
        try:
            self.log(f"📤 Uploading updated CIP plan to SharePoint...")

            # Create Excel file in memory with multiple sheets
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                for sheet_name, df in sheets_data.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            excel_buffer.seek(0)
            excel_content = excel_buffer.getvalue()
            self.log(f"Created Excel file with {len(excel_content)} bytes")

            # Upload to SharePoint with retry logic
            upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{CIP_PLAN_FILE_ID}/content"

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            for attempt in range(max_retries):
                try:
                    self.log(f"Upload attempt {attempt + 1}/{max_retries}")
                    
                    response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)

                    if response.status_code in [200, 201]:
                        self.log(f"✅ Successfully uploaded updated CIP plan to SharePoint")
                        return True
                    elif response.status_code == 423:
                        # File is locked
                        self.log(f"⚠️ File is locked (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            self.log(f"⏳ Waiting {retry_delay} seconds before retry...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            self.log(f"❌ File remains locked after {max_retries} attempts")
                            # Try to save to a backup location or with different name
                            return self.upload_backup_file(excel_content)
                    elif response.status_code == 401:
                        # Token expired, try refresh
                        self.log("🔄 Token expired during upload, refreshing...")
                        if self.refresh_access_token():
                            self.update_github_secrets()
                            headers['Authorization'] = f'Bearer {self.access_token}'
                            continue
                        else:
                            self.log("❌ Token refresh failed during upload")
                            return False
                    else:
                        self.log(f"❌ Error uploading to SharePoint: {response.status_code}")
                        self.log(f"Response: {response.text[:500]}")
                        if attempt < max_retries - 1:
                            self.log(f"⏳ Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            return False

                except requests.exceptions.RequestException as e:
                    self.log(f"❌ Network error during upload: {str(e)}")
                    if attempt < max_retries - 1:
                        self.log(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        return False

            return False

        except Exception as e:
            self.log(f"❌ Error uploading to SharePoint: {str(e)}")
            return False

    def upload_backup_file(self, excel_content):
        """Upload to a backup file when original is locked"""
        try:
            # Generate backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"CIP_plan_backup_{timestamp}.xlsx"
            
            self.log(f"🔄 Uploading to backup file: {backup_filename}")
            
            # Upload to the same folder but with different name
            # First get the parent folder
            file_info_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{CIP_PLAN_FILE_ID}"
            response = requests.get(file_info_url, headers=self.get_headers(), timeout=30)
            
            if response.status_code == 200:
                file_info = response.json()
                parent_id = file_info.get('parentReference', {}).get('id')
                
                if parent_id:
                    # Upload to parent folder with new name
                    upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{parent_id}:/{backup_filename}:/content"
                    
                    headers = {
                        'Authorization': f'Bearer {self.access_token}',
                        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    }
                    
                    response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)
                    
                    if response.status_code in [200, 201]:
                        self.log(f"✅ Successfully uploaded backup file: {backup_filename}")
                        self.log(f"⚠️ Original file was locked, please check and rename backup file manually")
                        return True
                    else:
                        self.log(f"❌ Failed to upload backup file: {response.status_code}")
                        return False
                else:
                    self.log(f"❌ Could not get parent folder information")
                    return False
            else:
                self.log(f"❌ Could not get file information for backup: {response.status_code}")
                return False
                
        except Exception as e:
            self.log(f"❌ Error uploading backup file: {str(e)}")
            return False

    def update_sheet_data(self, sheet_name, df):
        """Update specific sheet data in SharePoint Excel file"""
        # For now, we'll update the entire file. 
        # In future, could implement partial sheet updates if needed
        pass

# Helper function to parse dates in different formats
def parse_date(date_str):
    """Try to parse date with multiple formats and handle Excel date formats"""
    if not date_str or str(date_str).strip() in ['nan', 'None', '', 'NaT']:
        return None
    
    # If it's already a datetime object, return it
    if isinstance(date_str, datetime):
        return date_str
    
    # If it's a pandas timestamp, convert it
    if hasattr(date_str, 'to_pydatetime'):
        try:
            return date_str.to_pydatetime()
        except:
            pass
    
    # Convert to string and clean
    date_str = str(date_str).strip()
    
    # Handle Excel serial dates (numbers like 45123.0)
    try:
        # If it's a number that could be an Excel date
        if date_str.replace('.', '').isdigit():
            excel_date = float(date_str)
            # Excel date serial numbers are typically > 1 and < 50000 for reasonable dates
            if 1 < excel_date < 50000:
                # Excel epoch is 1900-01-01 (with some quirks)
                excel_epoch = datetime(1900, 1, 1)
                # Excel incorrectly treats 1900 as a leap year, so subtract 2 days
                return excel_epoch + timedelta(days=excel_date - 2)
    except (ValueError, TypeError):
        pass
    
    # Try various date formats
    date_formats = [
        '%B %d, %Y',     # June 7, 2025
        '%d/%m/%Y',      # 07/06/2025
        '%m/%d/%Y',      # 06/07/2025
        '%Y-%m-%d',      # 2025-06-07
        '%d-%m-%Y',      # 07-06-2025
        '%d %B %Y',      # 7 June 2025
        '%d %B, %Y',     # 7 June, 2025
        '%d/%m/%y',      # 07/06/25
        '%m/%d/%y',      # 06/07/25
        '%d-%m-%y',      # 07-06-25
        '%d.%m.%Y',      # 07.06.2025
        '%d.%m.%y',      # 07.06.25
        '%Y/%m/%d',      # 2025/06/07
        '%d-%b-%Y',      # 07-Jun-2025
        '%d-%b-%y',      # 07-Jun-25
        '%d %b %Y',      # 7 Jun 2025
        '%d %b %y',      # 7 Jun 25
        '%Y-%m-%d %H:%M:%S',  # 2025-06-07 00:00:00
        '%B %d %Y',      # June 7 2025 (no comma)
        '%b %d, %Y',     # Jun 7, 2025
        '%b %d %Y',      # Jun 7 2025
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            # Sanity check - date should be between 1900 and 2100
            if 1900 <= parsed_date.year <= 2100:
                return parsed_date
        except (ValueError, TypeError):
            continue
    
    # Try pandas to_datetime as last resort
    try:
        import pandas as pd
        parsed_date = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
        if not pd.isna(parsed_date):
            return parsed_date.to_pydatetime()
    except:
        pass
    
    print(f"⚠️ Warning: Could not parse date: '{date_str}'")
    return None

# Main function to update cleaning schedule using SharePoint
def update_cleaning_schedule():
    print("Đang cập nhật lịch vệ sinh từ SharePoint...")
    
    # Initialize SharePoint processor
    processor = SharePointCIPProcessor()
    
    # Download Excel file from SharePoint
    sheets_data = processor.download_excel_file()
    if not sheets_data:
        print("❌ Failed to download CIP plan file")
        return []
    
    # Get or create sheets data
    master_plan_df = sheets_data.get('Master plan', pd.DataFrame())
    cleaning_history_df = sheets_data.get('Cleaning History', pd.DataFrame())
    actual_result_df = sheets_data.get('Actual result', pd.DataFrame())
    
    # Initialize sheets if empty
    if master_plan_df.empty:
        headers = ['Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)', 
                'Ngày vệ sinh gần nhất', 'Ngày kế hoạch vệ sinh tiếp theo', 'Trạng thái', 'Đang chứa sản phẩm']
        master_plan_df = pd.DataFrame(columns=headers)
        sheets_data['Master plan'] = master_plan_df
    
    if cleaning_history_df.empty:
        history_headers = ['Khu vực', 'Thiết bị', 'Phương pháp', 
                        'Tần suất (ngày)', 'Ngày vệ sinh', 'Người thực hiện']
        cleaning_history_df = pd.DataFrame(columns=history_headers)
        sheets_data['Cleaning History'] = cleaning_history_df
    
    if actual_result_df.empty:
        actual_headers = ['Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)', 
                           'Ngày vệ sinh', 'Người thực hiện', 'Kết quả', 'Ghi chú']
        actual_result_df = pd.DataFrame(columns=actual_headers)
        sheets_data['Actual result'] = actual_result_df
    
    # Process Master plan data
    today = datetime.today()
    updated_values = []
    
    # Initialize counters FIRST - CRITICAL!
    processed_count = 0
    status_counts = {'Bình thường': 0, 'Sắp đến hạn': 0, 'Đến hạn': 0, 'Quá hạn': 0, 'Chưa có dữ liệu': 0, 'Lỗi': 0}
    
    # Check if data already has status column - maybe we don't need to calculate
    print(f"🔍 Checking existing status data...")
    
    # Check if there's already a status column with data
    existing_status_col = None
    for col in master_plan_df.columns:
        col_lower = str(col).lower().strip()
        if 'trạng thái' in col_lower:
            existing_status_col = col
            break
    
    if existing_status_col:
        print(f"✅ Found existing status column: '{existing_status_col}'")
        existing_statuses = master_plan_df[existing_status_col].value_counts()
        print(f"📊 Existing status breakdown:")
        for status, count in existing_statuses.items():
            print(f"  - '{status}': {count}")
        
        # If we already have status data, let's use it and just update dates
        use_existing_status = True
        print(f"🔄 Using existing status data instead of recalculating")
    else:
        print(f"⚠️ No existing status column found, will calculate status")
        use_existing_status = False
    
    for idx, row in master_plan_df.iterrows():
        try:
            # Get values, handle missing columns with more flexible column name matching
            area = ''
            device = ''
            method = ''
            freq_str = ''
            last_cleaning = ''
            has_product = ''
            existing_status = ''
            
            # More flexible column matching
            for col in master_plan_df.columns:
                col_lower = str(col).lower().strip()
                if 'khu' in col_lower and 'vực' in col_lower:
                    area = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
                elif 'thiết' in col_lower and 'bị' in col_lower:
                    device = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
                elif 'phương' in col_lower and 'pháp' in col_lower:
                    method = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
                elif 'tần' in col_lower and 'suất' in col_lower:
                    freq_str = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
                elif 'ngày' in col_lower and 'vệ sinh' in col_lower and 'gần' in col_lower:
                    last_cleaning = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
                elif 'chứa' in col_lower and 'sản phẩm' in col_lower:
                    has_product = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
                elif 'trạng thái' in col_lower:
                    existing_status = str(row.get(col, '')).strip() if pd.notna(row.get(col, '')) else ''
            
            # Debug first few rows
            if idx < 5:
                print(f"🔍 Row {idx} extracted data:")
                print(f"  area: '{area}', device: '{device}', method: '{method}'")
                print(f"  freq_str: '{freq_str}', last_cleaning: '{last_cleaning}', has_product: '{has_product}'")
                print(f"  existing_status: '{existing_status}'")
            
            # Skip empty rows
            if not area and not device:
                if idx < 5:
                    print(f"  -> Skipping empty row {idx}")
                continue
            
            # If we have existing status and it's valid, use it
            if use_existing_status and existing_status and existing_status not in ['nan', 'None', '']:
                current_status = existing_status
                
                # Still try to calculate next plan date if we have the data
                next_plan_str = ''
                if last_cleaning and last_cleaning not in ['nan', 'None', '']:
                    if freq_str and freq_str not in ['nan', 'None', '']:
                        try:
                            freq = int(float(freq_str))
                            last_cleaning_date = parse_date(last_cleaning)
                            if last_cleaning_date:
                                next_plan_date = last_cleaning_date + timedelta(days=freq)
                                next_plan_str = next_plan_date.strftime('%d/%m/%Y')
                        except (ValueError, TypeError):
                            pass
                
                updated_values.append([area, device, method, freq_str, last_cleaning, next_plan_str, current_status, has_product])
                
                # Safe increment using .get() method
                status_counts[current_status] = status_counts.get(current_status, 0) + 1
                processed_count += 1
                
                if idx < 5:
                    print(f"  -> Using existing status: {current_status}")
                continue
                
            # Otherwise, calculate status as before
            if not last_cleaning or last_cleaning in ['nan', 'None', '']:
                status = "Chưa có dữ liệu"
                updated_values.append([area, device, method, freq_str, last_cleaning, "", status, has_product])
                status_counts[status] = status_counts.get(status, 0) + 1
                if idx < 5:
                    print(f"  -> Status: {status} (no cleaning date)")
                continue
    
            freq = 0
            if freq_str and freq_str not in ['nan', 'None', '']:
                try:
                    freq = int(float(freq_str))
                except ValueError:
                    freq = 0
    
            last_cleaning_date = parse_date(last_cleaning)
            if not last_cleaning_date:
                status = "Định dạng ngày không hợp lệ"
                updated_values.append([area, device, method, freq_str, last_cleaning, "", status, has_product])
                status_counts['Lỗi'] = status_counts.get('Lỗi', 0) + 1
                if idx < 5:
                    print(f"  -> Status: {status} (invalid date: '{last_cleaning}')")
                continue
    
            next_plan_date = last_cleaning_date + timedelta(days=freq)
            next_plan_str = next_plan_date.strftime('%d/%m/%Y')
    
            days_until_next = (next_plan_date.date() - today.date()).days
            
            if days_until_next > 7:
                current_status = 'Bình thường'
            elif days_until_next > 0:
                current_status = 'Sắp đến hạn'
            elif days_until_next == 0:
                current_status = 'Đến hạn'
            else:
                current_status = 'Quá hạn'
    
            updated_values.append([area, device, method, freq_str, last_cleaning, next_plan_str, current_status, has_product])
            status_counts[current_status] = status_counts.get(current_status, 0) + 1
            processed_count += 1
            
            # Debug first few rows
            if idx < 5:
                print(f"  -> Calculated status: {current_status} (days until next: {days_until_next})")
                print(f"  -> Last cleaning: {last_cleaning_date.strftime('%d/%m/%Y')}, Next: {next_plan_str}")
            
            # Update the DataFrame (only if we calculated new values)
            if not use_existing_status:
                # Find the correct column names for updating
                for col in master_plan_df.columns:
                    col_lower = str(col).lower().strip()
                    if 'kế hoạch' in col_lower or ('ngày' in col_lower and 'tiếp theo' in col_lower):
                        master_plan_df.at[idx, col] = next_plan_str
                    elif 'trạng thái' in col_lower:
                        master_plan_df.at[idx, col] = current_status
            
        except Exception as e:
            print(f"❌ Error processing row {idx}: {str(e)}")
            print(f"❌ Full traceback: {traceback.format_exc()}")
            status_counts['Lỗi'] = status_counts.get('Lỗi', 0) + 1
            continue
    
    # Print processing summary
    print(f"\n📊 Processing Summary:")
    print(f"  - Total rows processed: {processed_count}")
    print(f"  - Status breakdown:")
    for status, count in status_counts.items():
        if count > 0:
            print(f"    - {status}: {count}")
    
    print(f"\n🎯 Due/Overdue Equipment Check:")
    due_count = status_counts.get('Đến hạn', 0) + status_counts.get('Quá hạn', 0)
    print(f"  - Equipment due for cleaning: {due_count}")
    print(f"    - Đến hạn: {status_counts.get('Đến hạn', 0)}")
    print(f"    - Quá hạn: {status_counts.get('Quá hạn', 0)}")
    
    # Update sheets data
    sheets_data['Master plan'] = master_plan_df
    
    # Update Actual Result with new cleaning records
    print("Kiểm tra và cập nhật bản ghi vệ sinh mới...")
    
    # Read existing records from Actual Result
    existing_records = set()  # Set of unique cleaning records (device + date)
    
    for idx, row in actual_result_df.iterrows():
        device_name = str(row.get('Thiết bị', '')).strip() if pd.notna(row.get('Thiết bị', '')) else ''
        cleaning_date_str = str(row.get('Ngày vệ sinh', '')).strip() if pd.notna(row.get('Ngày vệ sinh', '')) else ''
        if device_name and cleaning_date_str:
            record_key = f"{device_name}_{cleaning_date_str}"
            existing_records.add(record_key)
    
    # Identify new cleaning records from Master plan
    new_cleaning_records = []
    
    for row in updated_values:
        area, device, method, freq_str, last_cleaning, next_plan_str, status, has_product = row
        
        # Skip if no cleaning date or format is invalid
        if not last_cleaning or "không hợp lệ" in status.lower() or "chưa có dữ liệu" in status.lower():
            continue
            
        # Create unique key for this cleaning record
        record_key = f"{device}_{last_cleaning}"
        
        # Add to Actual Result if not already recorded
        if record_key not in existing_records:
            # Default values for new records
            person = "Tự động"  # Placeholder or default person
            result = "Đạt"      # Default result
            notes = ""          # Empty notes
            
            # Add new cleaning record
            new_cleaning_records.append({
                'Khu vực': area,
                'Thiết bị': device,
                'Phương pháp': method,
                'Tần suất (ngày)': freq_str,
                'Ngày vệ sinh': last_cleaning,
                'Người thực hiện': person,
                'Kết quả': result,
                'Ghi chú': notes
            })
            
            # Mark as processed to avoid duplicates
            existing_records.add(record_key)
    
    # Add new cleaning records to Actual Result sheet
    if new_cleaning_records:
        new_df = pd.DataFrame(new_cleaning_records)
        actual_result_df = pd.concat([actual_result_df, new_df], ignore_index=True)
        sheets_data['Actual result'] = actual_result_df
        print(f"Đã thêm {len(new_cleaning_records)} bản ghi vệ sinh mới vào Actual Result")
    else:
        print("Không có bản ghi vệ sinh mới để thêm vào Actual Result")
    
    print(f"Đã cập nhật {len(updated_values)} thiết bị.")
    
    # Try to upload updated file back to SharePoint
    upload_success = False
    if len(updated_values) > 0:
        print(f"\n📤 Attempting to upload updated file...")
        try:
            upload_success = processor.upload_excel_file(sheets_data)
        except Exception as e:
            print(f"⚠️ Upload failed with error: {str(e)}")
            upload_success = False
    
    # Create local backup if upload failed but we have data
    if not upload_success and len(updated_values) > 0:
        try:
            backup_filename = f"CIP_plan_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            with pd.ExcelWriter(backup_filename, engine='openpyxl') as writer:
                for sheet_name, df in sheets_data.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"💾 Created local backup: {backup_filename}")
        except Exception as e:
            print(f"❌ Failed to create local backup: {str(e)}")
    
    return updated_values

# Function to add a new cleaning record
def add_cleaning_record(area, device, method, freq, cleaning_date, person, result="Đạt", notes=""):
    """
    Add a new cleaning record and update Master plan and Actual Result
    Note: This function would need SharePoint integration for direct updates
    """
    print(f"Adding cleaning record for {device} on {cleaning_date}")
    # Implementation would require SharePoint integration
    return "Thành công"

# Function to update cleaning result
def update_cleaning_result(device, cleaning_date, result, notes=""):
    """
    Update the result of a cleaning record in the Actual Result sheet
    Note: This function would need SharePoint integration for direct updates
    """
    print(f"Updating cleaning result for {device} on {cleaning_date}")
    # Implementation would require SharePoint integration
    return "Thành công"

# Function to update product status
def update_product_status(device, has_product):
    """
    Update the product status for a device in the Master plan
    Note: This function would need SharePoint integration for direct updates
    """
    print(f"Updating product status for {device}")
    # Implementation would require SharePoint integration
    return "Thành công"

# Function to create status chart
def create_status_chart(updated_values):
    try:
        # Create DataFrame for visualization
        df = pd.DataFrame(updated_values, columns=[
            'Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)',
            'Ngày vệ sinh gần nhất', 'Ngày kế hoạch vệ sinh tiếp theo', 'Trạng thái', 'Đang chứa sản phẩm'
        ])
        
        # Set up figure with 2 subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # First subplot: Count statuses
        status_counts = df['Trạng thái'].value_counts()
        status_order = ['Bình thường', 'Sắp đến hạn', 'Đến hạn', 'Quá hạn']
        
        # Create a Series with all possible statuses and fill missing with 0
        status_data = pd.Series([0, 0, 0, 0], index=status_order)
        
        # Update with actual counts
        for status, count in status_counts.items():
            if status in status_data.index:
                status_data[status] = count
        
        # Create a bar chart for cleaning status
        colors = ['green', 'yellow', 'orange', 'red']
        ax1.bar(status_data.index, status_data.values, color=colors)
        ax1.set_title('Thống kê trạng thái thiết bị vệ sinh')
        ax1.set_ylabel('Số lượng')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Second subplot: Count product status for overdue equipment
        overdue_df = df[df['Trạng thái'].isin(['Đến hạn', 'Quá hạn'])]
        
        # Count devices with/without product
        product_status = overdue_df['Đang chứa sản phẩm'].fillna('Trống').map(lambda x: 'Có sản phẩm' if str(x).strip() else 'Trống')
        product_counts = product_status.value_counts()
        
        # Ensure both categories are present
        product_data = pd.Series([0, 0], index=['Có sản phẩm', 'Trống'])
        for status, count in product_counts.items():
            product_data[status] = count
        
        # Create a pie chart for product status
        ax2.pie(
            product_data.values,
            labels=product_data.index,
            colors=['red', 'green'],
            autopct='%1.1f%%',
            startangle=90
        )
        ax2.set_title('Trạng thái sản phẩm của thiết bị cần vệ sinh')
        ax2.axis('equal')
        
        plt.tight_layout()
        
        # Save chart for email
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=100)
        img_buffer.seek(0)
        
        plt.close()  # Close the plot to avoid warnings
        return img_buffer
    
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ: {str(e)}")
        return None

# Function to create results analysis chart
def create_results_chart():
    try:
        # This would need to get data from SharePoint
        # For now, return None
        return None
    
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ kết quả: {str(e)}")
        return None

# Modified send_email_report function with Outlook SMTP
def send_email_report(updated_values):
    print("Đang chuẩn bị gửi email báo cáo...")
    
    # Debug: Print all updated values to understand the data structure
    print(f"🔍 Total updated_values: {len(updated_values)}")
    if updated_values:
        print(f"🔍 Sample updated_values (first 3):")
        for i, row in enumerate(updated_values[:3]):
            print(f"  Row {i}: {row}")
            if len(row) > 6:
                print(f"    Status (index 6): '{row[6]}'")
    
    # Filter devices requiring attention
    due_rows = [row for row in updated_values if len(row) > 6 and row[6] in ['Đến hạn', 'Quá hạn']]
    
    print(f"🔍 Filtering logic:")
    print(f"  - Looking for status in ['Đến hạn', 'Quá hạn']")
    print(f"  - Found {len(due_rows)} due/overdue devices")
    
    # Debug: Print status breakdown
    status_breakdown = {}
    for row in updated_values:
        if len(row) > 6:
            status = row[6]
            status_breakdown[status] = status_breakdown.get(status, 0) + 1
    
    print(f"🔍 Status breakdown from updated_values:")
    for status, count in status_breakdown.items():
        print(f"  - '{status}': {count}")
    
    if due_rows:
        print(f"✅ Found {len(due_rows)} devices requiring attention")
        
        # Debug: Print due devices
        print(f"🔍 Due devices details:")
        for i, row in enumerate(due_rows[:5]):  # Show first 5
            print(f"  {i+1}. {row[0]} - {row[1]} - Status: {row[6]}")
        
        try:
            # Create charts
            status_img_buffer = create_status_chart(updated_values)
            results_img_buffer = create_results_chart()
            
            # Split the devices by area
            ro_station_rows = [row for row in due_rows if 'trạm ro' in str(row[0]).lower()]
            other_area_rows = [row for row in due_rows if 'trạm ro' not in str(row[0]).lower()]
            
            print(f"🔍 Area breakdown:")
            print(f"  - RO station devices: {len(ro_station_rows)}")
            print(f"  - Other area devices: {len(other_area_rows)}")
            
            # Define email recipient lists
            ro_recipients = [
                "mmb-ktcncsd@msc.masangroup.com", 
                "mmb-baotri-utilities@msc.masangroup.com", 
            ]
            
            other_recipients = [
                "mmb-ktcncsd@msc.masangroup.com",
            ]
            
            # Send RO station email if there are relevant items
            if ro_station_rows:
                print(f"📧 Sending email for RO station ({len(ro_station_rows)} devices)")
                send_area_specific_email(
                    ro_station_rows, 
                    ro_recipients, 
                    "Trạm RO", 
                    status_img_buffer, 
                    results_img_buffer
                )
            
            # Send other areas email if there are relevant items
            if other_area_rows:
                print(f"📧 Sending email for other areas ({len(other_area_rows)} devices)")
                send_area_specific_email(
                    other_area_rows, 
                    other_recipients, 
                    "Khu vực muối, cốt, chế biến mắm", 
                    status_img_buffer, 
                    results_img_buffer
                )
                
            print("✅ Email đã được gửi kèm bảng HTML và biểu đồ.")
            return True
            
        except Exception as e:
            print(f"❌ Lỗi khi gửi email: {str(e)}")
            print(f"❌ Traceback: {traceback.format_exc()}")
            return False
    else:
        print("⚠️ Không có thiết bị đến hạn/quá hạn, không gửi email.")
        print("🔍 This might be due to:")
        print("  1. Date parsing issues")
        print("  2. Incorrect status calculation")
        print("  3. Data structure problems")
        print("  4. Column mapping issues")
        return True

# Helper function to send area-specific emails with Outlook SMTP
def send_area_specific_email(filtered_rows, recipients, area_name, status_img_buffer, results_img_buffer):
    """
    Send an email for a specific area with the filtered rows using Outlook SMTP
    """
    # Create email
    msg = MIMEMultipart()
    msg['Subject'] = f'Báo cáo vệ sinh thiết bị - {area_name} - {datetime.today().strftime("%d/%m/%Y")}'
    msg['From'] = 'hoitkn@msc.masangroup.com'
    msg['To'] = ", ".join(recipients)
    
    # Prepare data for email summary
    empty_tanks = [row for row in filtered_rows if not str(row[7]).strip()]
    filled_tanks = [row for row in filtered_rows if str(row[7]).strip()]
    
    # HTML content with product status in a single table
    html_content = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; color: #333; }}
            .overdue {{ background-color: #ffcccc; }}
            .due-today {{ background-color: #ffeb99; }}
            .due-soon {{ background-color: #e6ffcc; }}
            .has-product {{ color: #cc0000; font-weight: bold; }}
            .empty {{ color: #009900; }}
            h2 {{ color: #003366; }}
            h3 {{ color: #004d99; margin-top: 25px; }}
            .summary {{ margin: 20px 0; }}
            .footer {{ margin-top: 30px; font-size: 0.9em; color: #666; }}
        </style>
    </head>
    <body>
        <h2>Báo cáo vệ sinh thiết bị - {area_name} - {datetime.today().strftime("%d/%m/%Y")}</h2>
        
        <div class="summary">
            <p><strong>Tổng số thiết bị cần vệ sinh:</strong> {len(filtered_rows)}</p>
            <p><strong>Thiết bị trống có thể vệ sinh ngay:</strong> {len(empty_tanks)}</p>
            <p><strong>Thiết bị đang chứa sản phẩm cần lên kế hoạch:</strong> {len(filled_tanks)}</p>
        </div>
        
        <h3>Danh sách thiết bị cần vệ sinh:</h3>
        <table>
            <thead>
                <tr>
                    <th>Khu vực</th>
                    <th>Thiết bị</th>
                    <th>Phương pháp</th>
                    <th>Tần suất (ngày)</th>
                    <th>Ngày vệ sinh gần nhất (KQ)</th>
                    <th>Ngày kế hoạch vệ sinh tiếp theo (KH)</th>
                    <th>Trạng thái</th>
                    <th>Đang chứa sản phẩm</th>
                </tr>
            </thead>
            <tbody>
    """
    
    # Add all tanks to the table (both empty and with product)
    # Sort the rows to prioritize empty tanks first
    sorted_rows = sorted(filtered_rows, key=lambda row: 1 if str(row[7]).strip() else 0)
    
    for row in sorted_rows:
        area, device, method, freq_str, last_cleaning, next_plan_str, status, has_product = row
        
        # Define CSS class based on status
        css_class = ""
        if status == "Quá hạn":
            css_class = "overdue"
        elif status == "Đến hạn":
            css_class = "due-today"
        
        # Define product status class
        product_class = "has-product" if str(has_product).strip() else "empty"
        
        html_content += f"""
                <tr class="{css_class}">
                    <td>{area}</td>
                    <td>{device}</td>
                    <td>{method}</td>
                    <td>{freq_str}</td>
                    <td>{last_cleaning}</td>
                    <td>{next_plan_str}</td>
                    <td>{status}</td>
                    <td class="{product_class}">{has_product}</td>
                </tr>
        """
    
    html_content += """
            </tbody>
        </table>
        
        <div class="footer">
            <p>Vui lòng xem SharePoint để biết chi tiết và cập nhật trạng thái của các thiết bị.</p>
            <p>Email này được tự động tạo bởi hệ thống. Vui lòng không trả lời.</p>
        </div>
    </body>
    </html>
    """
    
    # Attach HTML
    msg.attach(MIMEText(html_content, "html", "utf-8"))
    
    # Attach status chart if available
    if status_img_buffer:
        status_img_buffer.seek(0)  # Reset buffer position to start
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(status_img_buffer.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="cleaning_status.png"')
        msg.attach(part)
        
    # Attach results chart if available
    if results_img_buffer:
        results_img_buffer.seek(0)  # Reset buffer position to start
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(results_img_buffer.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="cleaning_results.png"')
        msg.attach(part)
    
    # Send email using Outlook SMTP
    smtp_server = 'smtp-mail.outlook.com'
    smtp_port = 587
    
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            print(f"📧 Connecting to Outlook SMTP server: {smtp_server}:{smtp_port}")
            
            # Start TLS encryption
            server.starttls()
            print("✅ TLS encryption started")
            
            # Get email password from environment variable
            email_password = os.environ.get('EMAIL_PASSWORD')
            if not email_password:
                print("❌ Cảnh báo: Không tìm thấy mật khẩu email trong biến môi trường EMAIL_PASSWORD.")
                print("💡 Đối với Outlook, bạn cần sử dụng App Password thay vì mật khẩu thường.")
                return False
            
            # Authenticate with Outlook
            print(f"🔐 Authenticating with email: hoitkn@msc.masangroup.com")
            server.login("hoitkn@msc.masangroup.com", email_password)
            print("✅ Authentication successful")
            
            # Send the message
            print(f"📤 Sending email to {len(recipients)} recipients...")
            server.send_message(msg)
            print("✅ Email sent successfully")
            
    except smtplib.SMTPAuthenticationError as e:
        print(f"❌ SMTP Authentication Error: {str(e)}")
        print("💡 For Outlook, use App Password instead of regular password")
        return False
        
    except Exception as e:
        print(f"❌ Error sending email: {str(e)}")
        return False
        
    print(f"Email cho {area_name} đã được gửi đến {len(recipients)} người nhận.")

# Main function to run everything
def run_update():
    print("Bắt đầu cập nhật hệ thống vệ sinh thiết bị từ SharePoint...")
    
    try:
        # Update cleaning schedule and get updated values
        updated_values = update_cleaning_schedule()
        
        # Send email report
        send_email_report(updated_values)
        
        print("Hoàn thành cập nhật.")
        return True
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return False

# Run the update if executed directly
if __name__ == "__main__":
    success = run_update()
    if success:
        print("✅ CIP Cleaning automation completed successfully!")
    else:
        print("❌ CIP Cleaning automation failed!")
        sys.exit(1)
