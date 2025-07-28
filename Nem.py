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

# SharePoint File ID from the new URL
SAMPLING_FILE_ID = '0D5DEB9D-23AE-5C76-0C64-9FAB248215DE'  # Sampling plan NÃM RAU.xlsx

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

class SharePointSamplingProcessor:
    """SharePoint integration for QA Sampling automation"""

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
            self.log(f"📥 Downloading Sampling plan file from SharePoint...")

            # Get file download URL using file ID
            url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{SAMPLING_FILE_ID}"
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

                            self.log(f"✅ Successfully downloaded sampling plan file")
                            return sheets_data

                        except Exception as e:
                            self.log(f"❌ Error reading Excel file: {str(e)}")
                            return None
                    else:
                        self.log(f"❌ Error downloading file content: {file_response.status_code}")
                else:
                    self.log(f"❌ No download URL found for sampling plan file")
            else:
                self.log(f"❌ Error getting file info: {response.status_code}")

        except Exception as e:
            self.log(f"❌ Error downloading sampling plan file: {str(e)}")

        return None

    def upload_excel_file(self, sheets_data):
        """Upload updated Excel file back to SharePoint with retry logic for locked files"""
        max_retries = 5
        retry_delay = 30  # seconds

        try:
            self.log(f"📤 Uploading updated sampling plan to SharePoint...")

            # Create Excel file in memory with multiple sheets
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                for sheet_name, df in sheets_data.items():
                    # Format date columns before saving
                    df_formatted = self.format_dataframe_for_excel(df)
                    df_formatted.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Auto-fit column widths for better readability
                    worksheet = writer.sheets[sheet_name]
                    self.auto_fit_columns(worksheet, df_formatted)

            excel_buffer.seek(0)
            excel_content = excel_buffer.getvalue()
            self.log(f"Created Excel file with {len(excel_content)} bytes")

            # Upload to SharePoint with retry logic
            upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{SAMPLING_FILE_ID}/content"

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            for attempt in range(max_retries):
                try:
                    self.log(f"Upload attempt {attempt + 1}/{max_retries}")

                    response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)

                    if response.status_code in [200, 201]:
                        self.log(f"✅ Successfully uploaded updated sampling plan to SharePoint")
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

    def auto_fit_columns(self, worksheet, dataframe):
        """Auto-fit column widths based on content"""
        try:
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                # Check header length
                if len(dataframe.columns) > 0:
                    header_length = len(str(dataframe.columns[column[0].column - 1]))
                    max_length = max(max_length, header_length)
                
                # Check data length in first 100 rows (for performance)
                for cell in list(column)[:101]:  # Include header + 100 data rows
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                # Set column width with some padding, but cap at reasonable max
                adjusted_width = min(max_length + 2, 50)  # Max width of 50
                worksheet.column_dimensions[column_letter].width = max(adjusted_width, 8)  # Min width of 8
                
        except Exception as e:
            self.log(f"Warning: Could not auto-fit columns: {str(e)}")

    def format_dataframe_for_excel(self, df):
        """Format dataframe for better Excel display"""
        df_formatted = df.copy()
        
        # Format date columns to DD/MM/YYYY
        date_columns = ['Ngày kiểm tra gần nhất', 'Kế hoạch lấy mẫu tiếp theo', 'Ngày kiểm tra', 'Kế hoạch lấy mẫu', 'Ngày thực hiện']
        
        for col in df_formatted.columns:
            col_name = str(col).strip()
            if any(date_keyword in col_name for date_keyword in date_columns):
                df_formatted[col] = df_formatted[col].apply(self.format_date_for_display)
                
        return df_formatted

    def format_date_for_display(self, date_value):
        """Format date to DD/MM/YYYY string"""
        if pd.isna(date_value) or date_value == '' or str(date_value).strip() in ['nan', 'None', '']:
            return ''
            
        # If already a string in correct format, return as is
        if isinstance(date_value, str) and len(date_value) == 10 and date_value.count('/') == 2:
            return date_value
            
        # Try to parse and format
        parsed_date = parse_date(date_value)
        if parsed_date:
            return parsed_date.strftime('%d/%m/%Y')
        else:
            return str(date_value)

    def upload_backup_file(self, excel_content):
        """Upload to a backup file when original is locked"""
        try:
            # Generate backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"Sampling_plan_NÃM_RAU_backup_{timestamp}.xlsx"

            self.log(f"🔄 Uploading to backup file: {backup_filename}")

            # Upload to the same folder but with different name
            # First get the parent folder
            file_info_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{SAMPLING_FILE_ID}"
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

# Helper function to parse dates in different formats
def parse_date(date_str):
    """Try to parse date with multiple formats and handle Excel date formats"""
    from datetime import datetime, timedelta  # Import at the top to avoid UnboundLocalError
    
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
                from datetime import datetime, timedelta
                excel_epoch = datetime(1900, 1, 1)
                # Excel incorrectly treats 1900 as a leap year, so subtract 2 days
                return excel_epoch + timedelta(days=excel_date - 2)
    except (ValueError, TypeError):
        pass

    # Try various date formats
    date_formats = [
        '%d/%m/%Y',      # 01/12/2024
        '%m/%d/%Y',      # 12/01/2024
        '%Y-%m-%d',      # 2024-12-01
        '%d-%m-%Y',      # 01-12-2024
        '%B %d, %Y',     # December 1, 2024
        '%d %B %Y',      # 1 December 2024
        '%d/%m/%y',      # 01/12/24
        '%m/%d/%y',      # 12/01/24
        '%d-%m-%y',      # 01-12-24
        '%d.%m.%Y',      # 01.12.2024
        '%d.%m.%y',      # 01.12.24
        '%Y/%m/%d',      # 2024/12/01
        '%d-%b-%Y',      # 01-Dec-2024
        '%d-%b-%y',      # 01-Dec-24
        '%d %b %Y',      # 1 Dec 2024
        '%d %b %y',      # 1 Dec 24
        '%Y-%m-%d %H:%M:%S',  # 2025-05-01 00:00:00
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

    print(f"Warning: Could not parse date: '{date_str}'")
    return None

# Function to update sampling schedule and find due samples
def update_sampling_schedule(df, check_type="Hóa lý"):
    print(f"Đang cập nhật lịch lấy mẫu {check_type}...")

    if df.empty:
        print(f"Không tìm thấy dữ liệu trong bảng {check_type}.")
        return [], [], df

    # Create a copy to avoid modifying original dataframe
    updated_df = df.copy()

    # Debug: Print detailed column information
    print(f"Data shape: {df.shape}")
    print(f"Available columns:")
    for i, col in enumerate(df.columns):
        print(f"  [{i}] '{col}' (type: {type(col)})")

    # Print first few non-empty rows to understand data structure
    print(f"Sample data (first 5 rows):")
    for idx in range(min(5, len(df))):
        row = df.iloc[idx]
        print(f"  Row {idx}:")
        for i, col in enumerate(df.columns):
            value = row[col]
            print(f"    [{i}] {col}: '{value}' (type: {type(value).__name__})")
        print()

    # Expected columns mapping with more comprehensive detection
    col_mapping = {}

    for col in df.columns:
        col_str = str(col).strip()
        col_lower = col_str.lower()

        # Debug each column matching
        print(f"Checking column: '{col_str}' -> '{col_lower}'")

        # Khu vực
        if any(keyword in col_lower for keyword in ['khu vực', 'khu_vuc', 'area', 'zone', 'khu']):
            col_mapping['khu_vuc'] = col
            print(f"  -> Matched as 'khu_vuc'")
        # Sản phẩm - MORE FLEXIBLE MATCHING
        elif any(keyword in col_lower for keyword in ['sản phẩm', 'san_pham', 'san pham', 'product', 'sản xuất', 'sanpham']):
            col_mapping['san_pham'] = col
            print(f"  -> Matched as 'san_pham'")
        # Check if it's exactly "Sản phẩm"
        elif col_str == 'Sản phẩm':
            col_mapping['san_pham'] = col
            print(f"  -> Exact match as 'san_pham'")
        # Line / Xưởng
        elif any(keyword in col_lower for keyword in ['line', 'xưởng', 'workshop', 'dây chuyền', 'xuong']):
            col_mapping['line'] = col
            print(f"  -> Matched as 'line'")
        # Chỉ tiêu kiểm
        elif any(keyword in col_lower for keyword in ['chỉ tiêu', 'chi_tieu', 'chi tieu', 'parameter', 'tiêu chí']):
            col_mapping['chi_tieu'] = col
            print(f"  -> Matched as 'chi_tieu'")
        # Tần suất
        elif any(keyword in col_lower for keyword in ['tần suất', 'tan_suat', 'tan suat', 'frequency', 'chu kỳ']):
            col_mapping['tan_suat'] = col
            print(f"  -> Matched as 'tan_suat'")
        # Ngày kiểm tra
        elif any(keyword in col_lower for keyword in ['ngày kiểm tra', 'last check', 'ngay_kiem_tra', 'kiểm tra gần nhất', 'kiem tra']):
            col_mapping['ngay_kiem_tra'] = col
            print(f"  -> Matched as 'ngay_kiem_tra'")
        # Sample ID
        elif any(keyword in col_lower for keyword in ['sample id', 'sample_id', 'sampleid', 'mã mẫu']):
            col_mapping['sample_id'] = col
            print(f"  -> Matched as 'sample_id'")
        # Kế hoạch
        elif any(keyword in col_lower for keyword in ['kế hoạch', 'next', 'ke_hoach', 'tiếp theo', 'ke hoach']):
            col_mapping['ke_hoach'] = col
            print(f"  -> Matched as 'ke_hoach'")
        else:
            print(f"  -> No match found")

    print(f"\nFinal detected columns: {col_mapping}")

    # Force manual column mapping if automatic detection fails
    if 'san_pham' not in col_mapping:
        print("\n⚠️ 'Sản phẩm' column not detected automatically, trying manual mapping...")
        columns_list = list(df.columns)
        for i, col in enumerate(columns_list):
            print(f"  Column {i}: '{col}'")
            if i == 1:  # Based on image, "Sản phẩm" is column B (index 1)
                col_mapping['san_pham'] = col
                print(f"  -> Force mapped column {i} as 'san_pham'")
                break

    # Add missing critical mappings by position if still missing
    if len(col_mapping) < 3:  # We need at least khu_vuc, san_pham, and one date/frequency
        print("\n⚠️ Critical columns missing, attempting position-based mapping...")
        columns_list = list(df.columns)

        # Based on the images provided:
        # A: Khu vực, B: Sản phẩm, C: Line/Xưởng, D: Chỉ tiêu kiểm, E: Tần suất, F: Ngày kiểm tra, G: Sample ID, H: Kế hoạch
        position_mapping = {
            0: 'khu_vuc',     # A: Khu vực
            1: 'san_pham',    # B: Sản phẩm
            2: 'line',        # C: Line / Xưởng
            3: 'chi_tieu',    # D: Chỉ tiêu kiểm
            4: 'tan_suat',    # E: Tần suất (ngày)
            5: 'ngay_kiem_tra',  # F: Ngày kiểm tra gần nhất
            6: 'sample_id',   # G: Sample ID
            7: 'ke_hoach'     # H: Kế hoạch lấy mẫu tiếp theo
        }

        for pos, field in position_mapping.items():
            if pos < len(columns_list) and field not in col_mapping:
                col_mapping[field] = columns_list[pos]
                print(f"  Position {pos} -> {field}: '{columns_list[pos]}'")

    print(f"\nUpdated detected columns: {col_mapping}")

    today = datetime.today()
    due_samples = []
    all_samples = []

    # Add 'Kế hoạch lấy mẫu tiếp theo' column if it doesn't exist
    if 'ke_hoach' not in col_mapping:
        next_plan_col = 'Kế hoạch lấy mẫu tiếp theo'
        if next_plan_col not in updated_df.columns:
            updated_df[next_plan_col] = ''
            col_mapping['ke_hoach'] = next_plan_col

    # Process each row with better error handling
    processed_count = 0
    for idx, row in updated_df.iterrows():
        try:
            # Extract data from row using column mapping
            khu_vuc = str(row.get(col_mapping.get('khu_vuc', ''), '')).strip()
            san_pham = str(row.get(col_mapping.get('san_pham', ''), '')).strip()
            line = str(row.get(col_mapping.get('line', ''), '')).strip()
            chi_tieu = str(row.get(col_mapping.get('chi_tieu', ''), '')).strip()
            tan_suat_str = str(row.get(col_mapping.get('tan_suat', ''), '')).strip()
            ngay_kiem_tra = str(row.get(col_mapping.get('ngay_kiem_tra', ''), '')).strip()
            sample_id = str(row.get(col_mapping.get('sample_id', ''), '')).strip()

            # Debug first few rows in detail
            if idx < 3:
                print(f"\nRow {idx} extracted data:")
                print(f"  khu_vuc: '{khu_vuc}' (from column '{col_mapping.get('khu_vuc', 'N/A')}')")
                print(f"  san_pham: '{san_pham}' (from column '{col_mapping.get('san_pham', 'N/A')}')")
                print(f"  line: '{line}' (from column '{col_mapping.get('line', 'N/A')}')")
                print(f"  chi_tieu: '{chi_tieu}' (from column '{col_mapping.get('chi_tieu', 'N/A')}')")
                print(f"  tan_suat_str: '{tan_suat_str}' (from column '{col_mapping.get('tan_suat', 'N/A')}')")
                print(f"  ngay_kiem_tra: '{ngay_kiem_tra}' (from column '{col_mapping.get('ngay_kiem_tra', 'N/A')}')")
                print(f"  sample_id: '{sample_id}' (from column '{col_mapping.get('sample_id', 'N/A')}')")

            # More lenient validation - require at least some data
            has_core_data = bool(
                (khu_vuc and khu_vuc not in ['nan', 'None', '']) or
                (san_pham and san_pham not in ['nan', 'None', '']) or
                (line and line not in ['nan', 'None', ''])
            )

            if not has_core_data:
                if idx < 5:
                    print(f"  Skipping row {idx}: No core data found")
                continue

            # Validate frequency
            if not tan_suat_str or tan_suat_str in ['nan', 'None', '']:
                if idx < 5:
                    print(f"  Skipping row {idx}: Missing frequency data")
                continue

            # Validate date
            if not ngay_kiem_tra or ngay_kiem_tra in ['nan', 'None', '']:
                if idx < 5:
                    print(f"  Skipping row {idx}: Missing date data")
                continue

            # Parse frequency
            tan_suat = 0
            try:
                if tan_suat_str and tan_suat_str not in ['nan', 'None', '']:
                    tan_suat = int(float(tan_suat_str))
                    if tan_suat <= 0:
                        if idx < 5:
                            print(f"  Skipping row {idx}: Invalid frequency: {tan_suat}")
                        continue
            except (ValueError, TypeError):
                if idx < 5:
                    print(f"  Skipping row {idx}: Cannot parse frequency: '{tan_suat_str}'")
                continue

            # Parse last inspection date
            ngay_kiem_tra_date = parse_date(ngay_kiem_tra)
            if not ngay_kiem_tra_date:
                if idx < 5:
                    print(f"  Skipping row {idx}: Cannot parse date: '{ngay_kiem_tra}'")
                continue

            # Calculate next sampling date
            next_sampling_date = ngay_kiem_tra_date + timedelta(days=tan_suat)
            next_sampling_str = next_sampling_date.strftime('%d/%m/%Y')

            # Update the plan column with consistent DD/MM/YYYY format
            if col_mapping.get('ke_hoach'):
                updated_df.at[idx, col_mapping['ke_hoach']] = next_sampling_str

            # Determine sample status
            days_until_next = (next_sampling_date.date() - today.date()).days
            status = "Đến hạn" if days_until_next <= 0 else "Chưa đến hạn"

            # Create sample record - use defaults for missing values
            sample_record = {
                'khu_vuc': khu_vuc or 'N/A',
                'san_pham': san_pham or 'N/A',
                'line': line or 'N/A',
                'chi_tieu': chi_tieu or 'N/A',
                'tan_suat': tan_suat_str,
                'ngay_kiem_tra': ngay_kiem_tra_date.strftime('%d/%m/%Y'),  # Format consistently
                'sample_id': sample_id or 'N/A',
                'ke_hoach': next_sampling_str,
                'loai_kiem_tra': check_type,
                'row_index': idx,
                'status': status
            }

            # Add to all samples
            all_samples.append(sample_record)

            # Add to due samples if due
            if days_until_next <= 0:
                due_samples.append(sample_record)

            processed_count += 1

            if idx < 3:
                print(f"  ✅ Successfully processed row {idx}")

        except Exception as e:
            print(f"Lỗi xử lý hàng {idx}: {str(e)}")
            if idx < 5:  # Print full error for first few rows
                print(f"Full error: {traceback.format_exc()}")
            continue

    print(f"\nProcessing summary for {check_type}:")
    print(f"  - Total rows in sheet: {len(updated_df)}")
    print(f"  - Rows successfully processed: {processed_count}")
    print(f"  - Samples tracked: {len(all_samples)}")
    print(f"  - Due samples: {len(due_samples)}")

    return due_samples, all_samples, updated_df

# Function to create summary report with better formatting
def create_summary_report(all_samples):
    """Create summary report DataFrame with improved formatting"""
    print("Đang tạo báo cáo tổng hợp...")

    if not all_samples:
        print("Không có mẫu nào để tạo báo cáo.")
        return pd.DataFrame()

    # Create summary DataFrame
    summary_data = []
    for sample in all_samples:
        summary_data.append([
            sample['khu_vuc'],
            sample['san_pham'],
            sample['line'],
            sample['chi_tieu'],
            sample['tan_suat'],
            sample['sample_id'],
            sample['ngay_kiem_tra'],  # Already formatted as DD/MM/YYYY
            sample['ke_hoach'],       # Already formatted as DD/MM/YYYY
            sample['loai_kiem_tra'],
            sample['status']
        ])

    # Define headers with proper Vietnamese formatting
    headers = [
        'Khu vực', 
        'Sản phẩm', 
        'Line / Xưởng', 
        'Chỉ tiêu kiểm tra', 
        'Tần suất (ngày)', 
        'Sample ID', 
        'Ngày kiểm tra gần nhất',  # DD/MM/YYYY
        'Kế hoạch lấy mẫu tiếp theo',  # DD/MM/YYYY
        'Loại kiểm tra', 
        'Trạng thái'
    ]

    summary_df = pd.DataFrame(summary_data, columns=headers)

    print(f"Đã tạo báo cáo tổng hợp với {len(summary_df)} mẫu.")
    return summary_df

# NEW FUNCTION: Create testing history report - Only track samples with new IDs
def create_history_report(existing_history, all_samples_from_sheets):
    """Create or update testing history report - only for samples with new Sample IDs"""
    print("Đang tạo/cập nhật lịch sử kiểm mẫu...")
    
    # Get existing Sample IDs from history to avoid duplicates
    existing_sample_ids = set()
    if existing_history is not None and not existing_history.empty:
        # Find Sample ID column in existing history
        id_columns = ['Sample ID', 'sample_id', 'Sample_ID', 'ID Mẫu']
        for col in existing_history.columns:
            if any(id_col.lower() in str(col).lower() for id_col in id_columns):
                existing_sample_ids = set(existing_history[col].astype(str).str.strip())
                break
        print(f"Tìm thấy {len(existing_sample_ids)} Sample ID trong lịch sử hiện có.")
    
    # Find samples with new IDs (not in existing history)
    new_history_data = []
    
    for sample in all_samples_from_sheets:
        sample_id = str(sample.get('sample_id', '')).strip()
        
        # Only add samples that have valid ID and are not already in history
        if (sample_id and 
            sample_id not in ['N/A', 'nan', 'None', ''] and
            sample_id not in existing_sample_ids):
            
            # Use the actual testing date (ngay_kiem_tra), not current date
            ngay_thuc_hien = sample['ngay_kiem_tra']  # This is already formatted as DD/MM/YYYY
            
            new_history_data.append([
                ngay_thuc_hien,             # Ngày thực hiện = Ngày kiểm tra gần nhất
                sample['khu_vuc'],          # Khu vực
                sample['san_pham'],         # Sản phẩm
                sample['line'],             # Line/Xưởng
                sample['chi_tieu'],         # Chỉ tiêu kiểm tra
                sample['loai_kiem_tra'],    # Loại kiểm tra (Hóa lý/Vi sinh)
                sample_id,                  # Sample ID
                '',                         # Ghi chú (để trống cho user điền)
                sample['ke_hoach']          # Ngày kế hoạch ban đầu
            ])
            
            print(f"  Thêm Sample ID {sample_id} - Ngày thực hiện: {ngay_thuc_hien}")
    
    # Define headers for history sheet (simplified)
    history_headers = [
        'Ngày thực hiện',           # DD/MM/YYYY
        'Khu vực',
        'Sản phẩm', 
        'Line / Xưởng',
        'Chỉ tiêu kiểm tra',
        'Loại kiểm tra',            # Hóa lý hoặc Vi sinh
        'Sample ID',
        'Ghi chú',                  # Để user thêm thông tin
        'Ngày kế hoạch'             # Ngày ban đầu theo kế hoạch
    ]
    
    # Create new history DataFrame
    new_history_df = pd.DataFrame(new_history_data, columns=history_headers)
    
    # If there's existing history, combine them
    if existing_history is not None and not existing_history.empty:
        print(f"Tìm thấy lịch sử hiện có với {len(existing_history)} bản ghi.")
        
        # Ensure existing history has the same columns
        for col in history_headers:
            if col not in existing_history.columns:
                existing_history[col] = ''
        
        # Keep only the columns we want
        existing_history = existing_history[history_headers]
        
        # Combine old and new history
        combined_history = pd.concat([existing_history, new_history_df], ignore_index=True)
        
        # Sort by date (most recent first)
        combined_history['Ngày thực hiện_sorted'] = pd.to_datetime(combined_history['Ngày thực hiện'], format='%d/%m/%Y', errors='coerce')
        combined_history = combined_history.sort_values('Ngày thực hiện_sorted', ascending=False, na_position='last')
        combined_history = combined_history.drop('Ngày thực hiện_sorted', axis=1)
        
        print(f"Đã thêm {len(new_history_data)} Sample ID mới. Tổng cộng: {len(combined_history)} bản ghi.")
        return combined_history
    else:
        print(f"Tạo lịch sử mới với {len(new_history_data)} Sample ID.")
        return new_history_df

# Create visualization charts for email
def create_charts(due_samples):
    try:
        if not due_samples:
            return None

        # Create a DataFrame from the samples
        df = pd.DataFrame(due_samples)

        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Plot 1: Group samples by area
        area_counts = df['khu_vuc'].value_counts()
        area_counts.plot(kind='bar', ax=ax1, color='skyblue')
        ax1.set_xlabel('Khu vực')
        ax1.set_ylabel('Số lượng mẫu')
        ax1.set_title('Số lượng mẫu theo khu vực')
        ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Group samples by test type
        type_counts = df['loai_kiem_tra'].value_counts()
        type_counts.plot(kind='pie', ax=ax2, autopct='%1.1f%%', startangle=90, colors=['#ff9999','#66b3ff'])
        ax2.set_title('Phân bố loại kiểm tra')
        ax2.set_ylabel('')

        plt.tight_layout()

        # Save chart to buffer
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=100)
        img_buffer.seek(0)

        plt.close()  # Close the plot to avoid warnings
        return img_buffer

    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ: {str(e)}")
        return None

# Send email notification for due samples
def send_email_notification(due_samples):
    if not due_samples:
        print("Không có mẫu đến hạn, không gửi email.")
        return True

    print(f"Đang gửi email thông báo cho {len(due_samples)} mẫu đến hạn...")

    try:
        # Create charts
        chart_buffer = create_charts(due_samples)

        # Create email
        msg = MIMEMultipart()
        msg['Subject'] = f'Thông báo lấy mẫu QA - {datetime.today().strftime("%d/%m/%Y")}'
        msg['From'] = 'hoitkn@msc.masangroup.com'

        # Recipients
        recipients = ["ktcnnemmb@msc.masangroup.com"]
        msg['To'] = ", ".join(recipients)

        # Group samples by type for better organization in email
        hoa_ly_samples = [s for s in due_samples if s['loai_kiem_tra'] == 'Hóa lý']
        vi_sinh_samples = [s for s in due_samples if s['loai_kiem_tra'] == 'Vi sinh']

        # HTML content with better formatting
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                table {{ 
                    border-collapse: collapse; 
                    width: 100%; 
                    margin-top: 15px;
                    background-color: white;
                }}
                th, td {{ 
                    border: 1px solid #ddd; 
                    padding: 8px; 
                    text-align: left;
                    font-size: 12px;
                }}
                th {{ 
                    background-color: #4CAF50; 
                    color: white;
                    font-weight: bold;
                }}
                .due {{ 
                    background-color: #fff3cd;
                    border-left: 4px solid #ffc107;
                }}
                h2 {{ 
                    color: #2c3e50; 
                    border-bottom: 2px solid #3498db;
                    padding-bottom: 10px;
                }}
                h3 {{ 
                    color: #34495e; 
                    margin-top: 25px;
                    background-color: #ecf0f1;
                    padding: 10px;
                    border-radius: 4px;
                }}
                .summary {{ 
                    margin: 20px 0;
                    background-color: #e8f4fd;
                    padding: 15px;
                    border-radius: 4px;
                    border-left: 4px solid #3498db;
                }}
                .summary-item {{
                    margin: 5px 0;
                    font-size: 14px;
                }}
                .footer {{ 
                    margin-top: 30px; 
                    font-size: 0.9em; 
                    color: #666;
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 4px;
                }}
                .status-due {{
                    color: #dc3545;
                    font-weight: bold;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>📋 Thông báo lấy mẫu QA - {datetime.today().strftime("%d/%m/%Y")}</h2>
                
                <div class="summary">
                    <div class="summary-item"><strong>📊 Tổng số mẫu cần lấy:</strong> <span class="status-due">{len(due_samples)}</span></div>
                    <div class="summary-item"><strong>🧪 Mẫu Hóa lý:</strong> {len(hoa_ly_samples)}</div>
                    <div class="summary-item"><strong>🦠 Mẫu Vi sinh:</strong> {len(vi_sinh_samples)}</div>
                    <div class="summary-item"><strong>📅 Thời gian tạo báo cáo:</strong> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}</div>
                </div>
        """

        # Add tables for each type
        if hoa_ly_samples:
            html_content += create_email_table("🧪 Hóa lý", hoa_ly_samples)

        if vi_sinh_samples:
            html_content += create_email_table("🦠 Vi sinh", vi_sinh_samples)

        html_content += """
                <div class="footer">
                    <h4>📝 Hướng dẫn thực hiện:</h4>
                    <ol>
                        <li>Vui lòng thực hiện lấy mẫu theo danh sách trên</li>
                        <li>Cập nhật Sample ID mới vào SharePoint sau khi lấy mẫu</li>
                        <li>Kiểm tra và cập nhật lịch sử trong sheet "Lịch sử kiểm mẫu"</li>
                        <li>Báo cáo tổng hợp đã được tự động cập nhật trong file Excel</li>
                    </ol>
                    <p><em>⚠️ Email này được tự động tạo bởi hệ thống QA Sampling. Vui lòng không trả lời email này.</em></p>
                </div>
            </div>
        </body>
        </html>
        """

        # Attach HTML
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        # Attach chart if available
        if chart_buffer:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(chart_buffer.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="sampling_status.png"')
            msg.attach(part)

        # Send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            email_password = os.environ.get('EMAIL_PASSWORD')
            if not email_password:
                print("Cảnh báo: Không tìm thấy mật khẩu email trong biến môi trường.")
                return False

            server.login("hoitkn@msc.masangroup.com", email_password)
            server.send_message(msg)

        print(f"Email đã được gửi đến {len(recipients)} người nhận.")
        return True

    except Exception as e:
        print(f"Lỗi khi gửi email: {str(e)}")
        return False

def create_email_table(check_type, samples):
    """Create HTML table for email with improved formatting"""
    html = f"""
    <h3>{check_type} - Danh sách mẫu cần lấy ({len(samples)} mẫu):</h3>
    <table>
        <thead>
            <tr>
                <th style="width: 12%;">Khu vực</th>
                <th style="width: 20%;">Sản phẩm</th>
                <th style="width: 12%;">Line/Xưởng</th>
                <th style="width: 15%;">Chỉ tiêu kiểm tra</th>
                <th style="width: 8%;">Tần suất (ngày)</th>
                <th style="width: 12%;">Ngày kiểm tra gần nhất</th>
                <th style="width: 10%;">Sample ID</th>
                <th style="width: 11%;">Kế hoạch tiếp theo</th>
            </tr>
        </thead>
        <tbody>
    """

    for sample in samples:
        html += f"""
            <tr class="due">
                <td>{sample['khu_vuc']}</td>
                <td>{sample['san_pham']}</td>
                <td>{sample['line']}</td>
                <td>{sample['chi_tieu']}</td>
                <td style="text-align: center;">{sample['tan_suat']}</td>
                <td style="text-align: center;">{sample['ngay_kiem_tra']}</td>
                <td style="text-align: center;">{sample['sample_id']}</td>
                <td style="text-align: center;">{sample['ke_hoach']}</td>
            </tr>
        """

    html += """
        </tbody>
    </table>
    """
    return html

# Main function to run everything
def run_update():
    print("Bắt đầu cập nhật lịch lấy mẫu QA từ SharePoint...")

    try:
        # Initialize SharePoint processor
        processor = SharePointSamplingProcessor()

        # Download Excel file from SharePoint
        sheets_data = processor.download_excel_file()
        if not sheets_data:
            print("❌ Failed to download sampling plan file")
            return False

        all_due_samples = []
        all_collected_samples = []
        updated_sheets = {}
        existing_history = None

        # Check if there's an existing history sheet
        history_sheet_names = ['Lịch sử kiểm mẫu', 'Lich su kiem mau', 'Testing History', 'History']
        for sheet_name in sheets_data:
            if any(hist_name.lower() in sheet_name.lower() for hist_name in history_sheet_names):
                existing_history = sheets_data[sheet_name]
                print(f"Tìm thấy sheet lịch sử: {sheet_name}")
                break

        # Process each sheet that looks like a sampling schedule
        for sheet_name, df in sheets_data.items():
            # Skip empty sheets, summary sheets, or history sheets
            if (df.empty or 
                'tổng hợp' in sheet_name.lower() or 
                'summary' in sheet_name.lower() or
                any(hist_name.lower() in sheet_name.lower() for hist_name in history_sheet_names)):
                updated_sheets[sheet_name] = df
                continue

            print(f"\nProcessing sheet: {sheet_name}")
            print("=" * 50)

            # Determine check type based on sheet name
            check_type = "Hóa lý"
            if 'vi sinh' in sheet_name.lower() or 'micro' in sheet_name.lower():
                check_type = "Vi sinh"
            elif 'hóa' in sheet_name.lower() or 'hoa' in sheet_name.lower() or 'chemical' in sheet_name.lower():
                check_type = "Hóa lý"

            # Update sampling schedule for this sheet
            due_samples, all_samples, updated_df = update_sampling_schedule(df, check_type)

            # Collect results
            all_due_samples.extend(due_samples)
            all_collected_samples.extend(all_samples)

            # Store updated dataframe
            updated_sheets[sheet_name] = updated_df

            # Add delay between processing sheets
            time.sleep(2)

        # Create summary report sheet with better formatting
        if all_collected_samples:
            summary_df = create_summary_report(all_collected_samples)
            updated_sheets['Báo cáo tổng hợp'] = summary_df

        # Create or update history report - track samples with new IDs
        if all_collected_samples:  # Track all samples, not just due ones
            history_df = create_history_report(existing_history, all_collected_samples)
            updated_sheets['Lịch sử kiểm mẫu'] = history_df

        # Print processing results
        print(f"\n📊 Kết quả xử lý tổng thể:")
        print(f"  - Tổng số mẫu được theo dõi: {len(all_collected_samples)}")
        print(f"  - Mẫu đến hạn cần lấy: {len(all_due_samples)}")
        print(f"  - Sheets đã xử lý: {len(updated_sheets)}")
        print(f"  - Đã tạo/cập nhật lịch sử kiểm mẫu: {'✅' if all_collected_samples else '⏭️'}")

        # Show sample of collected data for verification
        if all_collected_samples:
            print(f"\n📋 Mẫu dữ liệu đã xử lý (5 mẫu đầu):")
            for i, sample in enumerate(all_collected_samples[:5]):
                print(f"  {i+1}. {sample['loai_kiem_tra']} - {sample['san_pham']} (Line: {sample['line']}) - Status: {sample['status']}")

        # Try to upload updated file back to SharePoint (with timeout to avoid hanging)
        upload_success = False
        if len(all_collected_samples) > 0:  # Only upload if we have data
            print(f"\n📤 Attempting to upload updated file...")
            try:
                # Set a shorter timeout for upload attempts
                upload_success = processor.upload_excel_file(updated_sheets)
            except Exception as e:
                print(f"⚠️ Upload failed with error: {str(e)}")
                upload_success = False
        else:
            print(f"\n⚠️ No data processed, skipping upload")

        # Send email notification for due samples regardless of upload success
        email_success = True
        if all_due_samples:
            print(f"\n📧 Sending email notification for {len(all_due_samples)} due samples...")
            email_success = send_email_notification(all_due_samples)
        else:
            print(f"\n📧 No due samples found, no email notification needed")

        # Create local backup if upload failed but we have data
        if not upload_success and len(all_collected_samples) > 0:
            try:
                backup_filename = f"Sampling_plan_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                with pd.ExcelWriter(backup_filename, engine='openpyxl') as writer:
                    for sheet_name, df in updated_sheets.items():
                        # Apply formatting before saving
                        df_formatted = processor.format_dataframe_for_excel(df)
                        df_formatted.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        # Auto-fit column widths
                        worksheet = writer.sheets[sheet_name]
                        processor.auto_fit_columns(worksheet, df_formatted)
                        
                print(f"💾 Created local backup: {backup_filename}")
            except Exception as e:
                print(f"❌ Failed to create local backup: {str(e)}")

        # Final status determination
        print(f"\n🏁 Kết quả cuối cùng:")
        print(f"  - Xử lý dữ liệu: {'✅' if len(all_collected_samples) > 0 else '❌'}")
        print(f"  - Upload SharePoint: {'✅' if upload_success else '❌'}")
        print(f"  - Email thông báo: {'✅' if email_success else '❌'}")
        print(f"  - Lịch sử kiểm mẫu: {'✅' if all_collected_samples else '⏭️ (không có dữ liệu)'}")

        # Determine overall success
        # Success if we processed data successfully (upload failure is acceptable due to lock issues)
        if len(all_collected_samples) > 0:
            if upload_success:
                print("✅ Hoàn thành cập nhật thành công!")
            else:
                print("⚠️ Hoàn thành xử lý với cảnh báo - File không thể upload do bị lock hoặc lỗi khác")
                print("💡 Dữ liệu đã được xử lý và email thông báo đã gửi")
                print("💡 Vui lòng kiểm tra file trên SharePoint và đóng nếu đang mở, sau đó chạy lại workflow")
            return True
        else:
            print("❌ Không có dữ liệu được xử lý thành công")
            print("💡 Vui lòng kiểm tra cấu trúc file Excel và đảm bảo có dữ liệu hợp lệ")
            return False

    except Exception as e:
        print(f"Lỗi: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return False

# Run the update if executed directly
if __name__ == "__main__":
    success = run_update()
    if success:
        print("✅ QA Sampling automation completed successfully!")
    else:
        print("❌ QA Sampling automation failed!")
        sys.exit(1)
