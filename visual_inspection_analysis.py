# Enhanced Visual Inspection + Production Data Analysis
# FIXES: Date alignment issue + Added defect name tracking + QA analytics
# VERSION: 2.0 Enhanced

import pandas as pd
import re
from datetime import datetime, timedelta
import os
import sys
import json
import requests
import io
import base64
import traceback
from calendar import monthrange
import urllib.parse

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

# SharePoint Configuration
SHAREPOINT_CONFIG = {
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Files.ReadWrite.All', 'https://graph.microsoft.com/Sites.ReadWrite.All'],
    'site_name': 'MCH.MMB.QA',
    'base_url': 'masangroup.sharepoint.com',
    'onedrive_user_email': 'cuchtk@msc.masangroup.com',
    'onedrive_base_url': 'masangroup-my.sharepoint.com',
    'production_folder_path': '/Documents/HUYNH THI KIM CUC/ERP MMB/BÁO CÁO THÁNG/2025'
}

# SharePoint File IDs
SHAREPOINT_FILE_IDS = {
    'visual_inspection': '77FDDE39-0853-46EC-8BFB-0546460A3266',  # Visual Inspection_16092025.xlsx
    'fs_data_output': 'CDEBFC69-10BD-42F0-B777-3633405B072B',    # FS data.xlsx output
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
        
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get public key: {response.status_code} - {response.text}")
    
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
            
            response = requests.put(url, headers=headers, json=data, timeout=30)
            if response.status_code in [201, 204]:
                print(f"✅ Successfully updated {secret_name}")
                return True
            else:
                print(f"❌ Failed to update {secret_name}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error updating secret {secret_name}: {str(e)}")
            return False

class SharePointProcessor:
    """SharePoint integration class for authentication and data processing"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.site_id = None
        self.msal_app = None
        
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
                self.log(f"Warning: Unexpected response code: {response.status_code} - {response.text}")
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
                self.log("⚠️ No GITHUB_TOKEN found, skipping secrets update")
                return False
            
            repo = os.environ.get('GITHUB_REPOSITORY', '')
            if '/' not in repo:
                self.log("⚠️ Invalid GITHUB_REPOSITORY format, skipping secrets update")
                return False
            
            repo_owner, repo_name = repo.split('/')
            updater = GitHubSecretsUpdater(repo_owner, repo_name, github_token)
            
            success = True
            if self.access_token:
                if not updater.update_secret('SHAREPOINT_ACCESS_TOKEN', self.access_token):
                    success = False
            if self.refresh_token:
                if not updater.update_secret('SHAREPOINT_REFRESH_TOKEN', self.refresh_token):
                    success = False
            
            if success:
                self.log("✅ Successfully updated GitHub secrets")
            else:
                self.log("⚠️ Some GitHub secrets updates failed, continuing execution")
            return success
            
        except Exception as e:
            self.log(f"⚠️ Error updating GitHub Secrets: {str(e)}, continuing execution")
            return False

    def get_headers(self):
        """Get headers for API requests"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_folder_id_by_path(self, folder_path, user_email=None):
        """Get folder ID by path using Graph API"""
        try:
            encoded_path = urllib.parse.quote(folder_path)
            if user_email:
                url = f"{self.base_url}/users/{user_email}/drive/root:{encoded_path}"
            else:
                url = f"{self.base_url}/me/drive/root:{encoded_path}"
            
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            
            if response.status_code == 200:
                folder_info = response.json()
                folder_id = folder_info.get('id')
                self.log(f"✅ Found folder ID for path '{folder_path}': {folder_id}")
                return folder_id
            else:
                self.log(f"❌ Failed to get folder by path '{folder_path}': {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.log(f"❌ Error getting folder by path '{folder_path}': {str(e)}")
            return None

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
                self.log(f"❌ Error getting SharePoint site ID: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.log(f"❌ Error getting SharePoint site ID: {str(e)}")
            return None

    def download_excel_file_by_id(self, file_id, description="", source_type="sharepoint"):
        """Download Excel file from SharePoint or OneDrive by file ID"""
        try:
            self.log(f"📥 Downloading {description} from {source_type.upper()}...")
            if source_type == "onedrive":
                owner_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
                if owner_email:
                    url = f"{self.base_url}/users/{owner_email}/drive/items/{file_id}"
                else:
                    url = f"{self.base_url}/me/drive/items/{file_id}"
            else:
                site_id = self.get_site_id()
                if not site_id:
                    self.log("❌ Cannot get SharePoint site ID")
                    return None
                url = f"{self.base_url}/sites/{site_id}/drive/items/{file_id}"
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            if response.status_code == 200:
                file_info = response.json()
                download_url = file_info.get('@microsoft.graph.downloadUrl')
                if not download_url:
                    self.log(f"❌ No download URL found for {description}")
                    return None
                file_name = file_info.get('name', 'Unknown')
                self.log(f"✅ Found file: {file_name}")
                
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
                    self.log(f"❌ Error downloading file content: {file_response.status_code} - {file_response.text}")
                    return None
            else:
                self.log(f"❌ Error getting file info: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.log(f"❌ Error downloading {description}: {str(e)}")
            return None

    def list_folder_contents(self, folder_id, source_type="onedrive"):
        """List contents of a folder"""
        try:
            if source_type == "onedrive":
                owner_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
                if owner_email:
                    url = f"{self.base_url}/users/{owner_email}/drive/items/{folder_id}/children"
                else:
                    url = f"{self.base_url}/me/drive/items/{folder_id}/children"
            else:
                site_id = self.get_site_id()
                if not site_id:
                    return []
                url = f"{self.base_url}/sites/{site_id}/drive/items/{folder_id}/children"
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            if response.status_code == 200:
                folder_data = response.json()
                return folder_data.get('value', [])
            else:
                self.log(f"❌ Error listing folder contents: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            self.log(f"❌ Error listing folder contents: {str(e)}")
            return []

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
                self.log(f"❌ Error uploading to SharePoint: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.log(f"❌ Error uploading to SharePoint: {str(e)}")
            return False

def parse_lot_to_date(lot_code):
    """Parse lot code (DDMMYY) to datetime object"""
    if not lot_code or len(str(lot_code)) < 6:
        return None
    try:
        lot_str = str(lot_code)[:6]
        day = int(lot_str[:2])
        month = int(lot_str[2:4])
        year = int("20" + lot_str[4:6])
        return datetime(year, month, day)
    except (ValueError, TypeError):
        return None

def process_visual_inspection_data(visual_df):
    """ENHANCED: Process visual inspection data with defect names"""
    processed_data = []
    
    print("Processing visual inspection data...")
    print(f"Columns available: {list(visual_df.columns)}")
    
    for _, row in visual_df.iterrows():
        item = row.get('Item', '')
        lot = row.get('Lot', '')
        retest = str(row.get('Retest', '')).upper()
        reject_qty = row.get('Reject qty', 0)
        defect_result = str(row.get('Defect result', '')).upper()
        
        # NEW: Thêm defect name
        defect_name = row.get('Defect name', '')
        if pd.isna(defect_name):
            defect_name = ''
        else:
            defect_name = str(defect_name).strip()
        
        # NEW: Thêm thông tin bổ sung để phân tích
        inspector = row.get('Inspector', '')
        if pd.isna(inspector):
            inspector = ''
        
        prod_date = parse_lot_to_date(lot)
        if not prod_date:
            continue
            
        hold_qty = 0
        defect_qty = 0
        
        # Logic xử lý như cũ nhưng thêm defect_name
        if defect_result == 'FAIL':
            if retest == 'YES':
                defect_qty = reject_qty
            elif retest == 'NO':
                hold_qty = reject_qty
        
        processed_data.append({
            'Date': prod_date,
            'Item': item,
            'Lot': lot,
            'Hold_Qty': hold_qty,
            'Defect_Qty': defect_qty,
            'Defect_Name': defect_name,
            'Inspector': inspector,
            'Retest': retest,
            'Defect_Result': defect_result,
            'Original_Reject_Qty': reject_qty
        })
    
    result_df = pd.DataFrame(processed_data)
    
    # Debug info
    print(f"Processed {len(result_df)} inspection records")
    if not result_df.empty:
        print(f"Unique defect names: {result_df['Defect_Name'].unique()}")
        print(f"Defect summary:")
        defect_summary = result_df[result_df['Defect_Name'] != ''].groupby('Defect_Name').agg({
            'Hold_Qty': 'sum',
            'Defect_Qty': 'sum'
        }).sort_values('Hold_Qty', ascending=False)
        print(defect_summary)
    
    return result_df

def find_production_files_by_month(sp_processor, target_months):
    """Find FS production files for specific months using direct path and search fallback"""
    production_files = {}
    user_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
    
    try:
        # Try direct path first
        main_folder_path = SHAREPOINT_CONFIG['production_folder_path']
        sp_processor.log(f"🔍 Getting 2025 folder by path: {main_folder_path}")
        main_folder_id = sp_processor.get_folder_id_by_path(main_folder_path, user_email=user_email)
        
        if not main_folder_id:
            sp_processor.log("⚠️ Direct path failed, attempting search for '2025' folder...")
            search_url = f"{sp_processor.base_url}/users/{user_email}/drive/search(q='2025')"
            response = requests.get(search_url, headers=sp_processor.get_headers(), timeout=30)
            
            if response.status_code == 200:
                search_results = response.json().get('value', [])
                sp_processor.log(f"Found {len(search_results)} items matching '2025'")
                for item in search_results:
                    if (item.get('folder') and 
                        item.get('name') == '2025' and 
                        'BÁO CÁO THÁNG' in str(item.get('parentReference', {}).get('path', '')).upper()):
                        main_folder_id = item.get('id')
                        sp_processor.log(f"✅ Found 2025 folder via search: {main_folder_id}")
                        break
                if not main_folder_id:
                    for item in search_results:
                        if item.get('folder') and item.get('name') == '2025':
                            main_folder_id = item.get('id')
                            sp_processor.log(f"✅ Using fallback 2025 folder: {main_folder_id}")
                            break
            else:
                sp_processor.log(f"❌ Search failed: {response.status_code} - {response.text}")
        
        if not main_folder_id:
            sp_processor.log("❌ Could not find 2025 folder")
            return production_files
        
        # List main folder contents
        sp_processor.log(f"📂 Listing contents of 2025 folder (ID: {main_folder_id})...")
        main_contents = sp_processor.list_folder_contents(main_folder_id, "onedrive")
        
        month_folders = {}
        for item in main_contents:
            if item.get('folder') and item.get('name'):
                folder_name = item.get('name')
                sp_processor.log(f"   Found folder: '{folder_name}'")
                for month in target_months:
                    month_patterns = [
                        f"tháng {month}.2025",
                        f"thang {month}.2025",
                        f"tháng {month:02d}.2025",
                        f"thang {month:02d}.2025",
                        f"T{month:02d}.2025",
                        f"t{month:02d}.2025",
                        f"Tháng {month:02d}.2025",
                        f"THÁNG {month:02d}.2025"
                    ]
                    for pattern in month_patterns:
                        if pattern.lower() in folder_name.lower():
                            month_folders[month] = item.get('id')
                            sp_processor.log(f"✅ Matched month {month} folder: '{folder_name}'")
                            break
        
        sp_processor.log(f"📊 Found {len(month_folders)} month folders for target months: {list(month_folders.keys())}")
        
        # Find BC FS files in each month folder
        for month, folder_id in month_folders.items():
            sp_processor.log(f"🔍 Looking for BC FS file in month {month} folder...")
            folder_contents = sp_processor.list_folder_contents(folder_id, "onedrive")
            
            for item in folder_contents:
                if not item.get('name'):
                    continue
                item_name = item.get('name')
                sp_processor.log(f"   Checking file: '{item_name}'")
                fs_patterns = [
                    f'BC FS T{month:02d}',
                    f'BC_FS_T{month:02d}',
                    f'FS T{month:02d}',
                    'BC FS'
                ]
                item_name_upper = item_name.upper()
                for pattern in fs_patterns:
                    if (pattern.upper() in item_name_upper and 
                        (item_name.endswith('.xlsx') or item_name.endswith('.xlsb'))):
                        production_files[month] = item.get('id')
                        sp_processor.log(f"✅ Found BC FS file for month {month}: '{item_name}'")
                        break
                if month in production_files:
                    break
            if month not in production_files:
                sp_processor.log(f"⚠️ No BC FS file found for month {month}")
    
    except Exception as e:
        sp_processor.log(f"❌ Error finding production files: {str(e)}")
        traceback.print_exc()
    
    sp_processor.log(f"🎯 Final result: Found BC FS files for {len(production_files)} months: {list(production_files.keys())}")
    return production_files

def extract_production_data(production_sheets, month):
    """FIXED: Extract production data with corrected date alignment"""
    try:
        sheet_name = 'OEE trừ DNP'
        if sheet_name not in production_sheets:
            print(f"Sheet '{sheet_name}' not found in production file")
            print(f"Available sheets: {list(production_sheets.keys())}")
            return pd.DataFrame()
        
        df = production_sheets[sheet_name]
        
        if len(df) < 287:
            print(f"Production file doesn't have enough rows (has {len(df)}, need at least 287)")
            return pd.DataFrame()
        
        if len(df.columns) < 10:
            print(f"Production file doesn't have enough columns (has {len(df.columns)}, need at least 10)")
            return pd.DataFrame()
        
        current_year = 2025
        production_data = []
        days_in_month = monthrange(current_year, month)[1]
        
        print(f"Extracting production data for {days_in_month} days in month {month}/{current_year}")
        
        # FIXED: Test different base rows to find correct alignment
        # Based on your data, row 256 (index 255) seems to be day 1, so base should be 254
        base_row = 254  # Start from row 255 (index 254) for day 1
        
        print(f"🔍 DEBUG: Testing row alignment for month {month}...")
        for test_day in range(1, min(6, days_in_month + 1)):
            test_row = base_row + test_day  # 254 + 1 = 255 for day 1
            if test_row < len(df):
                test_value = df.iloc[test_row, 9]
                print(f"   Day {test_day:02d} → Row {test_row+1} (index {test_row}): {test_value}")
        
        for day in range(1, days_in_month + 1):
            # FIXED: Corrected formula - row 255 (index 254) should be day 1
            row_index = base_row + day  # 254 + 1 = 255 for day 1, etc.
            
            if row_index < len(df):
                prod_date = datetime(current_year, month, day)
                production_qty = df.iloc[row_index, 9]  # Column J (index 9)
                
                if pd.notna(production_qty):
                    try:
                        production_qty = float(production_qty)
                        if production_qty < 0:
                            production_qty = 0
                    except (ValueError, TypeError):
                        production_qty = 0
                else:
                    production_qty = 0
                
                production_data.append({
                    'Date': prod_date,
                    'Production_Qty': production_qty,
                    'Day': day,
                    'Row_Index': row_index + 1  # +1 for Excel row numbering
                })
                
                # Debug info for first few days
                if day <= 5:
                    print(f"✅ Day {day:02d} (Excel row {row_index+1}): {production_qty:,.0f}")
        
        result_df = pd.DataFrame(production_data)
        print(f"✅ Extracted {len(result_df)} days of production data")
        print(f"📊 Total production for month {month}: {result_df['Production_Qty'].sum():,.0f}")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Error extracting production data: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()

def merge_data_and_calculate_rates_enhanced(visual_processed, production_data):
    """ENHANCED: Merge function with defect name handling"""
    
    # Group visual inspection data by date, item, and defect name
    visual_grouped = visual_processed.groupby(['Date', 'Item', 'Defect_Name']).agg({
        'Hold_Qty': 'sum',
        'Defect_Qty': 'sum',
        'Lot': lambda x: ', '.join(x.unique())  # Combine lots
    }).reset_index()
    
    print(f"Visual data grouped with defect names: {len(visual_grouped)} combinations")
    
    # Group production data by date
    production_grouped = production_data.groupby('Date')['Production_Qty'].sum().reset_index()
    print(f"Production data grouped: {len(production_grouped)} dates")
    
    # Create a base dataset with all production dates
    base_dates = production_grouped[['Date', 'Production_Qty']].copy()
    base_dates['Item'] = 'All Items'
    base_dates['Defect_Name'] = ''
    base_dates['Hold_Qty'] = 0
    base_dates['Defect_Qty'] = 0
    base_dates['Lot'] = ''
    
    # Merge visual data with production data
    merged_visual = pd.merge(visual_grouped, production_grouped, on='Date', how='left')
    
    # Combine base dates with visual data
    combined_data = pd.concat([base_dates, merged_visual], ignore_index=True)
    
    # Remove duplicates where we have both 'All Items' and specific defect data for same date
    # Keep specific defect data, remove generic 'All Items' entries for dates with defects
    dates_with_defects = combined_data[combined_data['Defect_Name'] != '']['Date'].unique()
    filtered_data = combined_data[
        ~((combined_data['Item'] == 'All Items') & (combined_data['Date'].isin(dates_with_defects)))
    ]
    
    # Fill missing production quantities for visual records
    filtered_data['Production_Qty'] = filtered_data.groupby('Date')['Production_Qty'].transform(
        lambda x: x.fillna(method='ffill').fillna(method='bfill')
    )
    
    # Fill remaining missing values
    filtered_data['Production_Qty'] = filtered_data['Production_Qty'].fillna(0)
    filtered_data['Hold_Qty'] = filtered_data['Hold_Qty'].fillna(0)
    filtered_data['Defect_Qty'] = filtered_data['Defect_Qty'].fillna(0)
    
    # Calculate hold rate
    filtered_data['Hold_Rate'] = filtered_data.apply(
        lambda row: (row['Hold_Qty'] / row['Production_Qty'] * 100) if row['Production_Qty'] > 0 else 0, 
        axis=1
    )
    
    # Round hold rate to 2 decimal places
    filtered_data['Hold_Rate'] = filtered_data['Hold_Rate'].round(2)
    
    # Format date for output
    filtered_data['Date_Formatted'] = filtered_data['Date'].dt.strftime('%m/%d/%Y')
    
    # Rename columns for Vietnamese output
    final_df = filtered_data.rename(columns={
        'Date_Formatted': 'Ngày',
        'Item': 'Item',
        'Defect_Name': 'Tên lỗi',
        'Lot': 'Lot',
        'Production_Qty': 'Sản lượng',
        'Hold_Qty': 'Số lượng hold',
        'Defect_Qty': 'Số lượng lỗi',
        'Hold_Rate': 'Tỉ lệ hold (%)'
    })
    
    # Select final columns
    final_columns = ['Ngày', 'Item', 'Tên lỗi', 'Lot', 'Sản lượng', 'Số lượng hold', 'Số lượng lỗi', 'Tỉ lệ hold (%)']
    result_df = final_df[final_columns]
    
    # Sort by date descending, then by defect name
    result_df = result_df.sort_values(['Ngày', 'Tên lỗi', 'Item'], ascending=[False, True, True])
    
    return result_df

def generate_defect_analysis_report(final_df):
    """Generate comprehensive defect analysis report for QA"""
    
    print("\n" + "="*60)
    print("📊 DEFECT ANALYSIS REPORT FOR QA")
    print("="*60)
    
    # Convert date back to datetime for analysis
    final_df_analysis = final_df.copy()
    final_df_analysis['Date'] = pd.to_datetime(final_df_analysis['Ngày'], format='%m/%d/%Y')
    final_df_analysis['Month'] = final_df_analysis['Date'].dt.month
    final_df_analysis['Week'] = final_df_analysis['Date'].dt.isocalendar().week
    
    # 1. Overall Statistics
    total_production = final_df_analysis['Sản lượng'].sum()
    total_hold = final_df_analysis['Số lượng hold'].sum()
    total_defect = final_df_analysis['Số lượng lỗi'].sum()
    overall_hold_rate = (total_hold / total_production * 100) if total_production > 0 else 0
    
    print(f"📈 OVERALL STATISTICS:")
    print(f"   Total Production: {total_production:,.0f}")
    print(f"   Total Hold Quantity: {total_hold:,.0f}")
    print(f"   Total Defect Quantity: {total_defect:,.0f}")
    print(f"   Overall Hold Rate: {overall_hold_rate:.2f}%")
    print()
    
    # 2. Top Defect Types Analysis
    defect_analysis = final_df_analysis[final_df_analysis['Tên lỗi'] != ''].groupby('Tên lỗi').agg({
        'Số lượng hold': 'sum',
        'Số lượng lỗi': 'sum',
        'Sản lượng': 'sum'
    }).reset_index()
    
    if not defect_analysis.empty:
        defect_analysis['Total_Issues'] = defect_analysis['Số lượng hold'] + defect_analysis['Số lượng lỗi']
        defect_analysis['Defect_Rate'] = (defect_analysis['Total_Issues'] / defect_analysis['Sản lượng'] * 100)
        defect_analysis = defect_analysis.sort_values('Total_Issues', ascending=False)
        
        print(f"🔍 TOP DEFECT TYPES:")
        for _, row in defect_analysis.head(10).iterrows():
            print(f"   {row['Tên lỗi']}: {row['Total_Issues']:,.0f} issues ({row['Defect_Rate']:.2f}% rate)")
        print()
    else:
        defect_analysis = pd.DataFrame()
        print("🔍 No defect data found for analysis")
        print()
    
    # 3. Item-wise Analysis
    item_analysis = final_df_analysis[final_df_analysis['Item'] != 'All Items'].groupby('Item').agg({
        'Số lượng hold': 'sum',
        'Số lượng lỗi': 'sum',
        'Sản lượng': 'sum'
    }).reset_index()
    
    if not item_analysis.empty:
        item_analysis['Total_Issues'] = item_analysis['Số lượng hold'] + item_analysis['Số lượng lỗi']
        item_analysis['Hold_Rate'] = (item_analysis['Số lượng hold'] / item_analysis['Sản lượng'] * 100)
        item_analysis = item_analysis.sort_values('Hold_Rate', ascending=False)
        
        print(f"📦 ITEM-WISE ANALYSIS (Top 10 by Hold Rate):")
        for _, row in item_analysis.head(10).iterrows():
            if row['Hold_Rate'] > 0:
                print(f"   {row['Item']}: {row['Hold_Rate']:.2f}% hold rate ({row['Số lượng hold']:,.0f}/{row['Sản lượng']:,.0f})")
        print()
    else:
        item_analysis = pd.DataFrame()
        print("📦 No item-specific data found for analysis")
        print()
    
    # 4. Monthly Trend Analysis
    monthly_analysis = final_df_analysis.groupby('Month').agg({
        'Số lượng hold': 'sum',
        'Số lượng lỗi': 'sum',
        'Sản lượng': 'sum'
    }).reset_index()
    
    monthly_analysis['Hold_Rate'] = (monthly_analysis['Số lượng hold'] / monthly_analysis['Sản lượng'] * 100)
    
    print(f"📅 MONTHLY TREND:")
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    for _, row in monthly_analysis.iterrows():
        month_name = month_names[int(row['Month']) - 1]
        print(f"   {month_name} 2025: {row['Hold_Rate']:.2f}% hold rate")
    print()
    
    # 5. Critical Issues Identification
    if not defect_analysis.empty:
        critical_defects = defect_analysis[defect_analysis['Defect_Rate'] > 1.0]  # >1% defect rate
        if not critical_defects.empty:
            print(f"🚨 CRITICAL DEFECTS (>1% rate):")
            for _, row in critical_defects.iterrows():
                print(f"   ⚠️ {row['Tên lỗi']}: {row['Defect_Rate']:.2f}% - Immediate attention required")
        else:
            print(f"✅ No critical defects identified (all <1% rate)")
    else:
        print(f"ℹ️ No defect data available for critical analysis")
    print()
    
    return {
        'defect_analysis': defect_analysis,
        'item_analysis': item_analysis,
        'monthly_analysis': monthly_analysis,
        'overall_stats': {
            'total_production': total_production,
            'total_hold': total_hold,
            'overall_hold_rate': overall_hold_rate
        }
    }

def create_qa_dashboard_data(final_df):
    """Create data structure optimized for QA dashboard/visualization"""
    
    # Convert date for processing
    dashboard_df = final_df.copy()
    dashboard_df['Date'] = pd.to_datetime(dashboard_df['Ngày'], format='%m/%d/%Y')
    
    # Daily summary for trend charts
    daily_summary = dashboard_df.groupby('Date').agg({
        'Sản lượng': 'first',  # Production is same per day
        'Số lượng hold': 'sum',
        'Số lượng lỗi': 'sum'
    }).reset_index()
    
    daily_summary['Hold_Rate'] = (daily_summary['Số lượng hold'] / daily_summary['Sản lượng'] * 100)
    daily_summary['Date_Formatted'] = daily_summary['Date'].dt.strftime('%Y-%m-%d')
    
    # Defect type distribution
    defect_distribution = dashboard_df[dashboard_df['Tên lỗi'] != ''].groupby('Tên lỗi').agg({
        'Số lượng hold': 'sum',
        'Số lượng lỗi': 'sum'
    }).reset_index()
    
    if not defect_distribution.empty:
        defect_distribution['Total_Issues'] = defect_distribution['Số lượng hold'] + defect_distribution['Số lượng lỗi']
    
    # Item performance matrix
    item_performance = dashboard_df[dashboard_df['Item'] != 'All Items'].groupby(['Item', 'Tên lỗi']).agg({
        'Số lượng hold': 'sum',
        'Số lượng lỗi': 'sum',
        'Sản lượng': 'sum'
    }).reset_index()
    
    return {
        'daily_trend': daily_summary,
        'defect_distribution': defect_distribution,
        'item_defect_matrix': item_performance
    }

def validate_data_quality(final_df):
    """Validate data quality for QA analysis"""
    
    print("\n🔍 DATA QUALITY VALIDATION:")
    
    issues = []
    
    # Check for missing production data
    zero_production_days = final_df[final_df['Sản lượng'] == 0]
    if not zero_production_days.empty:
        issues.append(f"Found {len(zero_production_days)} days with zero production")
    
    # Check for extreme hold rates
    high_hold_rates = final_df[final_df['Tỉ lệ hold (%)'] > 10]  # >10% seems extreme
    if not high_hold_rates.empty:
        issues.append(f"Found {len(high_hold_rates)} records with >10% hold rate")
    
    # Check for missing defect names when there are holds
    missing_defect_names = final_df[
        (final_df['Số lượng hold'] > 0) & 
        (final_df['Tên lỗi'] == '')
    ]
    if not missing_defect_names.empty:
        issues.append(f"Found {len(missing_defect_names)} hold records without defect names")
    
    # Check for duplicate dates with same items
    duplicates = final_df.groupby(['Ngày', 'Item', 'Tên lỗi']).size()
    duplicates = duplicates[duplicates > 1]
    if not duplicates.empty:
        issues.append(f"Found {len(duplicates)} potential duplicate records")
    
    if issues:
        print("⚠️ DATA QUALITY ISSUES FOUND:")
        for issue in issues:
            print(f"   - {issue}")
        print("   Please review and clean data before analysis")
    else:
        print("✅ Data quality validation passed")
    
    return len(issues) == 0

def main():
    print("="*80)
    print("🔄 ENHANCED VISUAL INSPECTION + PRODUCTION DATA ANALYSIS")
    print("🎯 WITH DEFECT NAME TRACKING & CORRECTED DATE ALIGNMENT")
    print("🏭 OPTIMIZED FOR FMCG QA OPERATIONS")
    print("="*80)

    try:
        print("\n🔗 Initializing SharePoint/OneDrive connection...")
        sp_processor = SharePointProcessor()
        print("✅ SharePoint/OneDrive connection established")
        
        print("\n📥 Loading Visual Inspection data...")
        visual_sheets = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['visual_inspection'],
            "Visual Inspection data",
            source_type="sharepoint"
        )
        
        if not visual_sheets:
            print("❌ Failed to download Visual Inspection data")
            sys.exit(1)
        
        visual_df = None
        for sheet_name, df in visual_sheets.items():
            if len(df) > 10:
                visual_df = df
                print(f"✅ Using sheet '{sheet_name}' with {len(df)} records")
                print(f"   Columns: {list(df.columns)}")
                break
        
        if visual_df is None or visual_df.empty:
            print("❌ No valid Visual Inspection data found")
            sys.exit(1)
        
        print("\n🔄 Processing Visual Inspection data with defect names...")
        visual_processed = process_visual_inspection_data(visual_df)
        print(f"✅ Processed {len(visual_processed)} inspection records")
        
        if visual_processed.empty:
            print("❌ No valid inspection data after processing")
            sys.exit(1)
        
        unique_months = visual_processed['Date'].dt.month.unique()
        unique_months = [int(month) for month in unique_months]
        print(f"📅 Need production data for months: {sorted(unique_months)}")
        
        print("\n📥 Finding production files...")
        production_files = find_production_files_by_month(sp_processor, unique_months)
        
        all_production_data = []
        if production_files:
            print("\n📊 Processing production data with FIXED date alignment...")
            for month, file_id in production_files.items():
                print(f"📥 Processing month {month}...")
                production_sheets = sp_processor.download_excel_file_by_id(
                    file_id,
                    f"FS production data for month {month}",
                    source_type="onedrive"
                )
                
                if production_sheets:
                    # Use the FIXED extraction function
                    month_production = extract_production_data(production_sheets, month)
                    if not month_production.empty:
                        all_production_data.append(month_production)
                        print(f"✅ Extracted {len(month_production)} production records for month {month}")
                        
                        # Show sample for verification
                        sample_data = month_production.head(3)
                        print("   📋 Sample data verification:")
                        for _, row in sample_data.iterrows():
                            print(f"     {row['Date'].strftime('%m/%d/%Y')}: {row['Production_Qty']:,.0f} units (Excel Row {row['Row_Index']})")
                    else:
                        print(f"⚠️ No production data extracted for month {month}")
                else:
                    print(f"❌ Failed to download production file for month {month}")
        
        if not all_production_data:
            print("⚠️ No production data found, proceeding with visual inspection data only")
            # Create simplified report without production data
            final_report = visual_processed.groupby(['Date', 'Item', 'Defect_Name']).agg({
                'Hold_Qty': 'sum',
                'Defect_Qty': 'sum',
                'Lot': lambda x: ', '.join(x.unique())
            }).reset_index()
            final_report['Production_Qty'] = 0
            final_report['Hold_Rate'] = 0
            final_report['Date_Formatted'] = final_report['Date'].dt.strftime('%m/%d/%Y')
            
            final_report = final_report.rename(columns={
                'Date_Formatted': 'Ngày',
                'Item': 'Item',
                'Defect_Name': 'Tên lỗi',
                'Lot': 'Lot',
                'Production_Qty': 'Sản lượng',
                'Hold_Qty': 'Số lượng hold',
                'Defect_Qty': 'Số lượng lỗi',
                'Hold_Rate': 'Tỉ lệ hold (%)'
            })
        else:
            combined_production = pd.concat(all_production_data, ignore_index=True)
            print(f"✅ Combined production data: {len(combined_production)} records")
            
            print("\n🔄 Merging data with enhanced defect tracking...")
            final_report = merge_data_and_calculate_rates_enhanced(visual_processed, combined_production)
        
        if final_report.empty:
            print("❌ No data could be merged")
            sys.exit(1)
        
        print(f"✅ Final report generated: {len(final_report)} records")
        
        # NEW: Data quality validation
        print("\n🔍 Performing data quality validation...")
        data_quality_ok = validate_data_quality(final_report)
        
        # NEW: Generate comprehensive QA analysis
        analysis_results = generate_defect_analysis_report(final_report)
        
        # NEW: Create dashboard-ready data
        dashboard_data = create_qa_dashboard_data(final_report)
        
        print(f"\n📊 ENHANCED SUMMARY STATISTICS:")
        print(f"   - Total records: {len(final_report):,}")
        unique_defects = final_report[final_report['Tên lỗi'] != '']['Tên lỗi'].unique()
        print(f"   - Unique defect types: {len(unique_defects)}")
        if len(unique_defects) > 0:
            print(f"     Top defects: {', '.join(unique_defects[:5])}")
        print(f"   - Date range: {final_report['Ngày'].min()} to {final_report['Ngày'].max()}")
        unique_items = final_report[final_report['Item'] != 'All Items']['Item'].unique()
        print(f"   - Unique items: {len(unique_items)}")
        if len(unique_items) > 0:
            print(f"     Items analyzed: {', '.join(unique_items[:3])}" + ("..." if len(unique_items) > 3 else ""))
        
        print(f"\n📤 Uploading enhanced results to SharePoint...")
        
        # Upload main data to SharePoint
        success = sp_processor.upload_excel_to_sharepoint(
            final_report,  # Main sheet for compatibility
            SHAREPOINT_FILE_IDS['fs_data_output'],
            'FS_Analysis_Enhanced'
        )
        
        if success:
            print("✅ Enhanced results successfully uploaded to SharePoint!")
        else:
            print("❌ Failed to upload results to SharePoint")
        
        # Always create comprehensive local backup
        print("💾 Creating comprehensive backup with all analysis sheets...")
        backup_filename = f"FS_Analysis_Enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        try:
            with pd.ExcelWriter(backup_filename, engine='openpyxl') as writer:
                # Main data sheet
                final_report.to_excel(writer, sheet_name='Main_Data', index=False)
                
                # Analysis sheets
                if 'defect_analysis' in analysis_results and not analysis_results['defect_analysis'].empty:
                    analysis_results['defect_analysis'].to_excel(writer, sheet_name='Defect_Analysis', index=False)
                
                if 'item_analysis' in analysis_results and not analysis_results['item_analysis'].empty:
                    analysis_results['item_analysis'].to_excel(writer, sheet_name='Item_Analysis', index=False)
                
                if 'monthly_analysis' in analysis_results and not analysis_results['monthly_analysis'].empty:
                    analysis_results['monthly_analysis'].to_excel(writer, sheet_name='Monthly_Trend', index=False)
                
                # Dashboard data
                if not dashboard_data['daily_trend'].empty:
                    dashboard_data['daily_trend'].to_excel(writer, sheet_name='Daily_Trend', index=False)
                
                if not dashboard_data['defect_distribution'].empty:
                    dashboard_data['defect_distribution'].to_excel(writer, sheet_name='Defect_Distribution', index=False)
                
                if not dashboard_data['item_defect_matrix'].empty:
                    dashboard_data['item_defect_matrix'].to_excel(writer, sheet_name='Item_Defect_Matrix', index=False)
            
            print(f"✅ Comprehensive backup saved: {backup_filename}")
        except Exception as e:
            print(f"⚠️ Error creating backup: {str(e)}")
            # Fallback to simple backup
            final_report.to_excel(f"FS_Simple_Backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", index=False)
            print("✅ Simple backup created instead")
            
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

    print("\n" + "="*80)
    print("✅ ENHANCED VISUAL INSPECTION ANALYSIS COMPLETED!")
    print("✅ KEY IMPROVEMENTS IMPLEMENTED:")
    print("   🔧 FIXED: Production date alignment issue (corrected row mapping)")
    print("   📝 NEW: Defect name tracking for root cause analysis")
    print("   📊 NEW: Comprehensive QA analytics and reporting")
    print("   🔍 NEW: Data quality validation")
    print("   📈 NEW: Dashboard-ready data exports")
    print("   💼 NEW: Multiple analysis sheets for management insights")
    print("✅ READY FOR:")
    print("   🎯 Pareto analysis of defects")
    print("   📊 SPC implementation")
    print("   🔮 Predictive quality modeling")
    print("   📱 Real-time dashboard creation")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)
