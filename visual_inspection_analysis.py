# Visual Inspection + Production Data Analysis
# Combines Visual Inspection data with production data to calculate hold rates

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
    'onedrive_user_email': 'cuchtk@msc.masangroup.com',  # Owner of production data files
    'onedrive_base_url': 'masangroup-my.sharepoint.com',
    'production_folder_path': '/Documents/HUYNH THI KIM CUC/ERP MMB/BÁO CÁO THÁNG/2025'
}

# SharePoint File IDs  
SHAREPOINT_FILE_IDS = {
    'visual_inspection': '77FDDE39-0853-46EC-8BFB-0546460A3266',  # Visual Inspection_16092025.xlsx
    'fs_data_output': 'CDEBFC69-10BD-42F0-B777-3633405B072B',    # FS data.xlsx output
    'production_folder_path': '/Documents/HUYNH THI KIM CUC/ERP MMB/BÁO CÁO THÁNG/2025'  # Folder path
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

    def get_folder_id_by_path(self, folder_path, user_email=None):
        """Get folder ID by path using Graph API"""
        try:
            if user_email:
                # For specific user's OneDrive
                url = f"{self.base_url}/users/{user_email}/drive/root:{folder_path}"
            else:
                # For current user
                url = f"{self.base_url}/me/drive/root:{folder_path}"
            
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            
            if response.status_code == 200:
                folder_info = response.json()
                folder_id = folder_info.get('id')
                self.log(f"✅ Found folder ID for path '{folder_path}': {folder_id}")
                return folder_id
            else:
                self.log(f"❌ Failed to get folder by path: {response.status_code}")
                return None
                
        except Exception as e:
            self.log(f"❌ Error getting folder by path: {str(e)}")
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
                self.log(f"❌ Error getting SharePoint site ID: {response.status_code}")
                return None

        except Exception as e:
            self.log(f"❌ Error getting SharePoint site ID: {str(e)}")
            return None

    def download_excel_file_by_id(self, file_id, description="", source_type="sharepoint"):
        """Download Excel file from SharePoint or OneDrive by file ID"""
        try:
            self.log(f"📥 Downloading {description} from {source_type.upper()}...")

            if source_type == "onedrive":
                # For OneDrive files
                owner_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
                if owner_email:
                    url = f"{self.base_url}/users/{owner_email}/drive/items/{file_id}"
                else:
                    url = f"{self.base_url}/me/drive/items/{file_id}"
            else:
                # For SharePoint files
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
                    self.log(f"❌ Error downloading file content: {file_response.status_code}")
            else:
                self.log(f"❌ Error getting file info: {response.status_code}")
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
                self.log(f"❌ Error listing folder contents: {response.status_code}")
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
                self.log(f"❌ Error uploading to SharePoint: {response.status_code}")
                self.log(f"Response: {response.text[:500] if response.text else 'Empty response'}")
                return False

        except Exception as e:
            self.log(f"❌ Error uploading to SharePoint: {str(e)}")
            return False

def parse_lot_to_date(lot_code):
    """Parse lot code (DDMMYY) to datetime object"""
    if not lot_code or len(str(lot_code)) < 6:
        return None
    
    try:
        lot_str = str(lot_code)[:6]  # Take first 6 characters
        day = int(lot_str[:2])
        month = int(lot_str[2:4])
        year = int("20" + lot_str[4:6])  # Assuming 20xx
        
        return datetime(year, month, day)
    except (ValueError, TypeError):
        return None

def get_month_from_date(date_obj):
    """Get month number from datetime object"""
    if date_obj:
        return date_obj.month
    return None

def process_visual_inspection_data(visual_df):
    """Process visual inspection data to calculate hold and defect quantities"""
    processed_data = []
    
    for _, row in visual_df.iterrows():
        item = row.get('Item', '')
        lot = row.get('Lot', '')
        retest = str(row.get('Retest', '')).upper()
        reject_qty = row.get('Reject qty', 0)
        defect_result = str(row.get('Defect result', '')).upper()
        
        # Parse lot to get production date
        prod_date = parse_lot_to_date(lot)
        if not prod_date:
            continue
            
        # Apply business logic for hold vs defect classification
        hold_qty = 0
        defect_qty = 0
        
        if defect_result == 'FAIL':
            if retest == 'YES':
                # This is defect after reprocessing
                defect_qty = reject_qty
            elif retest == 'NO':
                # This is hold quantity
                hold_qty = reject_qty
        
        processed_data.append({
            'Date': prod_date,
            'Item': item,
            'Lot': lot,
            'Hold_Qty': hold_qty,
            'Defect_Qty': defect_qty,
            'Retest': retest,
            'Defect_Result': defect_result,
            'Original_Reject_Qty': reject_qty
        })
    
    return pd.DataFrame(processed_data)

def find_production_files_by_month(sp_processor, target_months):
    """Find FS production files for specific months using search approach instead of path
    
    Search for '2025' folder first, then navigate to month folders
    """
    production_files = {}
    
    try:
        sp_processor.log(f"🔍 Searching for 2025 production folder...")
        
        # Search for 2025 folder using Graph API search
        user_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
        search_url = f"{sp_processor.base_url}/users/{user_email}/drive/search(q='2025')"
        
        response = requests.get(search_url, headers=sp_processor.get_headers(), timeout=30)
        
        main_folder_id = None
        if response.status_code == 200:
            search_results = response.json().get('value', [])
            sp_processor.log(f"Found {len(search_results)} items matching '2025'")
            
            # Look for the main 2025 folder in the production path
            for item in search_results:
                if (item.get('folder') and 
                    item.get('name') == '2025' and 
                    'BÁO CÁO THÁNG' in str(item.get('parentReference', {}).get('path', ''))):
                    
                    main_folder_id = item.get('id')
                    sp_processor.log(f"✅ Found main 2025 folder: {item.get('id')}")
                    sp_processor.log(f"   Path: {item.get('parentReference', {}).get('path', '')}")
                    break
            
            # Fallback: just take the first folder named '2025'
            if not main_folder_id:
                for item in search_results:
                    if item.get('folder') and item.get('name') == '2025':
                        main_folder_id = item.get('id')
                        sp_processor.log(f"✅ Using fallback 2025 folder: {item.get('id')}")
                        break
        else:
            sp_processor.log(f"❌ Search failed: {response.status_code}")
        
        if not main_folder_id:
            sp_processor.log("❌ Could not find 2025 folder")
            return production_files
            
        # List main folder contents to find month folders
        sp_processor.log(f"📂 Listing contents of 2025 folder...")
        main_contents = sp_processor.list_folder_contents(main_folder_id, "onedrive")
        
        month_folders = {}
        for item in main_contents:
            if item.get('folder') and item.get('name'):
                folder_name = item.get('name', '').lower()
                sp_processor.log(f"   Found folder: '{item.get('name')}'")
                
                # Match month folders with exact patterns from your structure
                for month in target_months:
                    month_patterns = [
                        f"tháng {month}.2025",
                        f"thang {month}.2025"
                    ]
                    
                    for pattern in month_patterns:
                        if pattern in folder_name:
                            month_folders[month] = item.get('id')
                            sp_processor.log(f"✅ Matched month {month} folder: '{item.get('name')}'")
                            break
                    if month in month_folders:
                        break
        
        sp_processor.log(f"📊 Found {len(month_folders)} month folders for target months: {list(month_folders.keys())}")
        
        # For each month folder, find the BC FS file
        for month, folder_id in month_folders.items():
            sp_processor.log(f"🔍 Looking for BC FS file in month {month} folder...")
            folder_contents = sp_processor.list_folder_contents(folder_id, "onedrive")
            
            for item in folder_contents:
                if not item.get('name'):
                    continue
                    
                item_name = item.get('name', '')
                sp_processor.log(f"   Checking file: '{item_name}'")
                
                # Look for BC FS files with patterns matching your structure
                fs_patterns = [
                    f'BC FS T{month:02d}',  # BC FS T01, BC FS T02, etc.
                    f'BC_FS_T{month:02d}',
                    f'FS T{month:02d}',
                    'BC FS'  # Generic fallback
                ]
                
                item_name_upper = item_name.upper()
                for pattern in fs_patterns:
                    if pattern.upper() in item_name_upper and item_name.endswith('.xlsx'):
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
    """Extract production data from FS file's 'OEE trừ DNP' sheet
    
    Structure: 
    - Row 256 (index 255) = Day 01
    - Row 257 (index 256) = Day 02
    - ...
    - Row 286 (index 285) = Day 31
    - Column J (index 9) = Production quantity
    """
    try:
        sheet_name = 'OEE trừ DNP'
        if sheet_name not in production_sheets:
            print(f"Sheet '{sheet_name}' not found in production file")
            print(f"Available sheets: {list(production_sheets.keys())}")
            return pd.DataFrame()
        
        df = production_sheets[sheet_name]
        
        # Check if file has enough rows (need at least row 286 for day 31)
        if len(df) < 287:  # Index 286 + 1
            print(f"Production file doesn't have enough rows (has {len(df)}, need at least 287)")
            return pd.DataFrame()
        
        # Check if file has enough columns (need at least column J = index 9)
        if len(df.columns) < 10:
            print(f"Production file doesn't have enough columns (has {len(df.columns)}, need at least 10)")
            return pd.DataFrame()
        
        # Get the year - assume 2025 or current year
        current_year = 2025  # Based on your folder structure "2025"
        
        production_data = []
        
        # Get number of days in the month
        days_in_month = monthrange(current_year, month)[1]
        
        print(f"Extracting production data for {days_in_month} days in month {month}/{current_year}")
        
        for day in range(1, days_in_month + 1):
            # Row mapping: Day 1 = Row 256 (index 255), Day 2 = Row 257 (index 256), etc.
            row_index = 255 + day - 1
            
            if row_index < len(df):
                prod_date = datetime(current_year, month, day)
                production_qty = df.iloc[row_index, 9]  # Column J (index 9)
                
                # Clean production quantity
                if pd.notna(production_qty):
                    try:
                        production_qty = float(production_qty)
                        if production_qty < 0:  # Handle negative values
                            production_qty = 0
                    except (ValueError, TypeError):
                        production_qty = 0
                else:
                    production_qty = 0
                
                production_data.append({
                    'Date': prod_date,
                    'Production_Qty': production_qty
                })
                
                # Debug print for first few days
                if day <= 3:
                    print(f"Day {day:02d} (row {row_index+1}): {production_qty}")
        
        result_df = pd.DataFrame(production_data)
        print(f"Extracted {len(result_df)} days of production data")
        print(f"Total production for month {month}: {result_df['Production_Qty'].sum():,.0f}")
        
        return result_df
        
    except Exception as e:
        print(f"Error extracting production data: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()

def merge_data_and_calculate_rates(visual_processed, production_data):
    """Merge visual inspection data with production data and calculate rates
    
    Logic:
    - Visual inspection data: grouped by Date + Item (multiple items per day possible)
    - Production data: grouped by Date only (total daily production)  
    - Hold rate = Hold Quantity / Production Quantity * 100
    """
    
    # Group visual inspection data by date and item
    visual_grouped = visual_processed.groupby(['Date', 'Item']).agg({
        'Hold_Qty': 'sum',
        'Defect_Qty': 'sum'
    }).reset_index()
    
    print(f"Visual data grouped: {len(visual_grouped)} date-item combinations")
    
    # Group production data by date (sum all production for the day) 
    production_grouped = production_data.groupby('Date')['Production_Qty'].sum().reset_index()
    print(f"Production data grouped: {len(production_grouped)} dates")
    
    # Merge the datasets on Date
    merged_data = visual_grouped.merge(
        production_grouped, 
        on='Date', 
        how='left'
    )
    
    print(f"After merge: {len(merged_data)} records")
    
    # Fill missing production quantities with 0
    merged_data['Production_Qty'] = merged_data['Production_Qty'].fillna(0)
    
    # Calculate hold rate: Hold Qty / Production Qty * 100
    merged_data['Hold_Rate'] = merged_data.apply(
        lambda row: (row['Hold_Qty'] / row['Production_Qty'] * 100) 
        if row['Production_Qty'] > 0 else 0, axis=1
    )
    
    # Round hold rate to 2 decimal places
    merged_data['Hold_Rate'] = merged_data['Hold_Rate'].round(2)
    
    # Format date for output (MM/DD/YYYY for Power BI compatibility)
    merged_data['Date_Formatted'] = merged_data['Date'].dt.strftime('%m/%d/%Y')
    
    # Rename columns for Vietnamese output
    final_df = merged_data.rename(columns={
        'Date_Formatted': 'Ngày',
        'Item': 'Item', 
        'Production_Qty': 'Sản lượng',
        'Hold_Qty': 'Số lượng hold',
        'Defect_Qty': 'Số lượng lỗi',
        'Hold_Rate': 'Tỉ lệ hold (%)'
    })
    
    # Select final columns in desired order
    final_columns = ['Ngày', 'Item', 'Sản lượng', 'Số lượng hold', 'Số lượng lỗi', 'Tỉ lệ hold (%)']
    result_df = final_df[final_columns]
    
    # Sort by date descending, then by item
    result_df = result_df.sort_values(['Ngày', 'Item'], ascending=[False, True])
    
    return result_df

def main():
    print("="*80)
    print("🔄 VISUAL INSPECTION + PRODUCTION DATA ANALYSIS")
    print("="*80)

    # Initialize SharePoint processor
    print("\n🔗 Initializing SharePoint/OneDrive connection...")
    try:
        sp_processor = SharePointProcessor()
        print("✅ SharePoint/OneDrive connection established")
    except Exception as e:
        print(f"❌ SharePoint/OneDrive initialization failed: {str(e)}")
        sys.exit(1)

    # 1. Download Visual Inspection data from SharePoint
    print("\n📥 Loading Visual Inspection data...")
    try:
        visual_sheets = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['visual_inspection'],
            "Visual Inspection data",
            source_type="sharepoint"
        )
        
        if not visual_sheets:
            print("❌ Failed to download Visual Inspection data")
            sys.exit(1)
        
        # Find the main data sheet
        visual_df = None
        for sheet_name, df in visual_sheets.items():
            if len(df) > 10:  # Assume main sheet has substantial data
                visual_df = df
                print(f"✅ Using sheet '{sheet_name}' with {len(df)} records")
                break
        
        if visual_df is None or visual_df.empty:
            print("❌ No valid Visual Inspection data found")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error loading Visual Inspection data: {str(e)}")
        sys.exit(1)

    # 2. Process Visual Inspection data
    print("\n🔄 Processing Visual Inspection data...")
    visual_processed = process_visual_inspection_data(visual_df)
    print(f"✅ Processed {len(visual_processed)} inspection records")
    
    if visual_processed.empty:
        print("❌ No valid inspection data after processing")
        sys.exit(1)

    # 3. Determine which months we need production data for
    unique_months = visual_processed['Date'].dt.month.unique()
    print(f"📅 Need production data for months: {sorted(unique_months)}")

    # 4. Find and download production files
    print("\n📥 Finding production files...")
    production_files = find_production_files_by_month(
        sp_processor, 
        unique_months
    )
    
    if not production_files:
        print("❌ No production files found")
        sys.exit(1)

    # 5. Download and process production data for each month
    print("\n📊 Processing production data...")
    all_production_data = []
    
    for month, file_id in production_files.items():
        print(f"📥 Processing month {month}...")
        try:
            production_sheets = sp_processor.download_excel_file_by_id(
                file_id,
                f"FS production data for month {month}",
                source_type="onedrive"
            )
            
            if production_sheets:
                month_production = extract_production_data(production_sheets, month)
                if not month_production.empty:
                    all_production_data.append(month_production)
                    print(f"✅ Extracted {len(month_production)} production records for month {month}")
                else:
                    print(f"⚠️ No production data extracted for month {month}")
            else:
                print(f"❌ Failed to download production file for month {month}")
                
        except Exception as e:
            print(f"❌ Error processing month {month}: {str(e)}")
    
    if not all_production_data:
        print("❌ No production data could be processed")
        sys.exit(1)

    # Combine all production data
    combined_production = pd.concat(all_production_data, ignore_index=True)
    print(f"✅ Combined production data: {len(combined_production)} records")

    # 6. Merge data and calculate rates
    print("\n🔄 Merging data and calculating rates...")
    final_report = merge_data_and_calculate_rates(visual_processed, combined_production)
    
    if final_report.empty:
        print("❌ No data could be merged")
        sys.exit(1)

    print(f"✅ Final report generated: {len(final_report)} records")
    print(f"📊 Summary statistics:")
    print(f"   - Total production: {final_report['Sản lượng'].sum():,.0f}")
    print(f"   - Total hold quantity: {final_report['Số lượng hold'].sum():,.0f}")
    print(f"   - Total defect quantity: {final_report['Số lượng lỗi'].sum():,.0f}")
    print(f"   - Average hold rate: {final_report['Tỉ lệ hold (%)'].mean():.2f}%")

    # 7. Upload results to SharePoint
    print("\n📤 Uploading results to SharePoint...")
    try:
        success = sp_processor.upload_excel_to_sharepoint(
            final_report,
            SHAREPOINT_FILE_IDS['fs_data_output'],
            'FS_Analysis'
        )
        
        if success:
            print("✅ Results successfully uploaded to SharePoint!")
        else:
            print("❌ Failed to upload results to SharePoint")
            
            # Save locally as backup
            print("💾 Saving results locally as backup...")
            backup_filename = f"FS_Analysis_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            final_report.to_excel(backup_filename, index=False)
            print(f"Results saved to {backup_filename}")
            
    except Exception as e:
        print(f"❌ Error uploading results: {str(e)}")
        sys.exit(1)

    print("\n" + "="*80)
    print("✅ VISUAL INSPECTION ANALYSIS COMPLETED SUCCESSFULLY!")
    print("✅ DATA SOURCES:")
    print("   - Visual Inspection: SharePoint")
    print("   - Production Data: OneDrive (multiple months)")
    print("✅ OUTPUT: SharePoint (FS data.xlsx)")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)
