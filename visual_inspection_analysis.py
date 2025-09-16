# BI-Optimized Production & Defect Analysis - Clean Version
# Minimal logging, separated tables, 2025 data only

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
except ImportError:
    MSAL_AVAILABLE = False

try:
    from nacl import encoding, public
    NACL_AVAILABLE = True
except ImportError:
    NACL_AVAILABLE = False

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
    'visual_inspection': '77FDDE39-0853-46EC-8BFB-0546460A3266',
    'fs_data_output': 'CDEBFC69-10BD-42F0-B777-3633405B072B',
    'september_production': '2E609D5D-6F45-4B7D-AB1C-C3B6FC3E2014'
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
            return response.status_code in [201, 204]
                
        except Exception as e:
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
            except Exception as e:
                self.msal_app = None
        
        if not self.authenticate():
            raise Exception("SharePoint authentication failed during initialization")

    def log(self, message):
        """Log with timestamp - minimal"""
        print(f"{message}")

    def authenticate(self):
        """Authenticate using delegation flow with pre-generated tokens"""
        try:
            self.log("🔐 Authenticating...")
            access_token = os.environ.get('SHAREPOINT_ACCESS_TOKEN')
            refresh_token = os.environ.get('SHAREPOINT_REFRESH_TOKEN')

            if not access_token and not refresh_token:
                return False

            self.access_token = access_token
            self.refresh_token = refresh_token
            
            if access_token:
                if self.test_token_validity():
                    self.log("✅ Authentication successful")
                    return True
                    
            if refresh_token and self.msal_app:
                if self.refresh_access_token():
                    self.update_github_secrets()
                    return True
                else:
                    return False
            else:
                return False

        except Exception as e:
            return False

    def test_token_validity(self):
        """Test if current access token is valid"""
        try:
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/me", headers=headers, timeout=30)
            return response.status_code == 200
        except Exception as e:
            return False

    def refresh_access_token(self):
        """Refresh access token using refresh token with MSAL"""
        try:
            if not self.refresh_token or not self.msal_app:
                return False
            result = self.msal_app.acquire_token_by_refresh_token(
                self.refresh_token,
                scopes=SHAREPOINT_CONFIG['scopes']
            )
            if result and "access_token" in result:
                self.access_token = result['access_token']
                if 'refresh_token' in result:
                    self.refresh_token = result['refresh_token']
                return True
            else:
                return False
        except Exception as e:
            return False

    def update_github_secrets(self):
        """Update GitHub Secrets with new tokens"""
        try:
            github_token = os.environ.get('GITHUB_TOKEN')
            if not github_token:
                return False
            
            repo = os.environ.get('GITHUB_REPOSITORY', '')
            if '/' not in repo:
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
            
            return success
            
        except Exception as e:
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
                return folder_id
            else:
                return None
                
        except Exception as e:
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
                return self.site_id
            else:
                return None
        except Exception as e:
            return None

    def download_excel_file_by_id(self, file_id, description="", source_type="sharepoint"):
        """Download Excel file from SharePoint or OneDrive by file ID"""
        try:
            self.log(f"📥 Downloading {description}...")
            if source_type == "onedrive":
                owner_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
                if owner_email:
                    url = f"{self.base_url}/users/{owner_email}/drive/items/{file_id}"
                else:
                    url = f"{self.base_url}/me/drive/items/{file_id}"
            else:
                site_id = self.get_site_id()
                if not site_id:
                    return None
                url = f"{self.base_url}/sites/{site_id}/drive/items/{file_id}"
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            if response.status_code == 200:
                file_info = response.json()
                download_url = file_info.get('@microsoft.graph.downloadUrl')
                if not download_url:
                    return None
                
                file_response = requests.get(download_url, timeout=60)
                if file_response.status_code == 200:
                    excel_data = io.BytesIO(file_response.content)
                    
                    try:
                        excel_file = pd.ExcelFile(excel_data)
                        sheets_data = {}
                        
                        for sheet_name in excel_file.sheet_names:
                            excel_data.seek(0)
                            df = pd.read_excel(excel_data, sheet_name=sheet_name)
                            sheets_data[sheet_name] = df
                        
                        return sheets_data
                    except Exception as e:
                        return None
                else:
                    return None
            else:
                return None
        except Exception as e:
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
                return []
        except Exception as e:
            return []

    def upload_multi_sheet_excel(self, sheets_dict, file_id):
        """Upload multi-sheet Excel file to SharePoint"""
        try:
            self.log(f"📤 Uploading to SharePoint...")
            excel_buffer = io.BytesIO()
            
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                for sheet_name, df in sheets_dict.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            excel_buffer.seek(0)
            excel_content = excel_buffer.getvalue()
            
            site_id = self.get_site_id()
            if not site_id:
                return False
                
            upload_url = f"{self.base_url}/sites/{site_id}/drive/items/{file_id}/content"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            
            response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)
            if response.status_code in [200, 201]:
                self.log(f"✅ Upload successful")
                return True
            else:
                self.log(f"❌ Upload failed: {response.status_code}")
                if "locked" in response.text.lower():
                    self.log("⚠️ File is locked - close file in SharePoint/Excel")
                return False
        except Exception as e:
            self.log(f"❌ Upload error: {str(e)}")
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

def process_visual_inspection_data_separated(visual_df):
    """Process visual inspection data for separated tables"""
    processed_data = []
    
    print("📋 Processing visual inspection data...")
    
    for _, row in visual_df.iterrows():
        item = row.get('Item', '')
        lot = row.get('Lot', '')
        retest = str(row.get('Retest', '')).upper()
        reject_qty = row.get('Reject qty', 0)
        defect_result = str(row.get('Defect result', '')).upper()
        
        # Get defect name
        defect_name = row.get('Defect name', '')
        if pd.isna(defect_name):
            defect_name = ''
        else:
            defect_name = str(defect_name).strip()
        
        # Get inspector info
        inspector = row.get('Inspector', '')
        if pd.isna(inspector):
            inspector = ''
        
        prod_date = parse_lot_to_date(lot)
        if not prod_date:
            continue
            
        # Filter out 2024 data - ONLY 2025
        if prod_date.year != 2025:
            continue
            
        hold_qty = 0
        defect_qty = 0
        
        # Process defects
        if defect_result == 'FAIL':
            if retest == 'YES':
                defect_qty = reject_qty
            elif retest == 'NO':
                hold_qty = reject_qty
        
        # ONLY add records with actual defects (hold > 0 OR defect > 0)
        if hold_qty > 0 or defect_qty > 0:
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
    print(f"✅ Processed {len(result_df)} defect records (2025 only)")
    
    return result_df

def find_production_files_enhanced(sp_processor, target_months):
    """Enhanced production file finder with September file ID"""
    production_files = {}
    user_email = SHAREPOINT_CONFIG.get('onedrive_user_email')
    
    # Add September file directly using provided ID
    if 9 in target_months:
        production_files[9] = SHAREPOINT_FILE_IDS['september_production']
        print(f"✅ Using September file")
    
    # Find other months using existing logic
    other_months = [m for m in target_months if m != 9]
    
    if other_months:
        try:
            # Try direct path first
            main_folder_path = SHAREPOINT_CONFIG['production_folder_path']
            main_folder_id = sp_processor.get_folder_id_by_path(main_folder_path, user_email=user_email)
            
            if not main_folder_id:
                # Search fallback
                search_url = f"{sp_processor.base_url}/users/{user_email}/drive/search(q='2025')"
                response = requests.get(search_url, headers=sp_processor.get_headers(), timeout=30)
                
                if response.status_code == 200:
                    search_results = response.json().get('value', [])
                    for item in search_results:
                        if (item.get('folder') and 
                            item.get('name') == '2025' and 
                            'BÁO CÁO THÁNG' in str(item.get('parentReference', {}).get('path', '')).upper()):
                            main_folder_id = item.get('id')
                            break
                    if not main_folder_id:
                        for item in search_results:
                            if item.get('folder') and item.get('name') == '2025':
                                main_folder_id = item.get('id')
                                break
            
            if main_folder_id:
                # List main folder contents
                main_contents = sp_processor.list_folder_contents(main_folder_id, "onedrive")
                
                month_folders = {}
                for item in main_contents:
                    if item.get('folder') and item.get('name'):
                        folder_name = item.get('name')
                        for month in other_months:
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
                                    break
                
                # Find BC FS files in each month folder
                for month, folder_id in month_folders.items():
                    folder_contents = sp_processor.list_folder_contents(folder_id, "onedrive")
                    
                    for item in folder_contents:
                        if not item.get('name'):
                            continue
                        item_name = item.get('name')
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
                                break
                        if month in production_files:
                            break
        
        except Exception as e:
            pass
    
    print(f"📊 Found production files for months: {list(production_files.keys())}")
    return production_files

def extract_production_data(production_sheets, month):
    """Extract production data with corrected date alignment"""
    try:
        sheet_name = 'OEE trừ DNP'
        if sheet_name not in production_sheets:
            return pd.DataFrame()
        
        df = production_sheets[sheet_name]
        
        if len(df) < 287 or len(df.columns) < 10:
            return pd.DataFrame()
        
        current_year = 2025
        production_data = []
        days_in_month = monthrange(current_year, month)[1]
        
        # FIXED: Corrected row mapping
        base_row = 254  # Start from row 255 (index 254) for day 1
        
        for day in range(1, days_in_month + 1):
            row_index = base_row + day  # 254 + 1 = 255 for day 1
            
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
                    'Row_Index': row_index + 1
                })
        
        result_df = pd.DataFrame(production_data)
        print(f"✅ Month {month}: {result_df['Production_Qty'].sum():,.0f} total units")
        
        return result_df
        
    except Exception as e:
        return pd.DataFrame()

def create_separated_tables(visual_processed, production_data):
    """Create separated Production and Defect tables for BI analysis"""
    
    print("📊 Creating separated tables...")
    
    # TABLE 1: Production Data (Daily totals only)
    production_daily = production_data.groupby('Date')['Production_Qty'].sum().reset_index()
    production_daily['Item'] = 'All Items'
    production_daily['Date_Formatted'] = production_daily['Date'].dt.strftime('%m/%d/%Y')
    
    production_table = production_daily.rename(columns={
        'Date_Formatted': 'Ngày',
        'Item': 'Item',
        'Production_Qty': 'Sản lượng'
    })[['Ngày', 'Item', 'Sản lượng']]
    
    production_table = production_table.sort_values('Ngày', ascending=False)
    
    # TABLE 2: Defect Data (Only records with actual defects)
    defect_records = []
    
    for _, row in visual_processed.iterrows():
        # Only include records with actual issues
        if row['Hold_Qty'] > 0 or row['Defect_Qty'] > 0:
            # Calculate defect rate based on daily production
            daily_prod = production_daily[production_daily['Date'] == row['Date']]
            if not daily_prod.empty:
                daily_production = daily_prod['Production_Qty'].iloc[0]
                hold_rate = (row['Hold_Qty'] / daily_production * 100) if daily_production > 0 else 0
            else:
                daily_production = 0
                hold_rate = 0
            
            defect_records.append({
                'Date': row['Date'],
                'Item': row['Item'],
                'Defect_Name': row['Defect_Name'],
                'Lot': row['Lot'],
                'Hold_Qty': row['Hold_Qty'],
                'Defect_Qty': row['Defect_Qty'],
                'Hold_Rate': round(hold_rate, 2),
                'Inspector': row['Inspector'],
                'Daily_Production': daily_production
            })
    
    defect_df = pd.DataFrame(defect_records)
    
    if not defect_df.empty:
        defect_df['Date_Formatted'] = defect_df['Date'].dt.strftime('%m/%d/%Y')
        
        defect_table = defect_df.rename(columns={
            'Date_Formatted': 'Ngày',
            'Item': 'Item',
            'Defect_Name': 'Tên lỗi',
            'Lot': 'Lot',
            'Hold_Qty': 'Số lượng hold',
            'Defect_Qty': 'Số lượng lỗi',
            'Hold_Rate': 'Tỉ lệ hold (%)',
            'Inspector': 'Người kiểm tra'
        })[['Ngày', 'Item', 'Tên lỗi', 'Lot', 'Số lượng hold', 'Số lượng lỗi', 'Tỉ lệ hold (%)', 'Người kiểm tra']]
        
        defect_table = defect_table.sort_values(['Ngày', 'Tên lỗi'], ascending=[False, True])
    else:
        # Create empty defect table with proper columns
        defect_table = pd.DataFrame(columns=[
            'Ngày', 'Item', 'Tên lỗi', 'Lot', 'Số lượng hold', 'Số lượng lỗi', 'Tỉ lệ hold (%)', 'Người kiểm tra'
        ])
    
    print(f"✅ Production table: {len(production_table)} daily records")
    print(f"✅ Defect table: {len(defect_table)} defect records")
    
    return production_table, defect_table

def generate_summary_analytics(production_table, defect_table):
    """Generate summary analytics for management"""
    
    analytics = {}
    
    # Production summary
    total_production = production_table['Sản lượng'].sum()
    production_days = len(production_table[production_table['Sản lượng'] > 0])
    avg_daily_production = production_table[production_table['Sản lượng'] > 0]['Sản lượng'].mean()
    
    analytics['production_summary'] = pd.DataFrame([{
        'Metric': 'Total Production',
        'Value': f"{total_production:,.0f}",
        'Unit': 'units'
    }, {
        'Metric': 'Production Days',
        'Value': f"{production_days}",
        'Unit': 'days'
    }, {
        'Metric': 'Average Daily Production',
        'Value': f"{avg_daily_production:,.0f}",
        'Unit': 'units/day'
    }])
    
    # Defect summary
    if not defect_table.empty:
        total_hold = defect_table['Số lượng hold'].sum()
        total_defects = defect_table['Số lượng lỗi'].sum()
        overall_hold_rate = (total_hold / total_production * 100) if total_production > 0 else 0
        unique_defect_types = len(defect_table['Tên lỗi'].unique())
        
        analytics['defect_summary'] = pd.DataFrame([{
            'Metric': 'Total Hold Quantity',
            'Value': f"{total_hold:,.0f}",
            'Unit': 'units'
        }, {
            'Metric': 'Total Defect Quantity',
            'Value': f"{total_defects:,.0f}",
            'Unit': 'units'
        }, {
            'Metric': 'Overall Hold Rate',
            'Value': f"{overall_hold_rate:.2f}",
            'Unit': '%'
        }, {
            'Metric': 'Unique Defect Types',
            'Value': f"{unique_defect_types}",
            'Unit': 'types'
        }])
        
        # Top defects analysis
        defect_analysis = defect_table.groupby('Tên lỗi').agg({
            'Số lượng hold': 'sum',
            'Số lượng lỗi': 'sum'
        }).reset_index()
        defect_analysis['Total_Issues'] = defect_analysis['Số lượng hold'] + defect_analysis['Số lượng lỗi']
        defect_analysis['Percentage'] = (defect_analysis['Total_Issues'] / (total_hold + total_defects) * 100).round(2)
        analytics['top_defects'] = defect_analysis.sort_values('Total_Issues', ascending=False).head(10)
        
    else:
        analytics['defect_summary'] = pd.DataFrame([{
            'Metric': 'No defects recorded',
            'Value': '0',
            'Unit': '-'
        }])
        analytics['top_defects'] = pd.DataFrame()
    
    return analytics

def main():
    print("=" * 60)
    print("🎯 BI-OPTIMIZED PRODUCTION & DEFECT ANALYSIS")
    print("📊 SEPARATED TABLES | NO DOUBLE-COUNTING")
    print("=" * 60)

    try:
        sp_processor = SharePointProcessor()
        
        # Load Visual Inspection data
        visual_sheets = sp_processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['visual_inspection'],
            "Visual Inspection data",
            source_type="sharepoint"
        )
        
        if not visual_sheets:
            print("❌ Failed to download Visual Inspection data")
            return
        
        visual_df = None
        for sheet_name, df in visual_sheets.items():
            if len(df) > 10:
                visual_df = df
                break
        
        if visual_df is None or visual_df.empty:
            print("❌ No valid Visual Inspection data found")
            return
        
        # Process visual inspection data
        visual_processed = process_visual_inspection_data_separated(visual_df)
        
        if visual_processed.empty:
            print("⚠️ No defect data found for 2025")
        
        # Get unique months
        unique_months = []
        if not visual_processed.empty:
            unique_months = visual_processed['Date'].dt.month.unique().tolist()
        
        # Add current month (September)
        current_month = 9
        if current_month not in unique_months:
            unique_months.append(current_month)
        
        unique_months = [int(month) for month in unique_months]
        print(f"📅 Processing months: {sorted(unique_months)}")
        
        # Find and process production files
        production_files = find_production_files_enhanced(sp_processor, unique_months)
        
        all_production_data = []
        if production_files:
            for month, file_id in production_files.items():
                production_sheets = sp_processor.download_excel_file_by_id(
                    file_id,
                    f"Month {month}",
                    source_type="onedrive"
                )
                
                if production_sheets:
                    month_production = extract_production_data(production_sheets, month)
                    if not month_production.empty:
                        all_production_data.append(month_production)
        
        if not all_production_data:
            print("❌ No production data found")
            return
        
        combined_production = pd.concat(all_production_data, ignore_index=True)
        
        # Create separated tables
        production_table, defect_table = create_separated_tables(visual_processed, combined_production)
        
        # Generate analytics
        analytics = generate_summary_analytics(production_table, defect_table)
        
        # Prepare sheets for upload
        sheets_to_upload = {
            'Production_Data': production_table,
            'Defect_Data': defect_table,
            'Production_Summary': analytics['production_summary'],
            'Defect_Summary': analytics['defect_summary']
        }
        
        if not analytics['top_defects'].empty:
            sheets_to_upload['Top_Defects'] = analytics['top_defects']
        
        # Summary
        print(f"\n📊 FINAL SUMMARY:")
        print(f"   📈 Production records: {len(production_table)}")
        print(f"   🔍 Defect records: {len(defect_table)}")
        print(f"   🏭 Total production: {production_table['Sản lượng'].sum():,.0f} units")
        if not defect_table.empty:
            print(f"   ⚠️ Total holds: {defect_table['Số lượng hold'].sum():,.0f} units")
            print(f"   🎯 Defect types: {len(defect_table['Tên lỗi'].unique())}")
        
        # Upload to SharePoint
        success = sp_processor.upload_multi_sheet_excel(
            sheets_to_upload,
            SHAREPOINT_FILE_IDS['fs_data_output']
        )
        
        # Always create backup
        backup_filename = f"BI_Analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        
        try:
            with pd.ExcelWriter(backup_filename, engine='openpyxl') as writer:
                for sheet_name, df in sheets_to_upload.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"💾 Backup saved: {backup_filename}")
        except Exception as e:
            print(f"⚠️ Backup error: {str(e)}")
            
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")

    print("\n" + "=" * 60)
    print("✅ ANALYSIS COMPLETED!")
    print("📊 Production table: Daily totals (no duplication)")
    print("🔍 Defect table: Individual defects only")
    print("📋 Ready for BI dashboards")
    print("=" * 60)

if __name__ == "__main__":
    main()
