"""
SharePoint QA Data Processing - Delegation Flow Version
Xử lý dữ liệu QA từ SharePoint sử dụng delegation flow (không cần CLIENT_SECRET)
Phiên bản delegation của Visual_SharePoint.py
"""

import pandas as pd
import os
import sys
import io
import requests
from datetime import datetime, timedelta
import msal
import time
from config_delegation import GRAPH_API_CONFIG, SHAREPOINT_CONFIG, FILE_PATHS, OUTPUT_CONFIG, QA_CONFIG, SHAREPOINT_FILE_IDS, TOKEN_CONFIG

class SharePointDelegationProcessor:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.site_id = None
        self.processed_data = {}
        self.msal_app = None
        self.authenticate()
    
    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
    
    def authenticate(self):
        """Authenticate using delegation flow with pre-generated tokens"""
        try:
            self.log("🔐 Authenticating with delegation flow...")
            
            # Get tokens from environment variables
            access_token = GRAPH_API_CONFIG.get('access_token')
            refresh_token = GRAPH_API_CONFIG.get('refresh_token')
            
            if not access_token:
                self.log("❌ No access token found in environment variables")
                self.log("💡 Please run generate_tokens.py locally and add tokens to GitHub Secrets")
                return False
            
            self.access_token = access_token
            self.refresh_token = refresh_token
            
            # Test token validity
            if self.test_token_validity():
                self.log("✅ Access token is valid")
                return True
            else:
                self.log("⚠️ Access token expired, attempting refresh...")
                if self.refresh_access_token():
                    self.log("✅ Token refreshed successfully")
                    return True
                else:
                    self.log("❌ Token refresh failed")
                    return False
                    
        except Exception as e:
            self.log(f"❌ Authentication error: {str(e)}")
            return False
    
    def test_token_validity(self):
        """Test if current access token is valid"""
        try:
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/me", headers=headers)
            
            if response.status_code == 200:
                return True
            elif response.status_code == 401:
                return False
            else:
                self.log(f"Warning: Unexpected response code during token test: {response.status_code}")
                return False
                
        except Exception as e:
            self.log(f"Error testing token validity: {str(e)}")
            return False
    
    def refresh_access_token(self):
        """Refresh access token using refresh token"""
        try:
            if not self.refresh_token:
                self.log("❌ No refresh token available")
                return False
            
            # Create MSAL app if not exists
            if not self.msal_app:
                self.msal_app = msal.PublicClientApplication(
                    GRAPH_API_CONFIG['client_id'],
                    authority=GRAPH_API_CONFIG['authority']
                )
            
            # Try to refresh token
            accounts = self.msal_app.get_accounts()
            
            if accounts:
                result = self.msal_app.acquire_token_silent(
                    GRAPH_API_CONFIG['scopes'], 
                    account=accounts[0]
                )
                
                if result and "access_token" in result:
                    self.access_token = result['access_token']
                    if 'refresh_token' in result:
                        self.refresh_token = result['refresh_token']
                    return True
            
            self.log("❌ Unable to refresh token automatically")
            return False
            
        except Exception as e:
            self.log(f"❌ Error refreshing token: {str(e)}")
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
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                site_data = response.json()
                self.site_id = site_data['id']
                self.log(f"✅ Found site ID: {self.site_id}")
                return self.site_id
            elif response.status_code == 401:
                # Token might be expired, try refresh
                if self.refresh_access_token():
                    return self.get_site_id()  # Retry
                else:
                    self.log("❌ Authentication failed and token refresh unsuccessful")
                    return None
            else:
                self.log(f"❌ Error getting site ID: {response.status_code}")
                self.log(f"Response text: {response.text[:200]}")
                return None
                
        except Exception as e:
            self.log(f"❌ Error getting site ID: {str(e)}")
            return None
    
    def download_excel_file_by_id(self, file_id, description=""):
        """Download Excel file từ SharePoint bằng file ID với retry logic"""
        max_retries = TOKEN_CONFIG['max_retry_attempts']
        retry_delay = TOKEN_CONFIG['retry_delay']
        
        for attempt in range(max_retries):
            try:
                self.log(f"📥 Downloading {description}... (Attempt {attempt + 1}/{max_retries})")
                
                # Get file download URL using file ID
                url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}"
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 401 and attempt < max_retries - 1:
                    # Token expired, try refresh
                    self.log("🔄 Token expired, refreshing...")
                    if self.refresh_access_token():
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.log("❌ Token refresh failed")
                        return None
                
                if response.status_code == 200:
                    file_info = response.json()
                    download_url = file_info.get('@microsoft.graph.downloadUrl')
                    
                    if download_url:
                        # Download file content
                        file_response = requests.get(download_url)
                        
                        if file_response.status_code == 200:
                            # Read Excel từ memory
                            excel_data = io.BytesIO(file_response.content)
                            
                            # Read all sheets
                            excel_file = pd.ExcelFile(excel_data)
                            sheets_data = {}
                            
                            for sheet_name in excel_file.sheet_names:
                                # Reset position for each sheet
                                excel_data.seek(0)
                                df = pd.read_excel(excel_data, sheet_name=sheet_name)
                                sheets_data[sheet_name] = df
                                self.log(f"✅ Sheet '{sheet_name}': {len(df)} rows")
                            
                            self.log(f"✅ Successfully downloaded {description}")
                            return sheets_data
                        else:
                            self.log(f"❌ Error downloading file content: {file_response.status_code}")
                    else:
                        self.log(f"❌ No download URL found for {description}")
                else:
                    self.log(f"❌ Error getting file info: {response.status_code}")
                
                # If we reach here and it's not the last attempt, wait and retry
                if attempt < max_retries - 1:
                    self.log(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                
            except Exception as e:
                self.log(f"❌ Error downloading {description}: {str(e)}")
                if attempt < max_retries - 1:
                    self.log(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        return None
    
    def upload_excel_to_sharepoint(self, df, file_id, sheet_name="Processed_Data"):
        """Upload processed data to SharePoint Excel file với retry logic"""
        max_retries = TOKEN_CONFIG['max_retry_attempts']
        retry_delay = TOKEN_CONFIG['retry_delay']
        
        for attempt in range(max_retries):
            try:
                self.log(f"📤 Uploading data to SharePoint... (Attempt {attempt + 1}/{max_retries})")
                
                # Create Excel file in memory
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                excel_buffer.seek(0)
                excel_content = excel_buffer.getvalue()
                
                # Upload to SharePoint
                upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}/content"
                
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }
                
                response = requests.put(upload_url, headers=headers, data=excel_content)
                
                if response.status_code == 401 and attempt < max_retries - 1:
                    # Token expired, try refresh
                    self.log("🔄 Token expired during upload, refreshing...")
                    if self.refresh_access_token():
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.log("❌ Token refresh failed")
                        return False
                
                if response.status_code in [200, 201]:
                    self.log(f"✅ Successfully uploaded {len(df)} rows to SharePoint")
                    return True
                else:
                    self.log(f"❌ Error uploading to SharePoint: {response.status_code}")
                    self.log(f"Response: {response.text}")
                
                # If we reach here and it's not the last attempt, wait and retry
                if attempt < max_retries - 1:
                    self.log(f"⏳ Retrying upload in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                
            except Exception as e:
                self.log(f"❌ Error uploading to SharePoint: {str(e)}")
                if attempt < max_retries - 1:
                    self.log(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        return False

# Import all the processing functions from the original Visual.py
def parse_mdg_values(mdg_str):
    """Parse MĐG values that can be single values or comma-separated values like '1,2' or '3,4'"""
    if pd.isna(mdg_str) or mdg_str is None:
        return []
    
    try:
        mdg_str = str(mdg_str).strip()
        
        if ',' in mdg_str:
            mdg_values = []
            for value in mdg_str.split(','):
                try:
                    mdg_val = int(float(value.strip()))
                    mdg_values.append(mdg_val)
                except (ValueError, TypeError):
                    continue
            return mdg_values
        else:
            mdg_val = int(float(mdg_str))
            return [mdg_val]
    except (ValueError, TypeError):
        return []

def standardize_date(date_str):
    """Convert date string to datetime object"""
    try:
        if isinstance(date_str, str):
            if '/' in date_str:
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y', dayfirst=True)
                except:
                    pass
            return pd.to_datetime(date_str, dayfirst=True)
        return pd.to_datetime(date_str)
    except:
        return None

def parse_hour(hour_str):
    """Extract hour from hour string"""
    if pd.isna(hour_str) or not isinstance(hour_str, str):
        return None
    
    hour_str = hour_str.lower().strip()
    
    if 'h' in hour_str:
        try:
            hour_part = hour_str.split('h')[0]
            return int(hour_part)
        except:
            pass
    
    if ':' in hour_str:
        try:
            hour_part = hour_str.split(':')[0]
            return int(hour_part)
        except:
            pass
    
    try:
        return int(hour_str)
    except:
        return None

def determine_shift(hour):
    """Determine shift (Ca) based on hour"""
    if hour is None:
        return None
    
    if 6 <= hour < 14:
        return 1
    elif 14 <= hour < 22:
        return 2
    else:
        return 3

def get_target_tv(line):
    """Determine Target TV based on Line number"""
    if pd.isna(line):
        return None
    
    try:
        line_num = float(line)
        if 1 <= line_num <= 6:
            return QA_CONFIG['target_tv']['line_1_6']
        elif 7 <= line_num <= 8:
            return QA_CONFIG['target_tv']['line_7_8']
        else:
            return None
    except (ValueError, TypeError):
        return None

def create_mapping_key_with_hour_logic(row, sample_id_df):
    """Create a mapping key considering extended shift logic and MĐG grouping"""
    try:
        date_std = standardize_date(row['Ngày SX'])
        if date_std is None:
            return None
        
        date_key = date_std.strftime('%d/%m/%Y')
        hour = parse_hour(row.get('Giờ', ''))
        if hour is None:
            return None
            
        line = int(float(row['Line'])) if pd.notna(row['Line']) else None
        mdg_values = parse_mdg_values(row.get('MĐG', ''))
        if not mdg_values or line is None:
            return None
        
        # Determine possible shift codes based on hour
        possible_shift_codes = []
        
        if 6 <= hour < 14:
            possible_shift_codes = [1, 14]
        elif 14 <= hour < 18:
            possible_shift_codes = [2, 14]
        elif 18 <= hour < 22:
            possible_shift_codes = [2, 34]
        elif 22 <= hour <= 23:
            possible_shift_codes = [3, 34]
        elif 0 <= hour < 6:
            possible_shift_codes = [3, 34]
        else:
            ca = determine_shift(hour)
            if ca:
                possible_shift_codes = [ca]
        
        # Handle multiple MĐG values
        all_lookup_mdg_values = set()
        for mdg in mdg_values:
            if mdg == 2:
                mdg_lookup_values = [1, 2]
            elif mdg == 4:
                mdg_lookup_values = [3, 4]
            else:
                mdg_lookup_values = [mdg]
            
            all_lookup_mdg_values.update(mdg_lookup_values)
        
        # Try to find a match
        for shift_code in possible_shift_codes:
            for lookup_mdg in all_lookup_mdg_values:
                try:
                    matching_records = sample_id_df[
                        (sample_id_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_key) &
                        (sample_id_df['Ca'].astype(str).str.strip() == str(shift_code)) &
                        (sample_id_df['Line'].astype(str).str.strip() == str(line)) &
                        (sample_id_df['MĐG'].astype(str).str.strip() == str(lookup_mdg))
                    ]
                    
                    if not matching_records.empty:
                        return (date_key, shift_code, line, mdg_values[0])
                except Exception as e:
                    continue
        
        return None
        
    except (ValueError, TypeError, KeyError):
        return None

def create_simple_mapping_key(row):
    """Create mapping keys for sample_id_df records"""
    try:
        date_std = standardize_date(row['Ngày SX'])
        if date_std is None:
            return []
        
        date_key = date_std.strftime('%d/%m/%Y')
        ca = int(float(row['Ca'])) if pd.notna(row['Ca']) else None
        line = int(float(row['Line'])) if pd.notna(row['Line']) else None
        mdg_values = parse_mdg_values(row.get('MĐG', ''))
        
        if not mdg_values or ca is None or line is None:
            return []
        
        keys = []
        for mdg in mdg_values:
            if mdg == 1:
                keys.append((date_key, ca, line, 1))
                keys.append((date_key, ca, line, 2))
            elif mdg == 3:
                keys.append((date_key, ca, line, 3))
                keys.append((date_key, ca, line, 4))
            else:
                keys.append((date_key, ca, line, mdg))
        
        return [key for key in keys if isinstance(key, tuple) and len(key) == 4]
        
    except (ValueError, TypeError, KeyError) as e:
        print(f"Warning: Error in create_simple_mapping_key: {e}")
        return []

def expand_dataframe_for_multiple_mdg(df):
    """Expand dataframe rows that have comma-separated MĐG values"""
    expanded_rows = []
    
    for _, row in df.iterrows():
        mdg_values = parse_mdg_values(row.get('MĐG', ''))
        
        if len(mdg_values) <= 1:
            expanded_rows.append(row)
        else:
            for mdg_val in mdg_values:
                new_row = row.copy()
                new_row['MĐG'] = mdg_val
                new_row['MĐG_Original'] = row['MĐG']
                expanded_rows.append(new_row)
    
    return pd.DataFrame(expanded_rows)

def find_representative_production_data(vhm_name, sample_id_df, existing_aql_df):
    """Find representative production data for a given VHM"""
    try:
        vhm_sample_records = sample_id_df[sample_id_df['VHM'] == vhm_name]
        
        if vhm_sample_records.empty:
            return None, None
        
        sample_row = vhm_sample_records.iloc[0]
        
        sample_date = standardize_date(sample_row.get('Ngày SX', ''))
        sample_ca = sample_row.get('Ca', '')
        sample_line = sample_row.get('Line', '')
        sample_mdg = sample_row.get('MĐG', '')
        
        if sample_date is None:
            return sample_row, None
            
        date_str = sample_date.strftime('%d/%m/%Y')
        ca_str = str(sample_ca).strip()
        line_str = str(sample_line).strip()
        mdg_str = str(sample_mdg).strip()
        
        # Priority 1: Exact match
        matching_records = existing_aql_df[
            (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
            (existing_aql_df['Ca'].astype(str).str.strip() == ca_str) &
            (existing_aql_df['Line'].astype(str).str.strip() == line_str) &
            (existing_aql_df['MĐG'].astype(str).str.strip() == mdg_str)
        ]
        
        # Priority 2: MĐG grouping logic
        if matching_records.empty:
            try:
                mdg_val = int(float(sample_mdg))
                if mdg_val == 2:
                    matching_records = existing_aql_df[
                        (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
                        (existing_aql_df['Ca'].astype(str).str.strip() == ca_str) &
                        (existing_aql_df['Line'].astype(str).str.strip() == line_str) &
                        (existing_aql_df['MĐG'].astype(str).str.strip() == '1')
                    ]
                elif mdg_val == 4:
                    matching_records = existing_aql_df[
                        (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
                        (existing_aql_df['Ca'].astype(str).str.strip() == ca_str) &
                        (existing_aql_df['Line'].astype(str).str.strip() == line_str) &
                        (existing_aql_df['MĐG'].astype(str).str.strip() == '3')
                    ]
            except:
                pass
        
        # Priority 3: Same date and shift
        if matching_records.empty:
            matching_records = existing_aql_df[
                (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
                (existing_aql_df['Ca'].astype(str).str.strip() == ca_str)
            ]
        
        # Priority 4: Same date
        if matching_records.empty:
            matching_records = existing_aql_df[
                existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str
            ]
        
        # Priority 5: Same line
        if matching_records.empty:
            matching_records = existing_aql_df[
                existing_aql_df['Line'].astype(str).str.strip() == line_str
            ]
        
        production_data = matching_records.iloc[0] if not matching_records.empty else None
        return sample_row, production_data
            
    except Exception as e:
        print(f"Error finding representative production data for VHM {vhm_name}: {e}")
        return None, None

def main():
    print("="*60)
    print("🏭 MASAN QA DATA PROCESSING - SHAREPOINT DELEGATION FLOW")
    print("="*60)
    
    # Check if we have access token
    if not GRAPH_API_CONFIG.get('access_token'):
        print("❌ No SHAREPOINT_ACCESS_TOKEN found in environment")
        print("💡 Please run generate_tokens.py locally and add tokens to GitHub Secrets:")
        print("   1. SHAREPOINT_ACCESS_TOKEN")
        print("   2. SHAREPOINT_REFRESH_TOKEN (optional but recommended)")
        sys.exit(1)
    
    # Initialize processor
    processor = SharePointDelegationProcessor()
    
    if not processor.access_token:
        print("❌ Failed to authenticate with SharePoint")
        sys.exit(1)
    
    try:
        # Download Sample ID file
        processor.log("📥 Downloading Sample ID file...")
        sample_id_data = processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['sample_id'], 
            "Sample ID file"
        )
        
        if not sample_id_data:
            processor.log("❌ Failed to download Sample ID file")
            sys.exit(1)
        
        # Get the first sheet from Sample ID
        sample_id_df = list(sample_id_data.values())[0]
        processor.log(f"✅ Sample ID data: {len(sample_id_df)} rows")
        
        # Download Data SX file
        processor.log("📥 Downloading Data SX file...")
        data_sx_data = processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['data_sx'], 
            "Data SX file"
        )
        
        if not data_sx_data:
            processor.log("❌ Failed to download Data SX file")
            sys.exit(1)
        
        # Extract individual sheets
        id_aql_df = data_sx_data.get('ID AQL', pd.DataFrame())
        aql_goi_df = data_sx_data.get('AQL gói', pd.DataFrame())
        aql_to_ly_df = data_sx_data.get('AQL Tô ly', pd.DataFrame())
        
        processor.log(f"✅ ID AQL data: {len(id_aql_df)} rows")
        processor.log(f"✅ AQL gói data: {len(aql_goi_df)} rows")
        processor.log(f"✅ AQL Tô ly data: {len(aql_to_ly_df)} rows")
        
    except Exception as e:
        processor.log(f"❌ Error downloading files: {str(e)}")
        sys.exit(1)
    
    # Process data using the same logic as original Visual.py
    processor.log("🔄 Starting data processing...")
    
    # Check required columns
    required_columns_check = {
        'ID AQL': ['Line', 'Defect code', 'Ngày SX', 'Giờ', 'MĐG'],
        'AQL gói': ['Defect code', 'Defect name'],
        'AQL Tô ly': ['Defect code', 'Defect name'],
        'Sample ID': ['Ngày SX', 'Ca', 'Line', 'MĐG', 'VHM', '% Hao hụt OPP']
    }
    
    dataframes = {
        'ID AQL': id_aql_df,
        'AQL gói': aql_goi_df,
        'AQL Tô ly': aql_to_ly_df,
        'Sample ID': sample_id_df
    }
    
    for sheet_name, required_cols in required_columns_check.items():
        df = dataframes[sheet_name]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            processor.log(f"Warning: Missing columns in {sheet_name}: {missing_cols}")
    
    # Convert and standardize data
    if 'Line' in id_aql_df.columns:
        id_aql_df['Line'] = pd.to_numeric(id_aql_df['Line'], errors='coerce')
    
    # Expand MĐG values
    processor.log(f"Original rows before MĐG expansion: {len(id_aql_df)}")
    id_aql_df = expand_dataframe_for_multiple_mdg(id_aql_df)
    processor.log(f"Rows after MĐG expansion: {len(id_aql_df)}")
    
    # Standardize defect codes
    for df_name, df in [('ID AQL', id_aql_df), ('AQL gói', aql_goi_df), ('AQL Tô ly', aql_to_ly_df)]:
        if 'Defect code' in df.columns:
            df['Defect code'] = df['Defect code'].astype(str).str.strip()
    
    # Standardize dates and extract date components
    if 'Ngày SX' in id_aql_df.columns:
        id_aql_df['Ngày SX_std'] = id_aql_df['Ngày SX'].apply(standardize_date)
        id_aql_df['Ngày'] = id_aql_df['Ngày SX_std'].apply(lambda x: x.day if x else None)
        id_aql_df['Tuần'] = id_aql_df['Ngày SX_std'].apply(lambda x: x.isocalendar()[1] if x else None)
        id_aql_df['Tháng'] = id_aql_df['Ngày SX_std'].apply(lambda x: x.month if x else None)
    
    # Extract hour and determine shift
    if 'Giờ' in id_aql_df.columns:
        id_aql_df['hour'] = id_aql_df['Giờ'].apply(parse_hour)
        id_aql_df['Ca'] = id_aql_df['hour'].apply(determine_shift)
    
    # Add Target TV
    id_aql_df['Target TV'] = id_aql_df['Line'].apply(get_target_tv)
    
    # Create defect name mapping
    goi_defect_map = {}
    to_ly_defect_map = {}
    
    if 'Defect code' in aql_goi_df.columns and 'Defect name' in aql_goi_df.columns:
        goi_defect_map = dict(zip(aql_goi_df['Defect code'], aql_goi_df['Defect name']))
    
    if 'Defect code' in aql_to_ly_df.columns and 'Defect name' in aql_to_ly_df.columns:
        to_ly_defect_map = dict(zip(aql_to_ly_df['Defect code'], aql_to_ly_df['Defect name']))
    
    def map_defect_name(row):
        if pd.isna(row.get('Line')) or pd.isna(row.get('Defect code')):
            return None
        
        try:
            line = float(row['Line'])
            defect_code = str(row['Defect code']).strip()
            
            if 1 <= line <= 6:
                return goi_defect_map.get(defect_code, None)
            elif 7 <= line <= 8:
                return to_ly_defect_map.get(defect_code, None)
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    id_aql_df['Defect name'] = id_aql_df.apply(map_defect_name, axis=1)
    
    # Create VHM and % Hao hụt OPP mapping
    processor.log("Creating VHM and % Hao hụt OPP mapping...")
    vhm_mapping = {}
    hao_hut_mapping = {}
    
    for _, row in sample_id_df.iterrows():
        keys = create_simple_mapping_key(row)
        vhm_value = row.get('VHM', '')
        hao_hut_value = row.get('% Hao hụt OPP', '')
        
        for key in keys:
            if isinstance(key, tuple) and len(key) == 4:
                vhm_mapping[key] = vhm_value
                hao_hut_mapping[key] = hao_hut_value
    
    processor.log(f"Created {len(vhm_mapping)} mapping entries")
    
    # Apply VHM mapping
    def get_vhm(row):
        key = create_mapping_key_with_hour_logic(row, sample_id_df)
        return vhm_mapping.get(key, '') if key else ''
    
    def get_hao_hut_opp(row):
        key = create_mapping_key_with_hour_logic(row, sample_id_df)
        return hao_hut_mapping.get(key, '') if key else ''
    
    id_aql_df['VHM'] = id_aql_df.apply(get_vhm, axis=1)
    id_aql_df['% Hao hụt OPP'] = id_aql_df.apply(get_hao_hut_opp, axis=1)
    
    vhm_mapped_count = (id_aql_df['VHM'] != '').sum()
    processor.log(f"Successfully mapped VHM for {vhm_mapped_count} out of {len(id_aql_df)} records")
    
    # Create output dataframe
    required_output_columns = [
        'Ngày SX', 'Ngày', 'Tuần', 'Tháng', 'Sản phẩm', 'Item', 'Giờ', 'Ca', 'Line', 'MĐG', 
        'SL gói lỗi sau xử lý', 'Defect code', 'Defect name', 'Số lượng hold ( gói/thùng)',
        'Target TV', 'VHM', '% Hao hụt OPP', 'QA', 'Tên Trưởng ca'
    ]
    
    if 'MĐG_Original' in id_aql_df.columns:
        required_output_columns.append('MĐG_Original')
    
    # Ensure all columns exist
    for col in required_output_columns:
        if col not in id_aql_df.columns:
            id_aql_df[col] = ''
    
    available_columns = [col for col in required_output_columns if col in id_aql_df.columns]
    existing_aql_df = id_aql_df[available_columns].copy()
    
    # Create comprehensive dataset
    processor.log("Creating comprehensive dataset...")
    
    # Convert hold quantity to numeric
    existing_aql_df['Số lượng hold ( gói/thùng)_numeric'] = pd.to_numeric(
        existing_aql_df['Số lượng hold ( gói/thùng)'], errors='coerce'
    )
    
    # Get defect records
    defect_records = existing_aql_df[
        existing_aql_df['Số lượng hold ( gói/thùng)_numeric'] > 0
    ].copy().drop(columns=['Số lượng hold ( gói/thùng)_numeric'])
    
    processor.log(f"Found {len(defect_records)} records with defects")
    
    # Create comprehensive dataset
    comprehensive_rows = []
    
    # Add all defect records
    for _, defect_row in defect_records.iterrows():
        comprehensive_rows.append(defect_row)
    
    # Add zero-defect records for VHMs without defects
    defect_records_with_vhm = defect_records[
        (defect_records['VHM'] != '') & (defect_records['VHM'].notna())
    ]
    
    vhms_with_defects = set(defect_records_with_vhm['VHM'].unique())
    all_vhms_from_sample = set(sample_id_df['VHM'].dropna().unique())
    vhms_without_defects = all_vhms_from_sample - vhms_with_defects
    
    processor.log(f"Creating zero-defect records for {len(vhms_without_defects)} VHMs")
    
    for vhm_name in vhms_without_defects:
        try:
            sample_data, production_data = find_representative_production_data(
                vhm_name, sample_id_df, existing_aql_df
            )
            
            if sample_data is None:
                continue
            
            # Create zero-defect record
            zero_defect_record = {}
            
            sample_date = standardize_date(sample_data.get('Ngày SX', ''))
            zero_defect_record['Ngày SX'] = sample_data.get('Ngày SX', '')
            zero_defect_record['Ngày'] = sample_date.day if sample_date else ''
            zero_defect_record['Tuần'] = sample_date.isocalendar()[1] if sample_date else ''
            zero_defect_record['Tháng'] = sample_date.month if sample_date else ''
            zero_defect_record['Ca'] = sample_data.get('Ca', '')
            zero_defect_record['Line'] = sample_data.get('Line', '')
            zero_defect_record['MĐG'] = sample_data.get('MĐG', '')
            zero_defect_record['VHM'] = sample_data.get('VHM', '')
            zero_defect_record['% Hao hụt OPP'] = sample_data.get('% Hao hụt OPP', '')
            zero_defect_record['Số lượng hold ( gói/thùng)'] = 0
            
            # Production data if available
            if production_data is not None:
                zero_defect_record['Sản phẩm'] = production_data.get('Sản phẩm', '')
                zero_defect_record['Item'] = production_data.get('Item', '')
                zero_defect_record['Giờ'] = production_data.get('Giờ', '')
                zero_defect_record['QA'] = production_data.get('QA', '')
                zero_defect_record['Tên Trưởng ca'] = production_data.get('Tên Trưởng ca', '')
            else:
                zero_defect_record['Sản phẩm'] = ''
                zero_defect_record['Item'] = ''
                zero_defect_record['Giờ'] = ''
                zero_defect_record['QA'] = ''
                zero_defect_record['Tên Trưởng ca'] = ''
            
            # Target TV
            try:
                line_num = float(sample_data.get('Line', '')) if sample_data.get('Line', '') else None
                zero_defect_record['Target TV'] = get_target_tv(line_num)
            except:
                zero_defect_record['Target TV'] = ''
            
            # Fill remaining columns
            for col in available_columns:
                if col not in zero_defect_record:
                    zero_defect_record[col] = ''
            
            comprehensive_rows.append(pd.Series(zero_defect_record))
            
        except Exception as e:
            processor.log(f"Error creating zero-defect record for VHM {vhm_name}: {e}")
            continue
    
    # Create final dataframe
    if comprehensive_rows:
        comprehensive_df = pd.DataFrame(comprehensive_rows)
        comprehensive_df = comprehensive_df.reindex(columns=available_columns, fill_value='')
        
        # Sort by date
        if 'Ngày SX' in comprehensive_df.columns:
            comprehensive_df['Ngày SX_for_sort'] = comprehensive_df['Ngày SX'].apply(standardize_date)
            comprehensive_df = comprehensive_df.sort_values(by='Ngày SX_for_sort', ascending=False, na_position='last')
            comprehensive_df = comprehensive_df.drop(columns=['Ngày SX_for_sort'])
        
        processor.log(f"Final comprehensive dataset: {len(comprehensive_df)} records")
        
        # Upload to SharePoint
        success = processor.upload_excel_to_sharepoint(
            comprehensive_df, 
            SHAREPOINT_FILE_IDS['cf_data_output'],
            'Processed_Data'
        )
        
        if success:
            processor.log("✅ Data processing completed successfully!")
            processor.log(f"📊 Final dataset includes:")
            processor.log(f"  - Total records: {len(comprehensive_df)}")
            
            # Statistics
            comprehensive_df['temp_numeric'] = pd.to_numeric(
                comprehensive_df['Số lượng hold ( gói/thùng)'], errors='coerce'
            )
            defect_count = len(comprehensive_df[comprehensive_df['temp_numeric'] > 0])
            zero_defect_count = len(comprehensive_df[comprehensive_df['temp_numeric'] == 0])
            
            processor.log(f"  - Records with defects: {defect_count}")
            processor.log(f"  - Zero-defect records: {zero_defect_count}")
            
        else:
            processor.log("❌ Failed to upload data to SharePoint")
            sys.exit(1)
            
    else:
        processor.log("❌ No data to process")
        sys.exit(1)

if __name__ == "__main__":
    main()
