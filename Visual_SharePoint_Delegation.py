"""
SharePoint QA Data Processing - Delegation Flow Version with Auto Token Refresh
Xử lý dữ liệu QA từ SharePoint sử dụng delegation flow (không cần CLIENT_SECRET)
Tự động refresh token để chạy hàng ngày
"""

import pandas as pd
import os
import sys
import io
import requests
from datetime import datetime, timedelta
import msal
import time
import json
import base64
import traceback

# Import config
try:
    from config_delegation import GRAPH_API_CONFIG, SHAREPOINT_CONFIG, FILE_PATHS, OUTPUT_CONFIG, QA_CONFIG, SHAREPOINT_FILE_IDS, TOKEN_CONFIG
    print("✅ Config import successful")
except ImportError as e:
    print(f"❌ Config import error: {str(e)}")
    print("Available files in current directory:")
    print(os.listdir('.'))
    sys.exit(1)
except Exception as e:
    print(f"❌ Config error: {str(e)}")
    sys.exit(1)

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

class SharePointDelegationProcessor:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.site_id = None
        self.processed_data = {}
        self.msal_app = None
        
        # Initialize MSAL app
        self.msal_app = msal.PublicClientApplication(
            GRAPH_API_CONFIG['client_id'],
            authority=GRAPH_API_CONFIG['authority']
        )
        
        # Authenticate on initialization
        try:
            if not self.authenticate():
                raise Exception("Authentication failed during initialization")
        except Exception as e:
            self.log(f"❌ Initialization failed: {str(e)}")
            raise

    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        sys.stdout.flush()

    def authenticate(self):
        """Authenticate using delegation flow with pre-generated tokens"""
        try:
            self.log("🔐 Authenticating with delegation flow...")

            # Get tokens from environment variables
            access_token = os.environ.get('SHAREPOINT_ACCESS_TOKEN')
            refresh_token = os.environ.get('SHAREPOINT_REFRESH_TOKEN')

            if not access_token and not refresh_token:
                self.log("❌ No tokens found in environment variables")
                self.log("💡 Please run generate_tokens.py locally and add tokens to GitHub Secrets")
                return False

            self.access_token = access_token
            self.refresh_token = refresh_token
            
            if access_token:
                self.log(f"✅ Found access token: {access_token[:30]}...")
                
                # Test token validity
                if self.test_token_validity():
                    self.log("✅ Access token is valid")
                    return True
                else:
                    self.log("⚠️ Access token expired, attempting refresh...")
                    
            # Try to refresh token
            if refresh_token:
                if self.refresh_access_token_with_msal():
                    self.log("✅ Token refreshed successfully")
                    self.update_github_secrets()
                    return True
                else:
                    self.log("❌ Token refresh failed")
                    return False
            else:
                self.log("❌ No refresh token available")
                return False

        except Exception as e:
            self.log(f"❌ Authentication error: {str(e)}")
            return False

    def test_token_validity(self):
        """Test if current access token is valid"""
        try:
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/me", headers=headers, timeout=30)

            if response.status_code == 200:
                user_info = response.json()
                self.log(f"✅ Authenticated as: {user_info.get('displayName', 'Unknown')}")
                return True
            elif response.status_code == 401:
                return False
            else:
                self.log(f"Warning: Unexpected response code: {response.status_code}")
                return False

        except Exception as e:
            self.log(f"Error testing token validity: {str(e)}")
            return False

    def refresh_access_token_with_msal(self):
        """Refresh access token using refresh token with MSAL"""
        try:
            if not self.refresh_token:
                self.log("❌ No refresh token available")
                return False

            self.log("🔄 Attempting to refresh token using MSAL...")

            # Use MSAL to refresh token
            result = None
            
            # Try to get accounts from cache
            accounts = self.msal_app.get_accounts()
            
            if accounts:
                # Try silent token acquisition
                result = self.msal_app.acquire_token_silent(
                    GRAPH_API_CONFIG['scopes'], 
                    account=accounts[0]
                )
            
            # If silent acquisition fails, try direct refresh
            if not result or "access_token" not in result:
                # Create a custom token cache with our refresh token
                cache = msal.SerializableTokenCache()
                
                # Try to acquire token using refresh token
                result = self.msal_app.acquire_token_by_refresh_token(
                    self.refresh_token,
                    scopes=GRAPH_API_CONFIG['scopes']
                )

            if result and "access_token" in result:
                self.access_token = result['access_token']
                if 'refresh_token' in result:
                    self.refresh_token = result['refresh_token']
                    self.log("✅ Got new refresh token")
                
                # Calculate token expiry
                expires_in = result.get('expires_in', 3600)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
                
                self.log("✅ Token refreshed successfully")
                self.log(f"📅 New token expires at: {self.token_expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
                
                return True
            else:
                error = result.get('error_description', 'Unknown error') if result else 'No result'
                self.log(f"❌ Token refresh failed: {error}")
                return False

        except Exception as e:
            self.log(f"❌ Error refreshing token: {str(e)}")
            self.log(f"Traceback: {traceback.format_exc()}")
            return False

    def update_github_secrets(self):
        """Update GitHub Secrets with new tokens"""
        try:
            # Get GitHub token from environment
            github_token = os.environ.get('GITHUB_TOKEN')
            if not github_token:
                self.log("⚠️ No GITHUB_TOKEN found, cannot update secrets")
                return False
            
            # Get repository info from environment
            repo = os.environ.get('GITHUB_REPOSITORY', '')
            if '/' not in repo:
                self.log("⚠️ Invalid GITHUB_REPOSITORY format")
                return False
            
            repo_owner, repo_name = repo.split('/')
            
            # Initialize updater
            updater = GitHubSecretsUpdater(repo_owner, repo_name, github_token)
            
            # Update access token
            if self.access_token:
                success = updater.update_secret('SHAREPOINT_ACCESS_TOKEN', self.access_token)
                if not success:
                    self.log("⚠️ Failed to update access token in GitHub Secrets")
            
            # Update refresh token
            if self.refresh_token:
                success = updater.update_secret('SHAREPOINT_REFRESH_TOKEN', self.refresh_token)
                if not success:
                    self.log("⚠️ Failed to update refresh token in GitHub Secrets")
            
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
                self.log(f"✅ Found site ID: {self.site_id}")
                return self.site_id
            elif response.status_code == 401:
                # Token might be expired, try refresh
                if self.refresh_access_token_with_msal():
                    self.update_github_secrets()
                    return self.get_site_id()  # Retry
                else:
                    self.log("❌ Authentication failed and token refresh unsuccessful")
                    return None
            else:
                self.log(f"❌ Error getting site ID: {response.status_code}")
                self.log(f"Response text: {response.text[:500]}")
                return None

        except Exception as e:
            self.log(f"❌ Error getting site ID: {str(e)}")
            return None

    def download_excel_file_by_id(self, file_id, description=""):
        """Download Excel file từ SharePoint bằng file ID với retry logic"""
        max_retries = TOKEN_CONFIG['max_retry_attempts']
        retry_delay = TOKEN_CONFIG['retry_delay']

        self.log(f"📥 Starting download of {description}...")
        
        for attempt in range(max_retries):
            try:
                self.log(f"📥 Downloading {description}... (Attempt {attempt + 1}/{max_retries})")

                # Get file download URL using file ID
                url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}"
                response = requests.get(url, headers=self.get_headers(), timeout=30)

                if response.status_code == 401 and attempt < max_retries - 1:
                    # Token expired, try refresh
                    self.log("🔄 Token expired, refreshing...")
                    if self.refresh_access_token_with_msal():
                        self.update_github_secrets()
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
                        self.log(f"✅ Got download URL, downloading content...")
                        file_response = requests.get(download_url, timeout=60)

                        if file_response.status_code == 200:
                            # Read Excel từ memory
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
                elif response.status_code == 404:
                    self.log(f"❌ File not found: {file_id}")
                    return None
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
                self.log(f"Created Excel file with {len(excel_content)} bytes")

                # Upload to SharePoint
                upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{file_id}/content"

                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }

                response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)

                if response.status_code == 401 and attempt < max_retries - 1:
                    # Token expired, try refresh
                    self.log("🔄 Token expired during upload, refreshing...")
                    if self.refresh_access_token_with_msal():
                        self.update_github_secrets()
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
                    self.log(f"Response: {response.text[:500]}")

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

# ============================================================================
# IMPORT ALL PROCESSING FUNCTIONS FROM Visual.py
# ============================================================================

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
    """Create a mapping key considering extended shift logic and MĐG grouping based on actual working hours"""
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
    """Create mapping keys for sample_id_df records, handling MĐG grouping logic"""
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
    """Expand dataframe rows that have comma-separated MĐG values into separate rows"""
    expanded_rows = []

    for _, row in df.iterrows():
        mdg_values = parse_mdg_values(row.get('MĐG', ''))

        if len(mdg_values) <= 1:
            expanded_rows.append(row)
        else:
            for mdg_val in mdg_values:
                new_row = row.copy()
                new_row['MĐG'] = mdg_val
                new_row['MĐG_Original'] = row['MДG']
                expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows)

def find_representative_production_data(vhm_name, sample_id_df, existing_aql_df):
    """Find representative production data for a given VHM using the best available sample data"""
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

        # Priority 3-5: fallback logic
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
    """Main processing function - port từ Visual.py sang SharePoint"""
    print("="*60)
    print("🏭 MASAN QA DATA PROCESSING - SHAREPOINT DELEGATION FLOW")
    print("="*60)

    # Check environment variables
    print("\n🔧 Environment Check:")
    required_env_vars = ['TENANT_ID', 'CLIENT_ID', 'SHAREPOINT_ACCESS_TOKEN']
    missing_vars = []
    
    for var in required_env_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
        else:
            print(f"✅ {var}: Found")
    
    if missing_vars and 'SHAREPOINT_REFRESH_TOKEN' not in os.environ:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("💡 Please run generate_tokens.py locally and add tokens to GitHub Secrets:")
        print("   1. SHAREPOINT_ACCESS_TOKEN")
        print("   2. SHAREPOINT_REFRESH_TOKEN (required for auto-refresh)")
        sys.exit(1)
    
    # Initialize processor
    print(f"\n🚀 Initializing processor...")
    processor = SharePointDelegationProcessor()
    
    try:
        # Download files theo cấu trúc ĐÚNG
        print(f"\n📥 Downloading files with CORRECTED structure...")
        print(f"📋 File Structure:")
        print(f"  - Sample ID.xlsx = SOURCE SHEET (ID AQL, AQL gói, AQL Tô ly)")
        print(f"  - Data SX.xlsx = SAMPLE ID SHEET (VHM, % Hao hụt OPP)")
        print(f"  - CF data.xlsx = DESTINATION SHEET (Output)")
        
        # Download SOURCE SHEET (Sample ID.xlsx) - chứa ID AQL, AQL gói, AQL Tô ly
        source_sheet_data = processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['sample_id'], 
            "SOURCE SHEET (Sample ID.xlsx)"
        )

        if not source_sheet_data:
            print("❌ Failed to download source sheet")
            sys.exit(1)

        # Extract sheets từ source sheet
        id_aql_df = source_sheet_data.get('ID AQL', pd.DataFrame())
        aql_goi_df = source_sheet_data.get('AQL gói', pd.DataFrame())
        aql_to_ly_df = source_sheet_data.get('AQL Tô ly', pd.DataFrame())
        
        print(f"✅ ID AQL data: {len(id_aql_df)} rows")
        print(f"✅ AQL gói data: {len(aql_goi_df)} rows") 
        print(f"✅ AQL Tô ly data: {len(aql_to_ly_df)} rows")

        # Download SAMPLE ID SHEET (Data SX.xlsx) - chứa VHM và % Hao hụt OPP
        sample_id_sheet_data = processor.download_excel_file_by_id(
            SHAREPOINT_FILE_IDS['data_sx'],
            "SAMPLE ID SHEET (Data SX.xlsx)"
        )

        if not sample_id_sheet_data:
            print("❌ Failed to download sample ID sheet")
            sys.exit(1)

        # Get first sheet from sample ID sheet
        sample_id_df = list(sample_id_sheet_data.values())[0]
        print(f"✅ Sample ID data: {len(sample_id_df)} rows")
        print(f"Sample ID columns: {list(sample_id_df.columns)}")

    except Exception as e:
        print(f"❌ Critical error during file download: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

    # ========================================================================
    # APPLY FULL LOGIC FROM Visual.py
    # ========================================================================

    try:
        print(f"\n🔄 Processing data using Visual.py logic...")
        
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
                print(f"Warning: Missing columns in {sheet_name}: {missing_cols}")
                print(f"Available columns in {sheet_name}: {df.columns.tolist()}")
        
        # Convert 'Line' to numeric
        if 'Line' in id_aql_df.columns:
            id_aql_df['Line'] = pd.to_numeric(id_aql_df['Line'], errors='coerce')
        
        # Expand MĐG values
        print(f"Original rows before MĐG expansion: {len(id_aql_df)}")
        id_aql_df = expand_dataframe_for_multiple_mdg(id_aql_df)
        print(f"Rows after MĐG expansion: {len(id_aql_df)}")
        
        # Standardize defect codes
        for df_name, df in [('ID AQL', id_aql_df), ('AQL gói', aql_goi_df), ('AQL Tô ly', aql_to_ly_df)]:
            if 'Defect code' in df.columns:
                df['Defect code'] = df['Defect code'].astype(str).str.strip()
        
        # Standardize dates and extract date components
        if 'Ngày SX' in id_aql_df.columns:
            id_aql_df['Ngày SX_std'] = id_aql_df['Ngày SX'].apply(standardize_date)
            
            # ✅ SỬA LỖI: Sử dụng accessor .dt để tính toán nhanh hơn và tự động xử lý NaT
            # This is the fix for the "NaTType does not support isocalendar" error
            id_aql_df['Ngày'] = id_aql_df['Ngày SX_std'].dt.day
            id_aql_df['Tuần'] = id_aql_df['Ngày SX_std'].dt.isocalendar().week
            id_aql_df['Tháng'] = id_aql_df['Ngày SX_std'].dt.month
        
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
        print("Creating VHM and % Hao hụt OPP mapping...")
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
        
        print(f"Created {len(vhm_mapping)} mapping entries")
        
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
        print(f"Successfully mapped VHM for {vhm_mapped_count} out of {len(id_aql_df)} records")
        
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
        print("Creating comprehensive dataset...")
        
        # Convert hold quantity to numeric
        existing_aql_df['Số lượng hold ( gói/thùng)_numeric'] = pd.to_numeric(
            existing_aql_df['Số lượng hold ( gói/thùng)'], errors='coerce'
        )

        # Get defect records
        defect_records = existing_aql_df[
            existing_aql_df['Số lượng hold ( gói/thùng)_numeric'] > 0
        ].copy().drop(columns=['Số lượng hold ( gói/thùng)_numeric'])
        
        print(f"Found {len(defect_records)} records with defects")
        
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
        
        print(f"Creating zero-defect records for {len(vhms_without_defects)} VHMs")
        
        for vhm_name in vhms_without_defects:
            try:
                sample_data, production_data = find_representative_production_data(
                    vhm_name, sample_id_df, existing_aql_df
                )
                
                if sample_data is None:
                    continue
                
                # Create zero-defect record (same logic as Visual.py)
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
                print(f"Error creating zero-defect record for VHM {vhm_name}: {e}")
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

            print(f"Final comprehensive dataset: {len(comprehensive_df)} records")

            # Upload to SharePoint
            success = processor.upload_excel_to_sharepoint(
                comprehensive_df, 
                SHAREPOINT_FILE_IDS['cf_data_output'],
                'Processed_Data'
            )
            
            if success:
                print("✅ Data processing completed successfully!")
                print(f"📊 Final dataset includes:")
                print(f"  - Total records: {len(comprehensive_df)}")
                
                # Statistics
                comprehensive_df['temp_numeric'] = pd.to_numeric(
                    comprehensive_df['Số lượng hold ( gói/thùng)'], errors='coerce'
                )
                defect_count = len(comprehensive_df[comprehensive_df['temp_numeric'] > 0])
                zero_defect_count = len(comprehensive_df[comprehensive_df['temp_numeric'] == 0])
                
                print(f"  - Records with defects: {defect_count}")
                print(f"  - Zero-defect records: {zero_defect_count}")
                
            else:
                print("❌ Failed to upload data to SharePoint")
                sys.exit(1)
                
        else:
            print("❌ No data to process")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Critical error during data processing: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
