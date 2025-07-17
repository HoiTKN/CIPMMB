import os
import requests
import json
import time
from datetime import datetime
import pandas as pd
from io import BytesIO
import openpyxl
from config import SHAREPOINT_FILE_IDS

class SharePointDelegationProcessor:
    def __init__(self):
        """Initialize SharePoint processor with auto-refresh capability"""
        self.access_token = None
        self.token_expires_at = 0
        self.max_retries = 3
        self.retry_delay = 5
        
        try:
            if not self.authenticate():
                raise Exception("Authentication failed during initialization")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Processor initialized successfully")
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Initialization failed: {str(e)}")
            raise e

    def authenticate(self):
        """Authenticate with multiple fallback methods"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔐 Authenticating with delegation flow...")
        
        # Method 1: Try existing access token first
        existing_token = os.getenv('SHAREPOINT_ACCESS_TOKEN')
        if existing_token and existing_token.strip():
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Found access token: ***")
            self.access_token = existing_token.strip()
            
            # Test if token is still valid
            if self.test_token_validity():
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Token is valid")
                return True
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Access token expired, attempting refresh...")
        
        # Method 2: Try to get new token using device code flow
        if self.authenticate_device_code():
            return True
            
        # Method 3: Try to get token using client credentials (if secrets available)
        if self.authenticate_client_credentials():
            return True
            
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ All authentication methods failed")
        return False

    def test_token_validity(self):
        """Test if current token is valid by making a simple API call"""
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            
            # Simple test call to Microsoft Graph
            response = requests.get(
                'https://graph.microsoft.com/v1.0/me',
                headers=headers,
                timeout=30
            )
            
            return response.status_code == 200
        except:
            return False

    def authenticate_device_code(self):
        """Authenticate using device code flow (user-friendly for automation)"""
        try:
            tenant_id = os.getenv('TENANT_ID')
            client_id = os.getenv('CLIENT_ID')
            
            if not tenant_id or not client_id:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Missing TENANT_ID or CLIENT_ID for device code flow")
                return False
                
            # This method requires manual intervention, so skip for automation
            return False
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Device code authentication failed: {str(e)}")
            return False

    def authenticate_client_credentials(self):
        """Try client credentials if available"""
        try:
            tenant_id = os.getenv('TENANT_ID')
            client_id = os.getenv('CLIENT_ID')
            client_secret = os.getenv('CLIENT_SECRET')
            
            if not all([tenant_id, client_id, client_secret]):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Missing credentials for client credentials flow")
                return False
            
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            
            data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
                'scope': 'https://graph.microsoft.com/.default'
            }
            
            response = requests.post(token_url, data=data, timeout=30)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                self.token_expires_at = time.time() + token_data.get('expires_in', 3600)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Client credentials authentication successful")
                return True
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Client credentials failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Client credentials authentication failed: {str(e)}")
            return False

    def make_authenticated_request(self, method, url, **kwargs):
        """Make authenticated request with auto-retry on auth failure"""
        for attempt in range(self.max_retries):
            try:
                # Check if token needs refresh
                if time.time() >= self.token_expires_at - 300:  # Refresh 5 minutes before expiry
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Token expiring soon, refreshing...")
                    if not self.authenticate():
                        raise Exception("Failed to refresh token")
                
                headers = kwargs.get('headers', {})
                headers['Authorization'] = f'Bearer {self.access_token}'
                kwargs['headers'] = headers
                
                response = requests.request(method, url, timeout=60, **kwargs)
                
                # If auth failed, try to re-authenticate and retry
                if response.status_code == 401:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Authentication failed (401), attempt {attempt + 1}/{self.max_retries}")
                    
                    if attempt < self.max_retries - 1:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Re-authenticating...")
                        if self.authenticate():
                            time.sleep(self.retry_delay)
                            continue
                        else:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Re-authentication failed")
                    else:
                        raise Exception("Authentication failed after all retries")
                
                # If successful or other error, return response
                return response
                
            except requests.exceptions.RequestException as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Request failed, attempt {attempt + 1}/{self.max_retries}: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise
        
        raise Exception("Max retries exceeded")

    def download_file(self, file_id, file_name):
        """Download file from SharePoint with auto-retry"""
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📥 Downloading {file_name}...")
            
            # Get file download URL
            url = f"https://graph.microsoft.com/v1.0/drives/{self.get_drive_id()}/items/{file_id}/content"
            
            response = self.make_authenticated_request('GET', url)
            response.raise_for_status()
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Downloaded {file_name} ({len(response.content)} bytes)")
            return response.content
            
        except Exception as e:
            error_msg = f"Failed to download {file_name}: {str(e)}"
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ {error_msg}")
            raise Exception(error_msg)

    def upload_file(self, file_id, file_content, file_name):
        """Upload file to SharePoint with auto-retry"""
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📤 Uploading {file_name}...")
            
            url = f"https://graph.microsoft.com/v1.0/drives/{self.get_drive_id()}/items/{file_id}/content"
            
            headers = {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            
            response = self.make_authenticated_request('PUT', url, headers=headers, data=file_content)
            response.raise_for_status()
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Uploaded {file_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to upload {file_name}: {str(e)}"
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ {error_msg}")
            raise Exception(error_msg)

    def get_drive_id(self):
        """Get SharePoint drive ID with caching"""
        if not hasattr(self, '_drive_id'):
            try:
                site_url = os.getenv('SHAREPOINT_SITE_URL', 'https://masancorp.sharepoint.com/sites/QA')
                
                # Extract site info from URL
                url_parts = site_url.replace('https://', '').split('/')
                hostname = url_parts[0]
                site_path = '/'.join(url_parts[1:]) if len(url_parts) > 1 else ''
                
                # Get site ID
                site_api_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
                response = self.make_authenticated_request('GET', site_api_url)
                response.raise_for_status()
                
                site_data = response.json()
                site_id = site_data['id']
                
                # Get default drive
                drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
                response = self.make_authenticated_request('GET', drives_url)
                response.raise_for_status()
                
                drives_data = response.json()
                self._drive_id = drives_data['value'][0]['id']
                
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Failed to get drive ID: {str(e)}")
                # Fallback to a default drive ID if available
                self._drive_id = os.getenv('SHAREPOINT_DRIVE_ID', 'default')
        
        return self._drive_id

    def process_files(self):
        """Main processing logic with enhanced error handling"""
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Starting file processing...")
            
            # Download source files
            sample_id_content = self.download_file(
                SHAREPOINT_FILE_IDS['sample_id'], 
                "Sample ID.xlsx"
            )
            
            data_sx_content = self.download_file(
                SHAREPOINT_FILE_IDS['data_sx'], 
                "Data SX.xlsx"
            )
            
            # Process files (your existing logic here)
            processed_content = self.process_data(sample_id_content, data_sx_content)
            
            # Upload result
            self.upload_file(
                SHAREPOINT_FILE_IDS['cf_data_output'],
                processed_content,
                "CF data.xlsx"
            )
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Processing completed successfully")
            return True
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Processing failed: {str(e)}")
            raise

    def process_data(self, sample_id_content, data_sx_content):
        """Your existing data processing logic"""
        # Implement your data processing logic here
        # This is a placeholder - replace with your actual processing code
        
        try:
            # Load Excel files
            sample_id_wb = openpyxl.load_workbook(BytesIO(sample_id_content))
            data_sx_wb = openpyxl.load_workbook(BytesIO(data_sx_content))
            
            # Your processing logic here...
            # ...
            
            # Save processed data
            output_buffer = BytesIO()
            # Save your processed workbook to output_buffer
            # processed_wb.save(output_buffer)
            
            return output_buffer.getvalue()
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Data processing error: {str(e)}")
            raise

def main():
    """Main execution function"""
    try:
        print("=" * 60)
        print("🏭 MASAN QA DATA PROCESSING - AUTO-REFRESH FLOW")
        print("=" * 60)
        
        # Environment check
        print("🔧 Environment Check:")
        required_vars = ['TENANT_ID', 'CLIENT_ID', 'SHAREPOINT_ACCESS_TOKEN']
        for var in required_vars:
            status = "✅" if os.getenv(var) else "❌"
            print(f"{status} {var}: {'Found' if os.getenv(var) else 'Missing'}")
        
        print("🚀 Initializing processor...")
        processor = SharePointDelegationProcessor()
        
        print("📊 Processing files...")
        processor.process_files()
        
        print("✅ All operations completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        import traceback
        print("Traceback:", traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
