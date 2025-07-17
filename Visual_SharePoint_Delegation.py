import os
import requests
import json
import time
from datetime import datetime
import pandas as pd
from io import BytesIO
import openpyxl

# Define SharePoint file IDs directly
SHAREPOINT_FILE_IDS = {
    'sample_id': '8220CAEA-0CD9-585B-D483-DE0A82A98564',
    'data_sx': '6CB4A738-1EDD-4BC4-9996-43A815D3F5CF', 
    'cf_data_output': 'E1B65B6F-6A53-52E0-1BB3-3BCA75A32F63'
}

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
        
        # Method 2: Try to get token using client credentials (if secrets available)
        if self.authenticate_client_credentials():
            return True
            
        # Method 3: Fallback - use existing token anyway and let retry mechanism handle it
        if existing_token and existing_token.strip():
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Using existing token with retry mechanism")
            self.access_token = existing_token.strip()
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
                timeout=10
            )
            
            is_valid = response.status_code == 200
            if not is_valid:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Token test failed with status: {response.status_code}")
            
            return is_valid
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Token test failed with error: {str(e)}")
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
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                headers = kwargs.get('headers', {})
                headers['Authorization'] = f'Bearer {self.access_token}'
                headers['Accept'] = 'application/json'
                kwargs['headers'] = headers
                
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Making request (attempt {attempt + 1}/{self.max_retries}): {method} {url[:100]}...")
                
                response = requests.request(method, url, timeout=60, **kwargs)
                
                # If auth failed, try to re-authenticate and retry
                if response.status_code == 401:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Authentication failed (401), attempt {attempt + 1}/{self.max_retries}")
                    
                    if attempt < self.max_retries - 1:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Re-authenticating...")
                        
                        # Try to get fresh token
                        if self.authenticate_client_credentials():
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Re-authentication successful, retrying...")
                            time.sleep(self.retry_delay)
                            continue
                        else:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Re-authentication failed, using existing token")
                            time.sleep(self.retry_delay)
                            continue
                    else:
                        raise Exception(f"Authentication failed after {self.max_retries} attempts. Status: {response.status_code}")
                
                # If successful or other error, return response
                if response.status_code < 400:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Request successful: {response.status_code}")
                    return response
                else:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Request failed with status: {response.status_code}")
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        raise Exception(last_error)
                
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Request exception, attempt {attempt + 1}/{self.max_retries}: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise Exception(f"Request failed after {self.max_retries} attempts: {last_error}")
        
        raise Exception(f"Max retries exceeded. Last error: {last_error}")

    def get_drive_id(self):
        """Get SharePoint drive ID with simplified approach"""
        if not hasattr(self, '_drive_id'):
            try:
                # Try to get site and drive info
                site_url = os.getenv('SHAREPOINT_SITE_URL', 'https://masancorp.sharepoint.com/sites/QA')
                
                # Extract hostname and site path
                url_parts = site_url.replace('https://', '').split('/')
                hostname = url_parts[0]
                site_path = '/'.join(url_parts[1:]) if len(url_parts) > 1 else 'sites/QA'
                
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 Getting drive ID for: {hostname}:/{site_path}")
                
                # Get site ID
                site_api_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
                response = self.make_authenticated_request('GET', site_api_url)
                response.raise_for_status()
                
                site_data = response.json()
                site_id = site_data['id']
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Got site ID: {site_id}")
                
                # Get default drive
                drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
                response = self.make_authenticated_request('GET', drives_url)
                response.raise_for_status()
                
                drives_data = response.json()
                if drives_data.get('value'):
                    self._drive_id = drives_data['value'][0]['id']
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Got drive ID: {self._drive_id}")
                else:
                    raise Exception("No drives found")
                
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Failed to get drive ID: {str(e)}")
                # Use a fallback approach - try common drive IDs or use a default
                self._drive_id = os.getenv('SHAREPOINT_DRIVE_ID', 'b!default')
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ Using fallback drive ID: {self._drive_id}")
        
        return self._drive_id

    def download_file(self, file_id, file_name):
        """Download file from SharePoint with auto-retry"""
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📥 Downloading {file_name}...")
            
            # Get file download URL  
            drive_id = self.get_drive_id()
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/content"
            
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
            
            drive_id = self.get_drive_id()
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/content"
            
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
            
            # Process files (implement your existing logic here)
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
        """Data processing logic - placeholder for your implementation"""
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Processing data...")
            
            # Load Excel files
            sample_id_wb = openpyxl.load_workbook(BytesIO(sample_id_content))
            data_sx_wb = openpyxl.load_workbook(BytesIO(data_sx_content))
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Loaded Excel files")
            print(f"Sample ID sheets: {sample_id_wb.sheetnames}")
            print(f"Data SX sheets: {data_sx_wb.sheetnames}")
            
            # TODO: Implement your actual data processing logic here
            # For now, create a simple output file
            output_wb = openpyxl.Workbook()
            output_ws = output_wb.active
            output_ws.title = "CF Data"
            
            # Add some sample data
            output_ws['A1'] = "Processed Date"
            output_ws['B1'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            output_ws['A2'] = "Status"
            output_ws['B2'] = "Processing Completed"
            
            # Save to buffer
            output_buffer = BytesIO()
            output_wb.save(output_buffer)
            output_buffer.seek(0)
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Data processing completed")
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
        required_vars = ['TENANT_ID', 'CLIENT_ID']
        optional_vars = ['CLIENT_SECRET', 'SHAREPOINT_ACCESS_TOKEN', 'SHAREPOINT_SITE_URL']
        
        for var in required_vars:
            status = "✅" if os.getenv(var) else "❌"
            print(f"{status} {var}: {'Found' if os.getenv(var) else 'Missing'}")
        
        for var in optional_vars:
            status = "✅" if os.getenv(var) else "⚠️"
            print(f"{status} {var}: {'Found' if os.getenv(var) else 'Optional'}")
        
        print(f"📋 SharePoint File IDs: {SHAREPOINT_FILE_IDS}")
        
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
