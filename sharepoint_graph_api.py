"""
SharePoint QA Data Processing using Microsoft Graph API
Sử dụng API credentials từ IT team để truy cập SharePoint
"""

import pandas as pd
import requests
import os
import sys
import io
from datetime import datetime
import msal
from config import GRAPH_API_CONFIG, SHAREPOINT_CONFIG, FILE_PATHS, OUTPUT_CONFIG, QA_CONFIG

class SharePointGraphAPI:
    def __init__(self):
        self.access_token = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.authenticate()
    
    def authenticate(self):
        """Authenticate sử dụng Client ID và Client Secret từ IT team"""
        try:
            print("🔐 Authenticating with Microsoft Graph API...")
            
            # Tạo MSAL confidential client application
            app = msal.ConfidentialClientApplication(
                GRAPH_API_CONFIG['client_id'],
                authority=GRAPH_API_CONFIG['authority'],
                client_credential=GRAPH_API_CONFIG['client_secret']
            )
            
            # Acquire token cho application
            result = app.acquire_token_silent(GRAPH_API_CONFIG['scopes'], account=None)
            
            if not result:
                print("📡 Getting new access token...")
                result = app.acquire_token_for_client(scopes=GRAPH_API_CONFIG['scopes'])
            
            if "access_token" in result:
                self.access_token = result['access_token']
                print("✅ Successfully authenticated with Microsoft Graph API")
                return True
            else:
                print(f"❌ Authentication failed: {result.get('error_description', 'Unknown error')}")
                return False
                
        except Exception as e:
            print(f"❌ Authentication error: {str(e)}")
            return False
    
    def get_headers(self):
        """Get headers cho API requests"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def get_site_id(self):
        """Get SharePoint site ID"""
        try:
            url = f"{self.base_url}/sites/masangroup.sharepoint.com:/sites/MCH.MMB.QA"
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                site_data = response.json()
                site_id = site_data['id']
                print(f"✅ Found site ID: {site_id}")
                return site_id
            else:
                print(f"❌ Error getting site ID: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting site ID: {str(e)}")
            return None
    
    def list_drives(self, site_id):
        """List all drives in the site"""
        try:
            url = f"{self.base_url}/sites/{site_id}/drives"
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                drives = response.json()['value']
                print(f"📁 Found {len(drives)} drives:")
                for drive in drives:
                    print(f"  - {drive['name']} (ID: {drive['id']})")
                return drives
            else:
                print(f"❌ Error listing drives: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error listing drives: {str(e)}")
            return []
    
    def find_file_in_drive(self, site_id, folder_path, filename):
        """Tìm file trong SharePoint drive"""
        try:
            # Get drives
            drives = self.list_drives(site_id)
            
            for drive in drives:
                drive_id = drive['id']
                
                # Search for file trong drive này
                search_url = f"{self.base_url}/sites/{site_id}/drives/{drive_id}/root/search(q='{filename}')"
                response = requests.get(search_url, headers=self.get_headers())
                
                if response.status_code == 200:
                    items = response.json().get('value', [])
                    
                    for item in items:
                        if item['name'] == filename:
                            print(f"✅ Found file: {filename} in drive: {drive['name']}")
                            return drive_id, item['id'], item['@microsoft.graph.downloadUrl']
            
            print(f"❌ File not found: {filename}")
            return None, None, None
            
        except Exception as e:
            print(f"❌ Error finding file {filename}: {str(e)}")
            return None, None, None
    
    def download_excel_file(self, download_url, description=""):
        """Download Excel file từ SharePoint"""
        try:
            print(f"📥 Downloading {description}...")
            
            response = requests.get(download_url)
            
            if response.status_code == 200:
                # Read Excel từ memory
                excel_data = io.BytesIO(response.content)
                df = pd.read_excel(excel_data)
                
                print(f"✅ Successfully downloaded {description}: {len(df)} rows")
                return df
            else:
                print(f"❌ Error downloading {description}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error downloading {description}: {str(e)}")
            return None
    
    def process_qa_file(self, file_config):
        """Process một file QA cụ thể"""
        try:
            # Get site ID
            site_id = self.get_site_id()
            if not site_id:
                return None
            
            # Find và download file
            drive_id, file_id, download_url = self.find_file_in_drive(
                site_id, 
                file_config['folder'], 
                file_config['filename']
            )
            
            if download_url:
                df = self.download_excel_file(download_url, file_config['description'])
                return df
            else:
                print(f"❌ Could not find file: {file_config['filename']}")
                return None
                
        except Exception as e:
            print(f"❌ Error processing file {file_config['filename']}: {str(e)}")
            return None

class QADataProcessor:
    """Class để xử lý dữ liệu QA sau khi download từ SharePoint"""
    
    def __init__(self):
        self.sharepoint = SharePointGraphAPI()
        self.processed_data = {}
    
    def download_all_files(self):
        """Download tất cả files QA từ SharePoint"""
        print("\n🚀 Starting QA data download process...")
        
        for file_key, file_config in FILE_PATHS.items():
            print(f"\n--- Processing {file_config['description']} ---")
            df = self.sharepoint.process_qa_file(file_config)
            
            if df is not None:
                self.processed_data[file_key] = df
                print(f"✅ Successfully processed {file_config['description']}")
            else:
                print(f"❌ Failed to process {file_config['description']}")
        
        return len(self.processed_data) > 0
    
    def analyze_quality_data(self):
        """Phân tích dữ liệu quality - implement your existing logic here"""
        try:
            print("\n📊 Analyzing quality data...")
            
            # Implement your existing data processing logic here
            # Ví dụ: extract production info, standardize dates, etc.
            
            if 'sample_id' in self.processed_data:
                sample_df = self.processed_data['sample_id']
                print(f"📋 Sample ID data: {len(sample_df)} records")
                
                # Add your existing processing logic
                # - Clean dates
                # - Extract production information  
                # - Match QA and leaders
                # - Calculate quality metrics
            
            if 'quality_daily' in self.processed_data:
                daily_df = self.processed_data['quality_daily']
                print(f"📈 Daily quality data: {len(daily_df)} records")
                
                # Add your existing daily quality processing
            
            print("✅ Quality data analysis completed")
            return True
            
        except Exception as e:
            print(f"❌ Error in quality analysis: {str(e)}")
            return False
    
    def generate_reports(self):
        """Generate báo cáo QA"""
        try:
            print("\n📝 Generating QA reports...")
            
            # Create output directory
            os.makedirs(OUTPUT_CONFIG['local_output_dir'], exist_ok=True)
            
            # Generate timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Main processed data file
            main_output = os.path.join(
                OUTPUT_CONFIG['local_output_dir'],
                OUTPUT_CONFIG['processed_filename'].format(timestamp=timestamp)
            )
            
            with pd.ExcelWriter(main_output, engine='openpyxl') as writer:
                for file_key, df in self.processed_data.items():
                    sheet_name = FILE_PATHS[file_key]['description'].replace(' ', '_')[:30]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Main report saved: {main_output}")
            
            # Summary report
            summary_output = os.path.join(
                OUTPUT_CONFIG['local_output_dir'],
                OUTPUT_CONFIG['summary_filename'].format(timestamp=timestamp)
            )
            
            # Create summary DataFrame
            summary_data = []
            for file_key, df in self.processed_data.items():
                summary_data.append({
                    'File': FILE_PATHS[file_key]['description'],
                    'Records': len(df),
                    'Columns': len(df.columns),
                    'Last_Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(summary_output, index=False)
            
            print(f"✅ Summary report saved: {summary_output}")
            return True
            
        except Exception as e:
            print(f"❌ Error generating reports: {str(e)}")
            return False

def main():
    """Main function"""
    print("="*60)
    print("🏭 MASAN QA DATA PROCESSING - SHAREPOINT INTEGRATION")
    print("="*60)
    
    # Kiểm tra credentials
    if not GRAPH_API_CONFIG['client_secret']:
        print("❌ CLIENT_SECRET not found. Please ask IT team for this credential.")
        print("Set it as GitHub secret: CLIENT_SECRET")
        sys.exit(1)
    
    # Initialize processor
    processor = QADataProcessor()
    
    # Check authentication
    if not processor.sharepoint.access_token:
        print("❌ Failed to authenticate with SharePoint")
        sys.exit(1)
    
    # Download all files
    if not processor.download_all_files():
        print("❌ Failed to download QA files")
        sys.exit(1)
    
    # Analyze data
    if not processor.analyze_quality_data():
        print("❌ Failed to analyze quality data")
        sys.exit(1)
    
    # Generate reports
    if not processor.generate_reports():
        print("❌ Failed to generate reports")
        sys.exit(1)
    
    print("\n🎉 QA Data processing completed successfully!")
    print(f"📁 Check output folder: {OUTPUT_CONFIG['local_output_dir']}")

if __name__ == "__main__":
    main()
