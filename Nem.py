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
        """Upload updated Excel file back to SharePoint"""
        try:
            self.log(f"📤 Uploading updated sampling plan to SharePoint...")

            # Create Excel file in memory with multiple sheets
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                for sheet_name, df in sheets_data.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            excel_buffer.seek(0)
            excel_content = excel_buffer.getvalue()
            self.log(f"Created Excel file with {len(excel_content)} bytes")

            # Upload to SharePoint
            upload_url = f"{self.base_url}/sites/{self.get_site_id()}/drive/items/{SAMPLING_FILE_ID}/content"

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            response = requests.put(upload_url, headers=headers, data=excel_content, timeout=60)

            if response.status_code in [200, 201]:
                self.log(f"✅ Successfully uploaded updated sampling plan to SharePoint")
                return True
            else:
                self.log(f"❌ Error uploading to SharePoint: {response.status_code}")
                self.log(f"Response: {response.text[:500]}")
                return False

        except Exception as e:
            self.log(f"❌ Error uploading to SharePoint: {str(e)}")
            return False

# Helper function to parse dates in different formats
def parse_date(date_str):
    """Try to parse date with multiple formats"""
    if not date_str:
        return None
        
    date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%B %d, %Y', '%m/%d/%Y']
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except (ValueError, TypeError):
            continue
    return None

# Function to update sampling schedule and find due samples
def update_sampling_schedule(df, check_type="Hóa lý"):
    print(f"Đang cập nhật lịch lấy mẫu {check_type}...")
    
    if df.empty:
        print(f"Không tìm thấy dữ liệu trong bảng {check_type}.")
        return [], []
    
    # Create a copy to avoid modifying original dataframe
    updated_df = df.copy()
    
    # Expected columns mapping (flexible column matching)
    col_mapping = {}
    for col in df.columns:
        col_lower = str(col).lower()
        if 'khu vực' in col_lower or 'khu_vuc' in col_lower:
            col_mapping['khu_vuc'] = col
        elif 'sản phẩm' in col_lower or 'san_pham' in col_lower or 'product' in col_lower:
            col_mapping['san_pham'] = col
        elif 'line' in col_lower or 'xưởng' in col_lower:
            col_mapping['line'] = col
        elif 'chỉ tiêu' in col_lower or 'chi_tieu' in col_lower or 'parameter' in col_lower:
            col_mapping['chi_tieu'] = col
        elif 'tần suất' in col_lower or 'tan_suat' in col_lower or 'frequency' in col_lower:
            col_mapping['tan_suat'] = col
        elif 'ngày kiểm tra' in col_lower or 'last check' in col_lower or 'ngay_kiem_tra' in col_lower:
            col_mapping['ngay_kiem_tra'] = col
        elif 'sample id' in col_lower or 'sample_id' in col_lower:
            col_mapping['sample_id'] = col
        elif 'kế hoạch' in col_lower or 'next' in col_lower or 'ke_hoach' in col_lower:
            col_mapping['ke_hoach'] = col
    
    print(f"Detected columns: {col_mapping}")
    
    today = datetime.today()
    due_samples = []
    all_samples = []
    
    # Add 'Kế hoạch lấy mẫu tiếp theo' column if it doesn't exist
    if 'ke_hoach' not in col_mapping:
        next_plan_col = 'Kế hoạch lấy mẫu tiếp theo'
        if next_plan_col not in updated_df.columns:
            updated_df[next_plan_col] = ''
            col_mapping['ke_hoach'] = next_plan_col
    
    # Process each row
    for idx, row in updated_df.iterrows():
        try:
            # Extract data from row using flexible column mapping
            khu_vuc = row.get(col_mapping.get('khu_vuc', ''), '')
            san_pham = row.get(col_mapping.get('san_pham', ''), '')
            line = row.get(col_mapping.get('line', ''), '')
            chi_tieu = row.get(col_mapping.get('chi_tieu', ''), '')
            tan_suat_str = str(row.get(col_mapping.get('tan_suat', ''), ''))
            ngay_kiem_tra = row.get(col_mapping.get('ngay_kiem_tra', ''), '')
            sample_id = row.get(col_mapping.get('sample_id', ''), '')
            
            # Skip if missing critical data
            if not khu_vuc or not san_pham or not ngay_kiem_tra or not tan_suat_str:
                continue
                
            # Parse frequency
            tan_suat = 0
            try:
                tan_suat = int(float(tan_suat_str))
            except (ValueError, TypeError):
                print(f"Lỗi: Tần suất không hợp lệ ở hàng {idx}: '{tan_suat_str}'")
                continue
                
            # Parse last inspection date
            ngay_kiem_tra_date = parse_date(ngay_kiem_tra)
            if not ngay_kiem_tra_date:
                print(f"Lỗi: Định dạng ngày không hợp lệ ở hàng {idx}: '{ngay_kiem_tra}'")
                continue
                
            # Calculate next sampling date
            next_sampling_date = ngay_kiem_tra_date + timedelta(days=tan_suat)
            next_sampling_str = next_sampling_date.strftime('%d/%m/%Y')
            
            # Update the plan column
            if col_mapping.get('ke_hoach'):
                updated_df.at[idx, col_mapping['ke_hoach']] = next_sampling_str
            
            # Determine sample status
            days_until_next = (next_sampling_date.date() - today.date()).days
            status = "Đến hạn" if days_until_next <= 0 else "Chưa đến hạn"
            
            # Create sample record
            sample_record = {
                'khu_vuc': khu_vuc,
                'san_pham': san_pham,
                'line': line,
                'chi_tieu': chi_tieu,
                'tan_suat': tan_suat_str,
                'ngay_kiem_tra': ngay_kiem_tra,
                'sample_id': sample_id,
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
            
        except Exception as e:
            print(f"Lỗi xử lý hàng {idx}: {str(e)}")
            continue
    
    print(f"Đã cập nhật {len(all_samples)} mẫu kiểm tra {check_type}.")
    print(f"Có {len(due_samples)} mẫu {check_type} đến hạn cần lấy.")
    print(f"Tổng số mẫu {check_type} đã được theo dõi: {len(all_samples)}")
    
    return due_samples, all_samples, updated_df

# Function to create summary report
def create_summary_report(all_samples):
    """Create summary report DataFrame"""
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
            sample['ngay_kiem_tra'],
            sample['ke_hoach'],
            sample['loai_kiem_tra'],
            sample['status']
        ])
    
    # Define headers
    headers = ['Khu vực', 'Sản phẩm', 'Line / Xưởng', 'Chỉ tiêu kiểm', 
               'Tần suất (ngày)', 'Sample ID', 'Ngày kiểm tra', 
               'Kế hoạch lấy mẫu tiếp theo', 'Loại kiểm tra', 'Trạng thái']
    
    summary_df = pd.DataFrame(summary_data, columns=headers)
    
    print(f"Đã tạo báo cáo tổng hợp với {len(summary_df)} mẫu.")
    return summary_df

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
        
        # HTML content
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; color: #333; }}
                .due {{ background-color: #ffcccc; }}
                h2 {{ color: #003366; }}
                h3 {{ color: #004d99; margin-top: 25px; }}
                .summary {{ margin: 20px 0; }}
                .footer {{ margin-top: 30px; font-size: 0.9em; color: #666; }}
            </style>
        </head>
        <body>
            <h2>Thông báo lấy mẫu QA - {datetime.today().strftime("%d/%m/%Y")}</h2>
            
            <div class="summary">
                <p><strong>Tổng số mẫu cần lấy:</strong> {len(due_samples)}</p>
                <p><strong>Mẫu Hóa lý:</strong> {len(hoa_ly_samples)}</p>
                <p><strong>Mẫu Vi sinh:</strong> {len(vi_sinh_samples)}</p>
            </div>
        """
        
        # Add tables for each type
        if hoa_ly_samples:
            html_content += create_email_table("Hóa lý", hoa_ly_samples)
        
        if vi_sinh_samples:
            html_content += create_email_table("Vi sinh", vi_sinh_samples)
        
        html_content += """
            <div class="footer">
                <p>Vui lòng thực hiện lấy mẫu và cập nhật ID mẫu vào SharePoint.</p>
                <p>Báo cáo tổng hợp đã được cập nhật trong file Excel.</p>
                <p>Email này được tự động tạo bởi hệ thống. Vui lòng không trả lời.</p>
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
    """Create HTML table for email"""
    html = f"""
    <h3>Danh sách mẫu {check_type} cần lấy:</h3>
    <table>
        <thead>
            <tr>
                <th>Khu vực</th>
                <th>Sản phẩm</th>
                <th>Line / Xưởng</th>
                <th>Chỉ tiêu kiểm</th>
                <th>Tần suất (ngày)</th>
                <th>Ngày kiểm tra gần nhất</th>
                <th>Sample ID</th>
                <th>Kế hoạch lấy mẫu tiếp theo</th>
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
                <td>{sample['tan_suat']}</td>
                <td>{sample['ngay_kiem_tra']}</td>
                <td>{sample['sample_id']}</td>
                <td>{sample['ke_hoach']}</td>
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
        
        # Process each sheet that looks like a sampling schedule
        for sheet_name, df in sheets_data.items():
            # Skip empty sheets or summary sheets
            if df.empty or 'tổng hợp' in sheet_name.lower() or 'summary' in sheet_name.lower():
                updated_sheets[sheet_name] = df
                continue
            
            print(f"\nProcessing sheet: {sheet_name}")
            
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
        
        # Create summary report sheet
        if all_collected_samples:
            summary_df = create_summary_report(all_collected_samples)
            updated_sheets['Báo cáo tổng hợp'] = summary_df
        
        # Upload updated file back to SharePoint
        success = processor.upload_excel_file(updated_sheets)
        if not success:
            print("❌ Failed to upload updated file to SharePoint")
            return False
        
        # Send email notification for due samples
        if all_due_samples:
            send_email_notification(all_due_samples)
        
        print(f"\n✅ Hoàn thành cập nhật:")
        print(f"  - Tổng số mẫu được theo dõi: {len(all_collected_samples)}")
        print(f"  - Mẫu đến hạn cần lấy: {len(all_due_samples)}")
        print(f"  - Sheets đã cập nhật: {len(updated_sheets)}")
        
        return True
        
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
