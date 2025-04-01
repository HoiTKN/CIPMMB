import gspread
import os
import json
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import io
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import sys

# 1. Authentication setup
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Detect if running in GitHub Actions or similar CI environment
IN_CI_ENVIRONMENT = os.environ.get('CI') or os.environ.get('GITHUB_ACTIONS')

# Authentication logic with CI environment detection
def authenticate():
    creds = None
    
    # If in CI environment, use saved token or environment variable
    if IN_CI_ENVIRONMENT:
        print("Running in CI environment, using saved token...")
        try:
            # First try using the token from environment variable
            if os.environ.get('GOOGLE_TOKEN_JSON'):
                token_json = os.environ.get('GOOGLE_TOKEN_JSON')
                with open('token.json', 'w') as f:
                    f.write(token_json)
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            # Then try using the token file directly 
            elif os.path.exists('token.json'):
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            else:
                print("Error: No authentication token found.")
                sys.exit(1)
                
            # Refresh the token if necessary
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Save the refreshed token
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
                    
            return gspread.authorize(creds)
        except Exception as e:
            print(f"Authentication error: {str(e)}")
            sys.exit(1)
    
    # For local environment, use the normal OAuth flow
    else:
        print("Running in local environment, using OAuth authentication...")
        creds_file = 'client_secret.json'
        
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
                
        return gspread.authorize(creds)

# Initialize Google Sheets client
gc = authenticate()

# 2. Open Google Sheet with the QA sampling schedule
sheet_id = '1MwAGOGpCNZrUiJJQY-G2BKCUnIFua-nZS4vC8siGtUI'  # New spreadsheet ID
spreadsheet = gc.open_by_key(sheet_id)

# Get or create necessary worksheets
def get_or_create_sheet(name, rows=100, cols=20):
    try:
        worksheet = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
    return worksheet

# Get the main sampling schedule sheet and create history sheet if needed
sampling_schedule = get_or_create_sheet('Sheet1')  # Main sheet with sampling schedule
sampling_history = get_or_create_sheet('Lịch sử mẫu')  # History sheet to track samples

# 3. Helper function to parse dates in different formats
def parse_date(date_str):
    """Try to parse date with multiple formats"""
    if not date_str:
        return None
        
    date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%B %d, %Y']
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

# 4. Main function to update sampling schedule
def update_sampling_schedule():
    print("Đang cập nhật lịch lấy mẫu...")
    
    # Read sampling schedule data
    schedule_data = sampling_schedule.get_all_values()
    
    # Ensure headers exist
    if len(schedule_data) <= 1:
        print("Không tìm thấy dữ liệu hoặc không đủ hàng.")
        return []
    
    # Extract header and data rows
    header = schedule_data[0]
    rows = schedule_data[1:]
    
    # Find column indices
    col_indices = {
        'khu_vuc': header.index('Khu vực') if 'Khu vực' in header else 0,
        'san_pham': header.index('Sản phẩm') if 'Sản phẩm' in header else 1,
        'line': header.index('Line / Xưởng') if 'Line / Xưởng' in header else 2,
        'chi_tieu': header.index('Chỉ tiêu kiểm') if 'Chỉ tiêu kiểm' in header else 3,
        'tan_suat': header.index('Tần suất (ngày)') if 'Tần suất (ngày)' in header else 4,
        'ngay_kiem_tra': header.index('Ngày kiểm tra gần nhất') if 'Ngày kiểm tra gần nhất' in header else 5,
        'sample_id': header.index('Sample ID') if 'Sample ID' in header else 6,
        'ke_hoach': header.index('Kế hoạch lấy mẫu tiếp theo') if 'Kế hoạch lấy mẫu tiếp theo' in header else 7
    }
    
    today = datetime.today()
    updated_rows = []
    due_samples = []
    
    # Process each row and calculate next sampling date
    for i, row in enumerate(rows, start=2):  # Start from 2 as row 1 is the header
        # Extract data from row
        if len(row) <= max(col_indices.values()):
            # Skip rows that don't have enough columns
            continue
            
        khu_vuc = row[col_indices['khu_vuc']]
        san_pham = row[col_indices['san_pham']]
        line = row[col_indices['line']]
        chi_tieu = row[col_indices['chi_tieu']]
        tan_suat_str = row[col_indices['tan_suat']]
        ngay_kiem_tra = row[col_indices['ngay_kiem_tra']]
        sample_id = row[col_indices['sample_id']]
        ke_hoach = row[col_indices['ke_hoach']]
        
        # Skip if missing critical data
        if not khu_vuc or not san_pham or not ngay_kiem_tra or not tan_suat_str:
            continue
            
        # Parse frequency
        tan_suat = 0
        try:
            tan_suat = int(tan_suat_str)
        except ValueError:
            print(f"Lỗi: Tần suất không hợp lệ ở hàng {i}: '{tan_suat_str}'")
            continue
            
        # Parse last inspection date
        ngay_kiem_tra_date = parse_date(ngay_kiem_tra)
        if not ngay_kiem_tra_date:
            print(f"Lỗi: Định dạng ngày không hợp lệ ở hàng {i}: '{ngay_kiem_tra}'")
            continue
            
        # Calculate next sampling date
        next_sampling_date = ngay_kiem_tra_date + timedelta(days=tan_suat)
        next_sampling_str = next_sampling_date.strftime('%d/%m/%Y')
        
        # Determine sample status
        days_until_next = (next_sampling_date.date() - today.date()).days
        status = ""
        
        if days_until_next <= 0:
            status = "Đến hạn"
            due_samples.append({
                'khu_vuc': khu_vuc,
                'san_pham': san_pham,
                'line': line,
                'chi_tieu': chi_tieu,
                'tan_suat': tan_suat_str,
                'ngay_kiem_tra': ngay_kiem_tra,
                'ke_hoach': next_sampling_str,
                'row_index': i
            })
        
        # Update the Next Sampling Plan cell
        sampling_schedule.update_cell(i, col_indices['ke_hoach'] + 1, next_sampling_str)
        
        # Store updated row data
        updated_row = list(row)
        updated_row[col_indices['ke_hoach']] = next_sampling_str
        updated_rows.append(updated_row)
        
    print(f"Đã cập nhật {len(updated_rows)} mẫu kiểm tra.")
    print(f"Có {len(due_samples)} mẫu đến hạn cần lấy.")
    
    return due_samples

# 5. Function to check for new sample IDs and update history
def update_sample_history():
    print("Đang kiểm tra và cập nhật ID mẫu mới...")
    
    # Get data from both sheets
    schedule_data = sampling_schedule.get_all_values()
    history_data = sampling_history.get_all_values()
    
    # Check if history sheet has headers
    if not history_data:
        history_headers = ['Khu vực', 'Sản phẩm', 'Line / Xưởng', 'Chỉ tiêu kiểm', 
                           'Ngày kiểm tra', 'Sample ID', 'Ngày cập nhật']
        sampling_history.append_row(history_headers)
        history_data = [history_headers]
    
    # Extract header and data rows
    schedule_header = schedule_data[0]
    schedule_rows = schedule_data[1:]
    
    # Find column indices for schedule sheet
    schedule_indices = {
        'khu_vuc': schedule_header.index('Khu vực') if 'Khu vực' in schedule_header else 0,
        'san_pham': schedule_header.index('Sản phẩm') if 'Sản phẩm' in schedule_header else 1,
        'line': schedule_header.index('Line / Xưởng') if 'Line / Xưởng' in schedule_header else 2,
        'chi_tieu': schedule_header.index('Chỉ tiêu kiểm') if 'Chỉ tiêu kiểm' in schedule_header else 3,
        'ngay_kiem_tra': schedule_header.index('Ngày kiểm tra gần nhất') if 'Ngày kiểm tra gần nhất' in schedule_header else 5,
        'sample_id': schedule_header.index('Sample ID') if 'Sample ID' in schedule_header else 6,
    }
    
    # Create a set of existing sample IDs in history
    existing_samples = set()
    for row in history_data[1:]:
        if len(row) >= 6 and row[5]:  # Check if Sample ID column exists and has value
            existing_samples.add(row[5])
    
    # Find new samples to add to history
    new_samples = []
    for row in schedule_rows:
        # Skip rows without enough columns
        if len(row) <= max(schedule_indices.values()):
            continue
            
        # Extract data
        khu_vuc = row[schedule_indices['khu_vuc']]
        san_pham = row[schedule_indices['san_pham']]
        line = row[schedule_indices['line']]
        chi_tieu = row[schedule_indices['chi_tieu']]
        ngay_kiem_tra = row[schedule_indices['ngay_kiem_tra']]
        sample_id = row[schedule_indices['sample_id']]
        
        # Skip if no sample ID or missing data
        if not sample_id or not khu_vuc or not san_pham or not ngay_kiem_tra:
            continue
            
        # Check if this is a new sample ID
        if sample_id not in existing_samples:
            today_str = datetime.today().strftime('%d/%m/%Y')
            new_samples.append([
                khu_vuc,
                san_pham,
                line,
                chi_tieu,
                ngay_kiem_tra,
                sample_id,
                today_str  # Current date as update date
            ])
            existing_samples.add(sample_id)  # Add to set to avoid duplicates if multiple rows have same ID
    
    # Add new samples to history sheet
    if new_samples:
        sampling_history.append_rows(new_samples)
        print(f"Đã thêm {len(new_samples)} ID mẫu mới vào lịch sử.")
    else:
        print("Không tìm thấy ID mẫu mới.")
    
    return len(new_samples)

# 6. Create a summary chart for email
def create_status_chart(due_samples):
    try:
        # Group samples by area
        area_counts = {}
        for sample in due_samples:
            area = sample['khu_vuc']
            if area in area_counts:
                area_counts[area] += 1
            else:
                area_counts[area] = 1
        
        # Create bar chart for areas
        plt.figure(figsize=(10, 6))
        areas = list(area_counts.keys())
        counts = list(area_counts.values())
        
        plt.bar(areas, counts, color='skyblue')
        plt.xlabel('Khu vực')
        plt.ylabel('Số lượng mẫu cần lấy')
        plt.title('Số lượng mẫu cần lấy theo khu vực')
        plt.xticks(rotation=45, ha='right')
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

# 7. Send email notification for due samples
def send_email_notification(due_samples):
    if not due_samples:
        print("Không có mẫu đến hạn, không gửi email.")
        return True
    
    print(f"Đang gửi email thông báo cho {len(due_samples)} mẫu đến hạn...")
    
    try:
        # Create chart
        chart_buffer = create_status_chart(due_samples)
        
        # Create email
        msg = MIMEMultipart()
        msg['Subject'] = f'Thông báo lấy mẫu QA - {datetime.today().strftime("%d/%m/%Y")}'
        msg['From'] = 'hoitkn@msc.masangroup.com'
        
        # Recipients
        recipients = [
            "hoitkn@msc.masangroup.com",
            "qatpmbmi@msc.masangroup.com",
            "ktcnnemmb@msc.masangroup.com"
        ]
        msg['To'] = ", ".join(recipients)
        
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
            </div>
            
            <h3>Danh sách mẫu cần lấy:</h3>
            <table>
                <thead>
                    <tr>
                        <th>Khu vực</th>
                        <th>Sản phẩm</th>
                        <th>Line / Xưởng</th>
                        <th>Chỉ tiêu kiểm</th>
                        <th>Tần suất (ngày)</th>
                        <th>Ngày kiểm tra gần nhất</th>
                        <th>Kế hoạch lấy mẫu tiếp theo</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Add rows for each due sample
        for sample in due_samples:
            html_content += f"""
                    <tr class="due">
                        <td>{sample['khu_vuc']}</td>
                        <td>{sample['san_pham']}</td>
                        <td>{sample['line']}</td>
                        <td>{sample['chi_tieu']}</td>
                        <td>{sample['tan_suat']}</td>
                        <td>{sample['ngay_kiem_tra']}</td>
                        <td>{sample['ke_hoach']}</td>
                    </tr>
            """
        
        html_content += """
                </tbody>
            </table>
            
            <div class="footer">
                <p>Vui lòng thực hiện lấy mẫu và cập nhật ID mẫu vào Google Sheets.</p>
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
        
        # Send email using environment variable for password
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

# 8. Main function to run everything
def run_update():
    print("Bắt đầu cập nhật lịch lấy mẫu QA...")
    
    try:
        # Update sampling schedule and get due samples
        due_samples = update_sampling_schedule()
        
        # Check for new sample IDs and update history
        update_sample_history()
        
        # Send email notification for due samples
        if due_samples:
            send_email_notification(due_samples)
        
        print("Hoàn thành cập nhật.")
        return True
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return False

# Run the update if executed directly
if __name__ == "__main__":
    run_update()
