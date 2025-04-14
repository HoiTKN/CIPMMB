import gspread
import os
import json
import time
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
# Updated to use the new spreadsheet ID
sheet_id = '1RViEQh2nFxUDEs2ztXdkiir01zRwafShwx7ZPgSJBQE'
spreadsheet = gc.open_by_key(sheet_id)

# Get or create necessary worksheets
def get_or_create_sheet(name, rows=100, cols=20):
    try:
        worksheet = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
    return worksheet

# Get the main sampling schedule sheets
hoa_ly_sheet = get_or_create_sheet('hóa lý')  # First sheet from your images
vi_sinh_sheet = get_or_create_sheet('Vi sinh')  # Second sheet from your images
summary_sheet = get_or_create_sheet('Báo cáo tổng hợp')  # New summary sheet

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

# 4. Modified function to update sampling schedule and find due samples AND track all samples
def update_sampling_schedule(worksheet, check_type="Hóa lý"):
    print(f"Đang cập nhật lịch lấy mẫu {check_type}...")
    
    # Read sampling schedule data
    schedule_data = worksheet.get_all_values()
    
    # Ensure headers exist
    if len(schedule_data) <= 1:
        print(f"Không tìm thấy dữ liệu hoặc không đủ hàng trong bảng {check_type}.")
        return [], []
    
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
    all_samples = []  # NEW: To track all samples regardless of due date
    cells_to_update = []  # For batch updating
    
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
        
        # NEW: Add to all_samples list regardless of due date
        all_samples.append({
            'khu_vuc': khu_vuc,
            'san_pham': san_pham,
            'line': line,
            'chi_tieu': chi_tieu,
            'tan_suat': tan_suat_str,
            'ngay_kiem_tra': ngay_kiem_tra,
            'sample_id': sample_id,
            'ke_hoach': next_sampling_str,
            'loai_kiem_tra': check_type,
            'row_index': i,
            'status': 'Đến hạn' if days_until_next <= 0 else 'Chưa đến hạn'
        })
        
        # Still maintain separate list for due samples
        if days_until_next <= 0:
            status = "Đến hạn"
            due_samples.append({
                'khu_vuc': khu_vuc,
                'san_pham': san_pham,
                'line': line,
                'chi_tieu': chi_tieu,
                'tan_suat': tan_suat_str,
                'ngay_kiem_tra': ngay_kiem_tra,
                'sample_id': sample_id,
                'ke_hoach': next_sampling_str,
                'loai_kiem_tra': check_type,
                'row_index': i
            })
        
        # Add to batch update instead of immediate update
        if col_indices.get('ke_hoach') is not None:
            cells_to_update.append(gspread.Cell(i, col_indices['ke_hoach'] + 1, next_sampling_str))
        
        # Store updated row data
        updated_row = list(row)
        if col_indices.get('ke_hoach') is not None and len(updated_row) > col_indices['ke_hoach']:
            updated_row[col_indices['ke_hoach']] = next_sampling_str
        updated_rows.append(updated_row)
    
    # Batch update all cells at once to reduce API calls
    if cells_to_update:
        # Split into batches of 100 cells to avoid API limits
        batch_size = 100
        for i in range(0, len(cells_to_update), batch_size):
            batch = cells_to_update[i:i+batch_size]
            worksheet.update_cells(batch)
            # Add a small delay to avoid hitting rate limits
            if i + batch_size < len(cells_to_update):
                time.sleep(1)
        
    print(f"Đã cập nhật {len(updated_rows)} mẫu kiểm tra {check_type}.")
    print(f"Có {len(due_samples)} mẫu {check_type} đến hạn cần lấy.")
    print(f"Tổng số mẫu {check_type} đã được theo dõi: {len(all_samples)}")
    
    # Return both lists - due samples and all samples
    return due_samples, all_samples

# 5. NEW: Function to update the complete samples summary report
def update_complete_summary_report(all_samples):
    print("Đang cập nhật báo cáo tổng hợp tất cả mẫu...")
    
    if not all_samples:
        print("Không có mẫu nào, không cập nhật báo cáo tổng hợp.")
        return
    
    # Define headers for complete summary sheet
    headers = ['Khu vực', 'Sản phẩm', 'Line / Xưởng', 'Chỉ tiêu kiểm', 
               'Tần suất (ngày)', 'Sample ID', 'Ngày kiểm tra', 
               'Kế hoạch lấy mẫu tiếp theo', 'Loại kiểm tra', 'Trạng thái']
    
    # Clear the summary sheet and add headers
    summary_sheet.clear()
    summary_sheet.append_row(headers)
    
    # Prepare rows for complete summary
    summary_rows = []
    for sample in all_samples:
        summary_rows.append([
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
    
    # Add rows to summary sheet in batches
    if summary_rows:
        batch_size = 100
        for i in range(0, len(summary_rows), batch_size):
            batch = summary_rows[i:i+batch_size]
            summary_sheet.append_rows(batch)
            # Add a small delay between batches
            if i + batch_size < len(summary_rows):
                time.sleep(1)
    
    print(f"Đã cập nhật {len(summary_rows)} mẫu vào báo cáo tổng hợp.")
    
    # Apply conditional formatting to highlight due samples
    try:
        summary_sheet.format("J2:J1000", {
            "backgroundColor": {
                "red": 1.0,
                "green": 0.8,
                "blue": 0.8
            }
        }, {"textFormat": {"bold": True}}, 
        condition={"type": "TEXT_EQ", "values": [{"userEnteredValue": "Đến hạn"}]})
    except Exception as e:
        print(f"Lỗi khi áp dụng định dạng có điều kiện: {str(e)}")

# 6. Create visualization charts for email (unchanged)
def create_charts(due_samples):
    try:
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

# 7. Send email notification for due samples (unchanged)
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
        
        # Updated recipient as per request
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
        
        # Add Hoa ly samples table if exists
        if hoa_ly_samples:
            html_content += f"""
            <h3>Danh sách mẫu Hóa lý cần lấy:</h3>
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
            
            for sample in hoa_ly_samples:
                html_content += f"""
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
            
            html_content += """
                </tbody>
            </table>
            """
        
        # Add Vi sinh samples table if exists
        if vi_sinh_samples:
            html_content += f"""
            <h3>Danh sách mẫu Vi sinh cần lấy:</h3>
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
            
            for sample in vi_sinh_samples:
                html_content += f"""
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
            
            html_content += """
                </tbody>
            </table>
            """
        
        html_content += """
            <div class="footer">
                <p>Vui lòng thực hiện lấy mẫu và cập nhật ID mẫu vào Google Sheets.</p>
                <p>Báo cáo tổng hợp đã được cập nhật trong bảng tính.</p>
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

# 8. Modified main function to run everything with complete sample tracking
def run_update():
    print("Bắt đầu cập nhật lịch lấy mẫu QA...")
    
    try:
        # Add exponential backoff for API rate limiting
        max_retries = 5
        retry_count = 0
        backoff_time = 2  # Starting with 2 seconds
        
        all_due_samples = []
        all_collected_samples = []  # NEW: collect all samples
        
        # Update Hoa ly sampling schedule with retry logic
        while retry_count < max_retries:
            try:
                hoa_ly_due, hoa_ly_all = update_sampling_schedule(hoa_ly_sheet, "Hóa lý")
                all_due_samples.extend(hoa_ly_due)
                all_collected_samples.extend(hoa_ly_all)  # Add all Hoa ly samples
                break  # Exit the retry loop if successful
            except gspread.exceptions.APIError as e:
                if "429" in str(e) and retry_count < max_retries - 1:  # Rate limiting error
                    retry_count += 1
                    wait_time = backoff_time * (2 ** retry_count)  # Exponential backoff
                    print(f"API rate limit hit. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    raise  # Re-raise the exception if it's not a rate limit error or we've exceeded retries
        
        # Add delay between operations to avoid rate limits
        time.sleep(5)
        
        # Update Vi sinh sampling schedule
        retry_count = 0
        while retry_count < max_retries:
            try:
                vi_sinh_due, vi_sinh_all = update_sampling_schedule(vi_sinh_sheet, "Vi sinh")
                all_due_samples.extend(vi_sinh_due)
                all_collected_samples.extend(vi_sinh_all)  # Add all Vi sinh samples
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e) and retry_count < max_retries - 1:
                    retry_count += 1
                    wait_time = backoff_time * (2 ** retry_count)
                    print(f"API rate limit hit. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    raise
        
        # Add delay before updating summary
        time.sleep(5)
        
        # Update complete summary report with all samples
        retry_count = 0
        while retry_count < max_retries:
            try:
                update_complete_summary_report(all_collected_samples)
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e) and retry_count < max_retries - 1:
                    retry_count += 1
                    wait_time = backoff_time * (2 ** retry_count)
                    print(f"API rate limit hit. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    raise
        
        # Send email notification for due samples (unchanged)
        if all_due_samples:
            send_email_notification(all_due_samples)
        
        print("Hoàn thành cập nhật.")
        return True
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return False

# Run the update if executed directly
if __name__ == "__main__":
    run_update()
