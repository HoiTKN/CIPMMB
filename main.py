import gspread
import os
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

# 1. Authentication setup
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = None
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

gc = gspread.authorize(creds)

# 2. Open Google Sheet with simplified structure (just 2 sheets)
sheet_id = '1j8il_-mIGczDX-3eRYNP3jB2Jjo7FLH0DwyaJN6zia0'
spreadsheet = gc.open_by_key(sheet_id)

# Get or create necessary worksheets - simplified to only 2 sheets
def get_or_create_sheet(name, rows=100, cols=20):
    try:
        worksheet = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
    return worksheet

# We only need Master plan and Cleaning History
master_plan = get_or_create_sheet('Master plan')
cleaning_history = get_or_create_sheet('Cleaning History')

# 3. Helper function to parse dates in different formats
def parse_date(date_str):
    """Try to parse date with multiple formats"""
    if not date_str:
        return None
        
    date_formats = ['%B %d, %Y', '%d/%m/%Y', '%Y-%m-%d']
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

# 4. Main function to update cleaning schedule
def update_cleaning_schedule():
    print("Đang cập nhật lịch vệ sinh...")
    
    # Read Master plan data
    master_data = master_plan.get_all_values()
    
    # Check and prepare header if needed
    if not master_data:
        headers = ['Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)', 
                'Ngày vệ sinh gần nhất', 'Ngày kế hoạch vệ sinh tiếp theo', 'Trạng thái']
        master_plan.update_cells(
            cells=[
                gspread.Cell(1, i+1, header)
                for i, header in enumerate(headers)
            ]
        )
        master_data = [headers]
    
    # Check if Cleaning History has headers
    history_data = cleaning_history.get_all_values()
    if not history_data:
        history_headers = ['Khu vực', 'Thiết bị', 'Phương pháp', 
                        'Tần suất (ngày)', 'Ngày vệ sinh', 'Người thực hiện']
        cleaning_history.update_cells(
            cells=[
                gspread.Cell(1, i+1, header)
                for i, header in enumerate(history_headers)
            ]
        )
    
    # Process Master plan data
    header = master_data[0]
    rows = master_data[1:] if len(master_data) > 1 else []
    
    today = datetime.today()
    updated_values = []
    
    # Process each row and calculate status
    for i, row in enumerate(rows):
        # Ensure each row has 7 columns
        if len(row) >= 7:
            area, device, method, freq_str, last_cleaning, next_plan, status = row[:7]
        else:
            # If fewer columns, pad with empty strings
            padded_row = row + [''] * (7 - len(row))
            area, device, method, freq_str, last_cleaning, next_plan, status = padded_row
    
        if not last_cleaning:
            updated_values.append([area, device, method, freq_str, last_cleaning, "", "Chưa có dữ liệu"])
            continue
    
        freq = 0
        if freq_str:
            try:
                freq = int(freq_str)
            except ValueError:
                freq = 0
    
        last_cleaning_date = parse_date(last_cleaning)
        if not last_cleaning_date:
            updated_values.append([area, device, method, freq_str, last_cleaning, "", "Định dạng ngày không hợp lệ"])
            continue
    
        next_plan_date = last_cleaning_date + timedelta(days=freq)
        next_plan_str = next_plan_date.strftime('%d/%m/%Y')
    
        days_until_next = (next_plan_date.date() - today.date()).days
        
        if days_until_next > 7:
            current_status = 'Bình thường'
        elif days_until_next > 0:
            current_status = 'Sắp đến hạn'
        elif days_until_next == 0:
            current_status = 'Đến hạn'
        else:
            current_status = 'Quá hạn'
    
        updated_values.append([area, device, method, freq_str, last_cleaning, next_plan_str, current_status])
    
    # Update Master plan
    if updated_values:
        # Use update_cells method to avoid deprecated warnings
        cells_to_update = []
        for row_idx, row_data in enumerate(updated_values):
            for col_idx, cell_value in enumerate(row_data):
                cells_to_update.append(gspread.Cell(row_idx+2, col_idx+1, cell_value))
        
        master_plan.update_cells(cells_to_update)
    
    print(f"Đã cập nhật {len(updated_values)} thiết bị.")
    return updated_values

# 5. Function to add a new cleaning record
def add_cleaning_record(area, device, method, freq, cleaning_date, person):
    """
    Add a new cleaning record and update Master plan
    
    Parameters:
    area (str): Area name
    device (str): Device name
    method (str): Cleaning method
    freq (str): Frequency in days
    cleaning_date (str): Cleaning date (any format)
    person (str): Person who performed the cleaning
    """
    try:
        # Format the date consistently
        cleaned_date = parse_date(cleaning_date)
        if not cleaned_date:
            return f"Lỗi: Định dạng ngày '{cleaning_date}' không hợp lệ"
            
        formatted_date = cleaned_date.strftime('%d/%m/%Y')
        
        # Add new record to Cleaning History
        cleaning_history.append_row([area, device, method, freq, formatted_date, person])
        
        # Update Master plan with the new cleaning date
        master_data = master_plan.get_all_values()
        device_found = False
        
        for i, row in enumerate(master_data[1:], start=2):
            if len(row) >= 2 and row[1] == device:
                # Update last cleaning date
                master_plan.update_cell(i, 5, formatted_date)
                device_found = True
                
                # Calculate and update next cleaning date
                try:
                    freq_days = int(freq)
                    next_date = cleaned_date + timedelta(days=freq_days)
                    next_date_str = next_date.strftime('%d/%m/%Y')
                    master_plan.update_cell(i, 6, next_date_str)
                    
                    # Update status
                    days_until_next = (next_date.date() - datetime.today().date()).days
                    
                    if days_until_next > 7:
                        current_status = 'Bình thường'
                    elif days_until_next > 0:
                        current_status = 'Sắp đến hạn'
                    elif days_until_next == 0:
                        current_status = 'Đến hạn'
                    else:
                        current_status = 'Quá hạn'
                        
                    master_plan.update_cell(i, 7, current_status)
                except ValueError:
                    print(f"Cảnh báo: Không thể tính ngày vệ sinh tiếp theo (tần suất không hợp lệ)")
                
                break
        
        # If device not found in Master plan, add it
        if not device_found:
            try:
                freq_days = int(freq)
                next_date = cleaned_date + timedelta(days=freq_days)
                next_date_str = next_date.strftime('%d/%m/%Y')
                
                days_until_next = (next_date.date() - datetime.today().date()).days
                
                if days_until_next > 7:
                    current_status = 'Bình thường'
                elif days_until_next > 0:
                    current_status = 'Sắp đến hạn'
                elif days_until_next == 0:
                    current_status = 'Đến hạn'
                else:
                    current_status = 'Quá hạn'
                
                # Add new row to Master plan
                master_plan.append_row([
                    area, device, method, freq, 
                    formatted_date, next_date_str, current_status
                ])
                
            except ValueError:
                # If frequency is invalid, add with empty next date and status
                master_plan.append_row([
                    area, device, method, freq, 
                    formatted_date, "", "Tần suất không hợp lệ"
                ])
        
        message = f"Đã thêm bản ghi vệ sinh cho thiết bị {device}"
        print(message)
        return "Thành công"
        
    except Exception as e:
        error_message = f"Lỗi khi thêm bản ghi vệ sinh: {str(e)}"
        print(error_message)
        return "Lỗi"

# 6. Function to create a simple dashboard chart for email
def create_status_chart(updated_values):
    try:
        # Create DataFrame for visualization
        df = pd.DataFrame(updated_values, columns=[
            'Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)',
            'Ngày vệ sinh gần nhất', 'Ngày kế hoạch vệ sinh tiếp theo', 'Trạng thái'
        ])
        
        # Count statuses
        status_counts = df['Trạng thái'].value_counts()
        status_order = ['Bình thường', 'Sắp đến hạn', 'Đến hạn', 'Quá hạn']
        
        # Create a Series with all possible statuses and fill missing with 0
        status_data = pd.Series([0, 0, 0, 0], index=status_order)
        
        # Update with actual counts
        for status, count in status_counts.items():
            if status in status_data.index:
                status_data[status] = count
        
        # Create a simple bar chart
        plt.figure(figsize=(10, 6))
        colors = ['green', 'yellow', 'orange', 'red']
        plt.bar(status_data.index, status_data.values, color=colors)
        plt.title('Thống kê trạng thái thiết bị vệ sinh')
        plt.ylabel('Số lượng')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Save chart for email
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=100)
        img_buffer.seek(0)
        
        plt.close()  # Close the plot to avoid warnings
        return img_buffer
    
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ: {str(e)}")
        return None

# 7. Function to send email report
def send_email_report(updated_values):
    print("Đang chuẩn bị gửi email báo cáo...")
    
    # Filter devices requiring attention
    due_rows = [row for row in updated_values if row[6] in ['Đến hạn', 'Quá hạn']]
    
    if due_rows:
        try:
            # Create chart
            img_buffer = create_status_chart(updated_values)
            
            # Create email
            msg = MIMEMultipart()
            msg['Subject'] = f'Báo cáo vệ sinh thiết bị - {datetime.today().strftime("%d/%m/%Y")}'
            msg['From'] = 'hoitkn@msc.masangroup.com'
            
            recipients = ["hoitkn@msc.masangroup.com", "mmb-ktcncsd@msc.masangroup.com"]
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
                    .overdue {{ background-color: #ffcccc; }}
                    .due-today {{ background-color: #ffeb99; }}
                    .due-soon {{ background-color: #e6ffcc; }}
                    h2 {{ color: #003366; }}
                    .summary {{ margin: 20px 0; }}
                    .footer {{ margin-top: 30px; font-size: 0.9em; color: #666; }}
                </style>
            </head>
            <body>
                <h2>Báo cáo vệ sinh thiết bị - {datetime.today().strftime("%d/%m/%Y")}</h2>
                
                <div class="summary">
                    <p><strong>Tổng số thiết bị:</strong> {len(updated_values)}</p>
                    <p><strong>Thiết bị cần vệ sinh:</strong> {len(due_rows)}</p>
                </div>
                
                <h3>Danh sách thiết bị cần vệ sinh:</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Khu vực</th>
                            <th>Thiết bị</th>
                            <th>Phương pháp</th>
                            <th>Tần suất (ngày)</th>
                            <th>Ngày vệ sinh gần nhất</th>
                            <th>Ngày kế hoạch vệ sinh</th>
                            <th>Trạng thái</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for row in due_rows:
                area, device, method, freq_str, last_cleaning, next_plan_str, status = row
                
                # Define CSS class based on status
                css_class = ""
                if status == "Quá hạn":
                    css_class = "overdue"
                elif status == "Đến hạn":
                    css_class = "due-today"
                    
                html_content += f"""
                        <tr class="{css_class}">
                            <td>{area}</td>
                            <td>{device}</td>
                            <td>{method}</td>
                            <td>{freq_str}</td>
                            <td>{last_cleaning}</td>
                            <td>{next_plan_str}</td>
                            <td>{status}</td>
                        </tr>
                """
                
            html_content += """
                    </tbody>
                </table>
                
                <div class="footer">
                    <p>Vui lòng xem Google Sheets để biết chi tiết.</p>
                    <p>Email này được tự động tạo bởi hệ thống. Vui lòng không trả lời.</p>
                </div>
            </body>
            </html>
            """
            
            # Attach HTML
            msg.attach(MIMEText(html_content, "html", "utf-8"))
            
            # Attach chart if available
            if img_buffer:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(img_buffer.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="cleaning_status.png"')
                msg.attach(part)
            
            # Send email
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
               import os
email_password = os.environ.get('EMAIL_PASSWORD')
server.login("hoitkn@msc.masangroup.com", email_password)
                server.send_message(msg)
                
            print("Email đã được gửi kèm bảng HTML và biểu đồ.")
            return True
            
        except Exception as e:
            print(f"Lỗi khi gửi email: {str(e)}")
            return False
    else:
        print("Không có thiết bị đến hạn/quá hạn, không gửi email.")
        return True

# 8. Main function to run everything
def run_update():
    print("Bắt đầu cập nhật hệ thống vệ sinh thiết bị...")
    
    try:
        # Update cleaning schedule and get updated values
        updated_values = update_cleaning_schedule()
        
        # Send email report
        send_email_report(updated_values)
        
        print("Hoàn thành cập nhật.")
        return True
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return False

# Run the update if executed directly
if __name__ == "__main__":
    run_update()

# Example: How to add a cleaning record
# add_cleaning_record("Khu vực Cốt", "Bồn A1", "CIP 1", "60", "2025-02-25", "Nguyen Van A")
