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

# 2. Open Google Sheet with updated structure (3 sheets now)
sheet_id = '1j8il_-mIGczDX-3eRYNP3jB2Jjo7FLH0DwyaJN6zia0'
spreadsheet = gc.open_by_key(sheet_id)

# Get or create necessary worksheets - now adding Actual Result sheet
def get_or_create_sheet(name, rows=100, cols=20):
    try:
        worksheet = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
    return worksheet

# We need Master plan, Cleaning History, and Actual Result
master_plan = get_or_create_sheet('Master plan')
cleaning_history = get_or_create_sheet('Cleaning History')
actual_result = get_or_create_sheet('Actual result')

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
                'Ngày vệ sinh gần nhất', 'Ngày kế hoạch vệ sinh tiếp theo', 'Trạng thái', 'Đang chứa sản phẩm']
        master_plan.update_cells(
            cells=[
                gspread.Cell(1, i+1, header)
                for i, header in enumerate(headers)
            ]
        )
        master_data = [headers]
    
    # Check if headers need to be updated with the new column
    if len(master_data[0]) < 8 or master_data[0][7] != 'Đang chứa sản phẩm':
        master_data[0].append('Đang chứa sản phẩm')
        master_plan.update_cell(1, 8, 'Đang chứa sản phẩm')
    
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
    
    # Check if Actual Result has headers
    actual_data = actual_result.get_all_values()
    if not actual_data:
        actual_headers = ['Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)', 
                           'Ngày vệ sinh', 'Người thực hiện', 'Kết quả', 'Ghi chú']
        actual_result.update_cells(
            cells=[
                gspread.Cell(1, i+1, header)
                for i, header in enumerate(actual_headers)
            ]
        )
    
    # Process Master plan data
    header = master_data[0]
    rows = master_data[1:] if len(master_data) > 1 else []
    
    today = datetime.today()
    updated_values = []
    
    # Process each row and calculate status
    for i, row in enumerate(rows):
        # Ensure each row has 8 columns (including the new "Đang chứa sản phẩm" column)
        if len(row) >= 8:
            area, device, method, freq_str, last_cleaning, next_plan, status, has_product = row[:8]
        elif len(row) == 7:
            area, device, method, freq_str, last_cleaning, next_plan, status = row[:7]
            has_product = ""  # Default empty for the new column
        else:
            # If fewer columns, pad with empty strings
            padded_row = row + [''] * (8 - len(row))
            area, device, method, freq_str, last_cleaning, next_plan, status, has_product = padded_row
    
        if not last_cleaning:
            updated_values.append([area, device, method, freq_str, last_cleaning, "", "Chưa có dữ liệu", has_product])
            continue
    
        freq = 0
        if freq_str:
            try:
                freq = int(freq_str)
            except ValueError:
                freq = 0
    
        last_cleaning_date = parse_date(last_cleaning)
        if not last_cleaning_date:
            updated_values.append([area, device, method, freq_str, last_cleaning, "", "Định dạng ngày không hợp lệ", has_product])
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
    
        updated_values.append([area, device, method, freq_str, last_cleaning, next_plan_str, current_status, has_product])
    
    # Add this code at the end of your update_cleaning_schedule() function
# Just before the return updated_values statement

def update_cleaning_schedule():
    # ... [existing code remains unchanged] ...
    
    # Update Master plan
    if updated_values:
        # Use update_cells method to avoid deprecated warnings
        cells_to_update = []
        for row_idx, row_data in enumerate(updated_values):
            for col_idx, cell_value in enumerate(row_data):
                cells_to_update.append(gspread.Cell(row_idx+2, col_idx+1, cell_value))
        
        master_plan.update_cells(cells_to_update)
    
    print(f"Đã cập nhật {len(updated_values)} thiết bị.")
    
    # NEW CODE: Update Actual Result with new cleaning records
    # -------------------------------------------------------------
    print("Kiểm tra và cập nhật bản ghi vệ sinh mới...")
    
    # Read existing records from Actual Result
    actual_data = actual_result.get_all_values()
    existing_records = set()  # Set of unique cleaning records (device + date)
    
    # Skip header row
    for row in actual_data[1:]:
        if len(row) >= 5:  # Ensure row has enough columns
            device_name = row[1]  # Device column
            cleaning_date_str = row[4]  # Cleaning date column
            if device_name and cleaning_date_str:
                # Create unique key for existing record
                record_key = f"{device_name}_{cleaning_date_str}"
                existing_records.add(record_key)
    
    # Identify new cleaning records from Master plan
    new_cleaning_records = []
    
    for row in updated_values:
        area, device, method, freq_str, last_cleaning, next_plan_str, status, has_product = row
        
        # Skip if no cleaning date or format is invalid
        if not last_cleaning or "không hợp lệ" in status.lower() or "chưa có dữ liệu" in status.lower():
            continue
            
        # Create unique key for this cleaning record
        record_key = f"{device}_{last_cleaning}"
        
        # Add to Actual Result if not already recorded
        if record_key not in existing_records:
            # Default values for new records
            person = "Tự động"  # Placeholder or default person
            result = "Đạt"      # Default result
            notes = ""          # Empty notes
            
            # Add new cleaning record
            new_cleaning_records.append([
                area,
                device,
                method,
                freq_str,
                last_cleaning,
                person,
                result,
                notes
            ])
            
            # Mark as processed to avoid duplicates
            existing_records.add(record_key)
    
    # Add new cleaning records to Actual Result sheet
    if new_cleaning_records:
        actual_result.append_rows(new_cleaning_records)
        print(f"Đã thêm {len(new_cleaning_records)} bản ghi vệ sinh mới vào Actual Result")
    else:
        print("Không có bản ghi vệ sinh mới để thêm vào Actual Result")
    
    return updated_values

# 5. Function to add a new cleaning record
def add_cleaning_record(area, device, method, freq, cleaning_date, person, result="Đạt", notes=""):
    """
    Add a new cleaning record and update Master plan and Actual Result
    
    Parameters:
    area (str): Area name
    device (str): Device name
    method (str): Cleaning method
    freq (str): Frequency in days
    cleaning_date (str): Cleaning date (any format)
    person (str): Person who performed the cleaning
    result (str, optional): Cleaning result (default: "Đạt")
    notes (str, optional): Additional notes (default: "")
    """
    try:
        # Format the date consistently
        cleaned_date = parse_date(cleaning_date)
        if not cleaned_date:
            return f"Lỗi: Định dạng ngày '{cleaning_date}' không hợp lệ"
            
        formatted_date = cleaned_date.strftime('%d/%m/%Y')
        
        # Add new record to Cleaning History
        cleaning_history.append_row([area, device, method, freq, formatted_date, person])
        
        # Add new record to Actual Result sheet
        actual_result.append_row([area, device, method, freq, formatted_date, person, result, notes])
        
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
                    
                    # Update product status - Empty the "Đang chứa sản phẩm" field when cleaning is completed
                    master_plan.update_cell(i, 8, "")
                    
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
                
                # Add new row to Master plan with empty product status
                master_plan.append_row([
                    area, device, method, freq, 
                    formatted_date, next_date_str, current_status, ""
                ])
                
            except ValueError:
                # If frequency is invalid, add with empty next date and status
                master_plan.append_row([
                    area, device, method, freq, 
                    formatted_date, "", "Tần suất không hợp lệ", ""
                ])
        
        message = f"Đã thêm bản ghi vệ sinh cho thiết bị {device}"
        print(message)
        return "Thành công"
        
    except Exception as e:
        error_message = f"Lỗi khi thêm bản ghi vệ sinh: {str(e)}"
        print(error_message)
        return "Lỗi"

# 6. Function to update actual cleaning results for existing records
def update_cleaning_result(device, cleaning_date, result, notes=""):
    """
    Update the result of a cleaning record in the Actual Result sheet
    
    Parameters:
    device (str): Device name
    cleaning_date (str): Cleaning date to identify the record
    result (str): Cleaning result ("Đạt" or "Không đạt")
    notes (str, optional): Additional notes (default: "")
    
    Returns:
    str: Status message
    """
    try:
        # Get all records from Actual Result sheet
        actual_data = actual_result.get_all_values()
        if len(actual_data) <= 1:  # Only header row or empty
            return "Không tìm thấy bản ghi"
            
        # Format the date for comparison
        target_date = parse_date(cleaning_date)
        if not target_date:
            return f"Lỗi: Định dạng ngày '{cleaning_date}' không hợp lệ"
            
        target_date_str = target_date.strftime('%d/%m/%Y')
        
        # Search for the matching record
        record_found = False
        for i, row in enumerate(actual_data[1:], start=2):
            if len(row) >= 6:  # Ensure row has enough columns
                row_device = row[1]
                row_date = row[4]
                
                # Check if device and date match
                if row_device == device and row_date == target_date_str:
                    # Update result and notes (columns 7 and 8)
                    actual_result.update_cell(i, 7, result)
                    actual_result.update_cell(i, 8, notes)
                    record_found = True
                    break
        
        if record_found:
            message = f"Đã cập nhật kết quả vệ sinh cho thiết bị {device} (ngày {target_date_str})"
            print(message)
            return "Thành công"
        else:
            message = f"Không tìm thấy bản ghi vệ sinh cho thiết bị {device} (ngày {target_date_str})"
            print(message)
            return "Không tìm thấy"
            
    except Exception as e:
        error_message = f"Lỗi khi cập nhật kết quả vệ sinh: {str(e)}"
        print(error_message)
        return "Lỗi"

# 7. Function to update product status for a device
def update_product_status(device, has_product):
    """
    Update the product status for a device in the Master plan
    
    Parameters:
    device (str): Device name
    has_product (str): Product status information
    
    Returns:
    str: Status message
    """
    try:
        # Get all records from Master plan
        master_data = master_plan.get_all_values()
        if len(master_data) <= 1:  # Only header row or empty
            return "Không tìm thấy thiết bị"
        
        # Search for the matching device
        device_found = False
        for i, row in enumerate(master_data[1:], start=2):
            if len(row) >= 2 and row[1] == device:
                # Update product status (column 8)
                master_plan.update_cell(i, 8, has_product)
                device_found = True
                break
        
        if device_found:
            message = f"Đã cập nhật trạng thái sản phẩm cho thiết bị {device}"
            print(message)
            return "Thành công"
        else:
            message = f"Không tìm thấy thiết bị {device}"
            print(message)
            return "Không tìm thấy"
            
    except Exception as e:
        error_message = f"Lỗi khi cập nhật trạng thái sản phẩm: {str(e)}"
        print(error_message)
        return "Lỗi"

# 8. Function to create a simple dashboard chart for email, now with product status
def create_status_chart(updated_values):
    try:
        # Create DataFrame for visualization
        df = pd.DataFrame(updated_values, columns=[
            'Khu vực', 'Thiết bị', 'Phương pháp', 'Tần suất (ngày)',
            'Ngày vệ sinh gần nhất', 'Ngày kế hoạch vệ sinh tiếp theo', 'Trạng thái', 'Đang chứa sản phẩm'
        ])
        
        # Set up figure with 2 subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # First subplot: Count statuses
        status_counts = df['Trạng thái'].value_counts()
        status_order = ['Bình thường', 'Sắp đến hạn', 'Đến hạn', 'Quá hạn']
        
        # Create a Series with all possible statuses and fill missing with 0
        status_data = pd.Series([0, 0, 0, 0], index=status_order)
        
        # Update with actual counts
        for status, count in status_counts.items():
            if status in status_data.index:
                status_data[status] = count
        
        # Create a bar chart for cleaning status
        colors = ['green', 'yellow', 'orange', 'red']
        ax1.bar(status_data.index, status_data.values, color=colors)
        ax1.set_title('Thống kê trạng thái thiết bị vệ sinh')
        ax1.set_ylabel('Số lượng')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Second subplot: Count product status for overdue equipment
        overdue_df = df[df['Trạng thái'].isin(['Đến hạn', 'Quá hạn'])]
        
        # Count devices with/without product
        product_status = overdue_df['Đang chứa sản phẩm'].fillna('Trống').map(lambda x: 'Có sản phẩm' if x.strip() else 'Trống')
        product_counts = product_status.value_counts()
        
        # Ensure both categories are present
        product_data = pd.Series([0, 0], index=['Có sản phẩm', 'Trống'])
        for status, count in product_counts.items():
            product_data[status] = count
        
        # Create a pie chart for product status
        ax2.pie(
            product_data.values,
            labels=product_data.index,
            colors=['red', 'green'],
            autopct='%1.1f%%',
            startangle=90
        )
        ax2.set_title('Trạng thái sản phẩm của thiết bị cần vệ sinh')
        ax2.axis('equal')
        
        plt.tight_layout()
        
        # Save chart for email
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=100)
        img_buffer.seek(0)
        
        plt.close()  # Close the plot to avoid warnings
        return img_buffer
    
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ: {str(e)}")
        return None

# 9. Function to create a results analysis chart for email
def create_results_chart():
    try:
        # Get data from Actual Result sheet
        actual_data = actual_result.get_all_values()
        if len(actual_data) <= 1:  # Only header or empty
            return None
            
        # Create DataFrame for analysis
        df = pd.DataFrame(actual_data[1:], columns=actual_data[0])
        
        # Count results
        if 'Kết quả' in df.columns:
            result_counts = df['Kết quả'].value_counts()
            
            # Create a pie chart of results
            plt.figure(figsize=(8, 8))
            colors = ['green', 'red', 'gray']
            
            plt.pie(
                result_counts.values,
                labels=result_counts.index,
                colors=colors[:len(result_counts)],
                autopct='%1.1f%%',
                startangle=140
            )
            plt.axis('equal')
            plt.title('Phân tích kết quả vệ sinh thiết bị')
            
            # Save chart for email
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=100)
            img_buffer.seek(0)
            
            plt.close()  # Close the plot to avoid warnings
            return img_buffer
        else:
            return None
    
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ kết quả: {str(e)}")
        return None

# 10. Enhanced function to send email report with product status information
def send_email_report(updated_values):
    print("Đang chuẩn bị gửi email báo cáo...")
    
    # Filter devices requiring attention
    due_rows = [row for row in updated_values if row[6] in ['Đến hạn', 'Quá hạn']]
    
    if due_rows:
        try:
            # Create charts
            status_img_buffer = create_status_chart(updated_values)
            results_img_buffer = create_results_chart()
            
            # Create email
            msg = MIMEMultipart()
            msg['Subject'] = f'Báo cáo vệ sinh thiết bị - {datetime.today().strftime("%d/%m/%Y")}'
            msg['From'] = 'hoitkn@msc.masangroup.com'
            
            recipients = ["hoitkn@msc.masangroup.com", "mmb-ktcncsd@msc.masangroup.com","haont1@msc.masangroup.com","datnd@msc.masangroup.com","chungnt2@msc.masangroup.com","luannt4@msc.masangroup.com","quangnd2@msc.masangroup.com"]
            msg['To'] = ", ".join(recipients)
            
            # Prepare data for email summary
            empty_tanks = [row for row in due_rows if not row[7].strip()]
            filled_tanks = [row for row in due_rows if row[7].strip()]
            
            # HTML content with product status in a single table
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
                    .has-product {{ color: #cc0000; font-weight: bold; }}
                    .empty {{ color: #009900; }}
                    h2 {{ color: #003366; }}
                    h3 {{ color: #004d99; margin-top: 25px; }}
                    .summary {{ margin: 20px 0; }}
                    .footer {{ margin-top: 30px; font-size: 0.9em; color: #666; }}
                </style>
            </head>
            <body>
                <h2>Báo cáo vệ sinh thiết bị - {datetime.today().strftime("%d/%m/%Y")}</h2>
                
                <div class="summary">
                    <p><strong>Tổng số thiết bị:</strong> {len(updated_values)}</p>
                    <p><strong>Thiết bị cần vệ sinh:</strong> {len(due_rows)}</p>
                    <p><strong>Thiết bị trống có thể vệ sinh ngay:</strong> {len(empty_tanks)}</p>
                    <p><strong>Thiết bị đang chứa sản phẩm cần lên kế hoạch:</strong> {len(filled_tanks)}</p>
                </div>
                
                <h3>Danh sách thiết bị cần vệ sinh:</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Khu vực</th>
                            <th>Thiết bị</th>
                            <th>Phương pháp</th>
                            <th>Tần suất (ngày)</th>
                            <th>Ngày vệ sinh gần nhất (KQ)</th>
                            <th>Ngày kế hoạch vệ sinh tiếp theo (KH)</th>
                            <th>Trạng thái</th>
                            <th>Đang chứa sản phẩm</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            # Add all tanks to the table (both empty and with product)
            # Sort the rows to prioritize empty tanks first
            sorted_rows = sorted(due_rows, key=lambda row: 1 if row[7].strip() else 0)
            
            for row in sorted_rows:
                area, device, method, freq_str, last_cleaning, next_plan_str, status, has_product = row
                
                # Define CSS class based on status
                css_class = ""
                if status == "Quá hạn":
                    css_class = "overdue"
                elif status == "Đến hạn":
                    css_class = "due-today"
                
                # Define product status class
                product_class = "has-product" if has_product.strip() else "empty"
                
                html_content += f"""
                        <tr class="{css_class}">
                            <td>{area}</td>
                            <td>{device}</td>
                            <td>{method}</td>
                            <td>{freq_str}</td>
                            <td>{last_cleaning}</td>
                            <td>{next_plan_str}</td>
                            <td>{status}</td>
                            <td class="{product_class}">{has_product}</td>
                        </tr>
                """
            
            html_content += """
                    </tbody>
                </table>
                
                <div class="footer">
                    <p>Vui lòng xem Google Sheets để biết chi tiết và cập nhật trạng thái của các thiết bị.</p>
                    <p>Email này được tự động tạo bởi hệ thống. Vui lòng không trả lời.</p>
                </div>
            </body>
            </html>
            """
            
            # Attach HTML
            msg.attach(MIMEText(html_content, "html", "utf-8"))
            
            # Attach status chart if available
            if status_img_buffer:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(status_img_buffer.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="cleaning_status.png"')
                msg.attach(part)
                
            # Attach results chart if available
            if results_img_buffer:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(results_img_buffer.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="cleaning_results.png"')
                msg.attach(part)
            
            # Send email using environment variable for password
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
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

# 11. Main function to run everything
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
