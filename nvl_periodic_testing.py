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

# 1. Authentication setup - reusing from main.py
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

# Helper function to parse dates in different formats
def parse_date(date_str):
    """Try to parse date with multiple formats"""
    if not date_str or date_str.strip() == "":
        return None
    
    # Clean up the date string
    date_str = date_str.strip()
    
    # First try with two-digit year formats
    date_formats_short = ['%d/%m/%y', '%d-%m-%y']
    for fmt in date_formats_short:
        try:
            date = datetime.strptime(date_str, fmt)
            # Adjust years for two-digit format (assuming 21st century for now)
            if date.year < 100:
                if date.year < 30:  # Adjust this threshold as needed
                    date = date.replace(year=date.year + 2000)
                else:
                    date = date.replace(year=date.year + 1900)
            return date
        except ValueError:
            continue
            
    # Then try with four-digit year formats
    date_formats_long = ['%d/%m/%Y', '%Y-%m-%d', '%B %d, %Y', '%d-%m-%Y']
    for fmt in date_formats_long:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
            
    print(f"Could not parse date: {date_str}")
    return None

# 2. Function to update periodic testing dates
def update_periodic_testing_dates():
    print("Updating raw material periodic testing dates...")
    
    try:
        # Initialize Google Sheets client
        gc = authenticate()
        
        # Open Google Sheet
        sheet_id = '18ayNvfnUkjuqk_vWojK0EFNoVNp4zwd7IPVpWYcYTPw'
        spreadsheet = gc.open_by_key(sheet_id)
        
        # Select the "Master data" worksheet
        try:
            worksheet = spreadsheet.worksheet('Master data')
        except gspread.exceptions.WorksheetNotFound:
            print("Error: 'Master data' worksheet not found.")
            return None
        
        # Get all values from the worksheet
        all_data = worksheet.get_all_values()
        if not all_data:
            print("No data found in worksheet.")
            return None
        
        # Extract headers and data
        headers = all_data[0]
        data_rows = all_data[1:]
        
        # Find column indices
        try:
            periodic_test_col_idx = headers.index('Ngày kiểm định kỳ')
            test_expiry_col_idx = headers.index('Thời hạn KĐK')
        except ValueError as e:
            print(f"Error finding required columns: {e}")
            return None
            
        # Required columns for the report
        required_cols = [
            'MPO Phụ Trách', 'Ngành', 'Item', 'Tên NVL', 
            'Nhà cung cấp', 'Mã NCC', 'Nhà sản xuất', 
            'Số hồ sơ công bố', 'Ngày kiểm định kỳ', 'Thời hạn KĐK'
        ]
        
        required_cols_idx = []
        for col in required_cols:
            try:
                required_cols_idx.append(headers.index(col))
            except ValueError:
                print(f"Warning: Column '{col}' not found, will be skipped in report.")
                required_cols_idx.append(-1)
        
        # Current date for comparison
        today = datetime.today()
        upcoming_expiry_threshold = today + timedelta(days=7)
        
        # Lists to store rows with different statuses
        rows_to_update = []  # Rows where we need to calculate Thời hạn KĐK
        rows_expiring_soon = []  # Rows expiring within 7 days
        rows_expired = []  # Rows already expired
        rows_missing_test_date = []  # Rows missing test date
        
        # Process each row
        for row_idx, row in enumerate(data_rows, start=2):  # Start from 2 because row 1 is headers
            # Check if the row has enough columns - expand row with empty strings if needed
            while len(row) <= max(periodic_test_col_idx, test_expiry_col_idx):
                row.append("")
                
            # Check if we have test date data
            periodic_test_date_str = row[periodic_test_col_idx].strip() if periodic_test_col_idx < len(row) else ""
            test_expiry_date_str = row[test_expiry_col_idx].strip() if test_expiry_col_idx < len(row) else ""
            
            # Extract required column values for this row regardless of test date
            row_data = {}
            for i, col_idx in enumerate(required_cols_idx):
                if col_idx >= 0 and col_idx < len(row):
                    row_data[required_cols[i]] = row[col_idx]
                else:
                    row_data[required_cols[i]] = ""
            
            # Check if this row is missing test date data but has other important data
            if not periodic_test_date_str and any([
                row_data.get('Item', '').strip(),  # Has Item code
                row_data.get('Tên NVL', '').strip(),  # Has material name
            ]):
                # Add to missing test date list
                rows_missing_test_date.append(row_data)
                continue  # Skip further processing of this row
                
            # Skip rows with no test date AND no important data
            if not periodic_test_date_str:
                continue
            
            # Handle multiple dates in the same cell
            # Split by common delimiters (newline, space, comma)
            periodic_test_dates = [date.strip() for date in periodic_test_date_str.replace('\n', ' ').replace(',', ' ').split()]
            test_expiry_dates = [date.strip() for date in test_expiry_date_str.replace('\n', ' ').replace(',', ' ').split()]
            
            # Process each date separately
            for date_idx, periodic_date_str in enumerate(periodic_test_dates):
                periodic_test_date = parse_date(periodic_date_str)
                if not periodic_test_date:
                    print(f"Row {row_idx}: Invalid periodic test date format: '{periodic_date_str}'")
                    continue
                
                # Check if we have a matching expiry date
                test_expiry_date = None
                test_expiry_date_str_current = ""
                
                if date_idx < len(test_expiry_dates):
                    test_expiry_date_str_current = test_expiry_dates[date_idx]
                    test_expiry_date = parse_date(test_expiry_date_str_current)
                
                # Calculate expiry date if not provided - 1 year after test date
                if (not test_expiry_date_str_current or not test_expiry_date) and periodic_test_date:
                    expiry_date = periodic_test_date + timedelta(days=365)
                    expiry_date_str = expiry_date.strftime('%d/%m/%Y')
                    
                    # For the first date only, update the cell if it's empty
                    if date_idx == 0 and not test_expiry_date_str:
                        rows_to_update.append((row_idx, test_expiry_col_idx + 1, expiry_date_str))
                    
                    test_expiry_date = expiry_date
                    test_expiry_date_str_current = expiry_date_str
                
                # Skip if we still don't have a valid expiry date
                if not test_expiry_date:
                    continue
                    
                # Make sure we have a valid test_expiry_date before proceeding with status checks
                
            # Extract required column values
            row_data = {}
            for i, col_idx in enumerate(required_cols_idx):
                if col_idx >= 0 and col_idx < len(row):
                    row_data[required_cols[i]] = row[col_idx]
                else:
                    row_data[required_cols[i]] = ""
            
            # Check if expired
            if test_expiry_date.date() < today.date():
                rows_expired.append(row_data)
            # Check if expiring soon (within 7 days)
            elif test_expiry_date.date() <= upcoming_expiry_threshold.date():
                rows_expiring_soon.append(row_data)
        
        # Update expiry dates in the worksheet
        if rows_to_update:
            # Create a list of Cell objects to update all at once
            cells_to_update = [gspread.Cell(row, col, val) for row, col, val in rows_to_update]
            worksheet.update_cells(cells_to_update)
            print(f"Updated {len(rows_to_update)} rows with calculated expiry dates.")
        
        # Return the rows that need attention
        return {
            'expiring_soon': rows_expiring_soon,
            'expired': rows_expired,
            'missing_test_date': rows_missing_test_date
        }
        
    except Exception as e:
        print(f"Error updating periodic testing dates: {str(e)}")
        return None

# 3. Function to create a visualization of expiry status
def create_expiry_chart(expired_count, expiring_soon_count, missing_test_date_count):
    try:
        plt.figure(figsize=(12, 6))
        
        # Create bar chart of expiry status
        labels = ['Đã hết hạn', 'Sắp hết hạn (7 ngày)', 'Thiếu ngày kiểm định kỳ']
        counts = [expired_count, expiring_soon_count, missing_test_date_count]
        colors = ['red', 'orange', 'blue']
        
        plt.bar(labels, counts, color=colors)
        plt.title('Trạng thái kiểm định kỳ NVL')
        plt.ylabel('Số lượng NVL')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add count labels on top of each bar
        for i, count in enumerate(counts):
            plt.text(i, count + 0.5, str(count), ha='center')
        
        # Save chart to buffer
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=100)
        img_buffer.seek(0)
        
        plt.close()  # Close the plot to avoid warnings
        return img_buffer
        
    except Exception as e:
        print(f"Error creating expiry chart: {str(e)}")
        return None

# 4. Function to send email report
def send_email_report(report_data):
    print("Preparing to send email report...")
    
    # If no data requires attention, exit early
    if not report_data or (
        not report_data['expired'] and 
        not report_data['expiring_soon'] and 
        not report_data['missing_test_date']
    ):
        print("No raw materials require attention. No email sent.")
        return False
    
    try:
        expired_rows = report_data['expired']
        expiring_soon_rows = report_data['expiring_soon']
        missing_test_date_rows = report_data['missing_test_date']
        
        # Create chart for visualization
        chart_buffer = create_expiry_chart(
            len(expired_rows), 
            len(expiring_soon_rows), 
            len(missing_test_date_rows)
        )
        
        # Create email
        msg = MIMEMultipart()
        msg['Subject'] = f'Báo cáo kiểm định kỳ NVL - {datetime.today().strftime("%d/%m/%Y")}'
        msg['From'] = 'hoitkn@msc.masangroup.com'
        
        recipients = ["hoitkn@msc.masangroup.com", "qanvlmb@msc.masangroup.com"]
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
                .expired {{ background-color: #ffcccc; }}
                .expiring-soon {{ background-color: #ffeb99; }}
                .missing-data {{ background-color: #cce0ff; }}
                h2 {{ color: #003366; }}
                h3 {{ color: #004d99; margin-top: 25px; }}
                .summary {{ margin: 20px 0; }}
                .footer {{ margin-top: 30px; font-size: 0.9em; color: #666; }}
            </style>
        </head>
        <body>
            <h2>Báo cáo kiểm định kỳ NVL - {datetime.today().strftime("%d/%m/%Y")}</h2>
            
            <div class="summary">
                <p><strong>Số lượng NVL đã hết hạn kiểm định kỳ:</strong> {len(expired_rows)}</p>
                <p><strong>Số lượng NVL sắp hết hạn kiểm định kỳ (trong 7 ngày):</strong> {len(expiring_soon_rows)}</p>
                <p><strong>Số lượng NVL thiếu ngày kiểm định kỳ:</strong> {len(missing_test_date_rows)}</p>
            </div>
        """
        
        # Add expired materials section if any
        if expired_rows:
            html_content += """
            <h3>Danh sách NVL đã hết hạn kiểm định kỳ:</h3>
            <table>
                <thead>
                    <tr>
                        <th>MPO Phụ Trách</th>
                        <th>Ngành</th>
                        <th>Item</th>
                        <th>Tên NVL</th>
                        <th>Nhà cung cấp</th>
                        <th>Mã NCC</th>
                        <th>Nhà sản xuất</th>
                        <th>Số hồ sơ công bố</th>
                        <th>Ngày kiểm định kỳ</th>
                        <th>Thời hạn KĐK</th>
                        <th>Ghi chú</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for row in expired_rows:
                html_content += """
                    <tr class="expired">
                """
                for field in ['MPO Phụ Trách', 'Ngành', 'Item', 'Tên NVL', 'Nhà cung cấp', 
                             'Mã NCC', 'Nhà sản xuất', 'Số hồ sơ công bố', 
                             'Ngày kiểm định kỳ', 'Thời hạn KĐK']:
                    html_content += f"""
                        <td>{row.get(field, '')}</td>
                    """
                # Add test date info column if available
                test_date_info = row.get('_test_date_info', '')
                html_content += f"""
                    <td>{test_date_info}</td>
                """
                # Add test date info column if available
                test_date_info = row.get('_test_date_info', '')
                html_content += f"""
                    <td>{test_date_info}</td>
                """
                html_content += """
                    </tr>
                """
                
            html_content += """
                </tbody>
            </table>
            """
        
        # Add missing test date materials section if any
        if missing_test_date_rows:
            html_content += """
            <h3>Danh sách NVL chưa có ngày kiểm định kỳ:</h3>
            <table>
                <thead>
                    <tr>
                        <th>MPO Phụ Trách</th>
                        <th>Ngành</th>
                        <th>Item</th>
                        <th>Tên NVL</th>
                        <th>Nhà cung cấp</th>
                        <th>Mã NCC</th>
                        <th>Nhà sản xuất</th>
                        <th>Số hồ sơ công bố</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for row in missing_test_date_rows:
                html_content += """
                    <tr class="missing-data">
                """
                for field in ['MPO Phụ Trách', 'Ngành', 'Item', 'Tên NVL', 'Nhà cung cấp', 
                             'Mã NCC', 'Nhà sản xuất', 'Số hồ sơ công bố']:
                    html_content += f"""
                        <td>{row.get(field, '')}</td>
                    """
                html_content += """
                    </tr>
                """
                
            html_content += """
                </tbody>
            </table>
            """
        
        # Add expiring soon materials section if any
        if expiring_soon_rows:
            html_content += """
            <h3>Danh sách NVL sắp hết hạn kiểm định kỳ (trong 7 ngày):</h3>
            <table>
                <thead>
                    <tr>
                        <th>MPO Phụ Trách</th>
                        <th>Ngành</th>
                        <th>Item</th>
                        <th>Tên NVL</th>
                        <th>Nhà cung cấp</th>
                        <th>Mã NCC</th>
                        <th>Nhà sản xuất</th>
                        <th>Số hồ sơ công bố</th>
                        <th>Ngày kiểm định kỳ</th>
                        <th>Thời hạn KĐK</th>
                        <th>Ghi chú</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for row in expiring_soon_rows:
                html_content += """
                    <tr class="expiring-soon">
                """
                for field in ['MPO Phụ Trách', 'Ngành', 'Item', 'Tên NVL', 'Nhà cung cấp', 
                             'Mã NCC', 'Nhà sản xuất', 'Số hồ sơ công bố', 
                             'Ngày kiểm định kỳ', 'Thời hạn KĐK']:
                    html_content += f"""
                        <td>{row.get(field, '')}</td>
                    """
                html_content += """
                    </tr>
                """
                
            html_content += """
                </tbody>
            </table>
            """
        
        # Add footer
        html_content += """
            <div class="footer">
                <p>Vui lòng xem Google Sheets để biết chi tiết và cập nhật.</p>
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
            part.add_header('Content-Disposition', 'attachment; filename="periodic_testing_status.png"')
            msg.attach(part)
        
        # Send email using environment variable for password
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            email_password = os.environ.get('EMAIL_PASSWORD')
            server.login("hoitkn@msc.masangroup.com", email_password)
            server.send_message(msg)
            
        print("Email report sent successfully!")
        return True
        
    except Exception as e:
        print(f"Error sending email report: {str(e)}")
        return False

# 5. Main function to run everything
def # Main execution code
if __name__ == "__main__":
    try:
        run_periodic_testing_monitor()
    except Exception as e:
        print(f"Error running periodic testing monitor: {str(e)}")
        import traceback
        traceback.print_exc():
    print("Starting raw material periodic testing monitoring...")
    
    try:
        # Update periodic testing dates and get report data
        report_data = update_periodic_testing_dates()
        
        # Send email report
        if report_data:
            send_email_report(report_data)
        
        print("Raw material periodic testing monitoring completed.")
        return True
    except Exception as e:
        print(f"Error in periodic testing monitoring: {str(e)}")
        return False

# Run the update if executed directly
if __name__ == "__main__":
    run_periodic_testing_monitor()
