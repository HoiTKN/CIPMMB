import gspread
import os
import json
import requests
import msal
import base64
import traceback
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from datetime import datetime, timedelta
import pandas as pd
import io
import sys

# SharePoint Configuration for Graph API
SHAREPOINT_CONFIG = {
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Sites.ReadWrite.All'],
}

# Global processor variable for Graph API
global_processor = None

# Google Sheets authentication setup
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Detect if running in GitHub Actions or similar CI environment
IN_CI_ENVIRONMENT = os.environ.get('CI') or os.environ.get('GITHUB_ACTIONS')

class GraphAPIProcessor:
    """Microsoft Graph API processor for sending emails"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.msal_app = None

        # Initialize MSAL app
        self.msal_app = msal.PublicClientApplication(
            SHAREPOINT_CONFIG['client_id'],
            authority=SHAREPOINT_CONFIG['authority']
        )

        # Authenticate on initialization
        if not self.authenticate():
            print("⚠️ Graph API authentication failed - will fallback to no email")

    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        sys.stdout.flush()

    def authenticate(self):
        """Authenticate using delegation flow with pre-generated tokens"""
        try:
            self.log("🔐 Authenticating with Microsoft Graph API...")

            # Get tokens from environment variables
            access_token = os.environ.get('SHAREPOINT_ACCESS_TOKEN')
            refresh_token = os.environ.get('SHAREPOINT_REFRESH_TOKEN')

            if not access_token and not refresh_token:
                self.log("❌ No Graph API tokens found in environment variables")
                return False

            self.access_token = access_token
            self.refresh_token = refresh_token

            if access_token:
                self.log(f"✅ Found access token: {access_token[:30]}...")

                # Test token validity
                if self.test_token_validity():
                    self.log("✅ Graph API access token is valid")
                    return True
                else:
                    self.log("⚠️ Graph API access token expired, attempting refresh...")

            # Try to refresh token
            if refresh_token:
                if self.refresh_access_token():
                    self.log("✅ Graph API token refreshed successfully")
                    return True
                else:
                    self.log("❌ Graph API token refresh failed")
                    return False
            else:
                self.log("❌ No Graph API refresh token available")
                return False

        except Exception as e:
            self.log(f"❌ Graph API authentication error: {str(e)}")
            return False

    def test_token_validity(self):
        """Test if current access token is valid"""
        try:
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/me", headers=headers, timeout=30)

            if response.status_code == 200:
                user_info = response.json()
                self.log(f"✅ Authenticated to Graph API as: {user_info.get('displayName', 'Unknown')}")
                return True
            elif response.status_code == 401:
                return False
            else:
                self.log(f"Warning: Unexpected response code: {response.status_code}")
                return False

        except Exception as e:
            self.log(f"Error testing Graph API token validity: {str(e)}")
            return False

    def refresh_access_token(self):
        """Refresh access token using refresh token with MSAL"""
        try:
            if not self.refresh_token:
                self.log("❌ No refresh token available")
                return False

            self.log("🔄 Attempting to refresh Graph API token using MSAL...")

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

                self.log("✅ Graph API token refreshed successfully")
                return True
            else:
                error = result.get('error_description', 'Unknown error') if result else 'No result'
                self.log(f"❌ Graph API token refresh failed: {error}")
                return False

        except Exception as e:
            self.log(f"❌ Error refreshing Graph API token: {str(e)}")
            return False

    def get_headers(self):
        """Get headers for API requests"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

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
    
    # Clean up the date string - remove quotes and extra whitespace
    date_str = date_str.strip().strip("'").strip('"')
    
    # First try with two-digit year formats
    date_formats_short = ['%d/%m/%y', '%m/%d/%y', '%d-%m-%y']
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
    date_formats_long = ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%B %d, %Y', '%d-%m-%Y']
    for fmt in date_formats_long:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # Try American date format with single-digit month
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            if 1 <= month <= 12 and 1 <= day <= 31:
                if year < 100:
                    year = 2000 + year if year < 30 else 1900 + year
                return datetime(year, month, day)
    except Exception:
        pass
            
    print(f"Could not parse date: {date_str}")
    return None

# Function to update periodic testing dates
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
                row_data['Status'] = 'Thiếu ngày kiểm định kỳ'
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
                        # For test date and expiry date columns, use the current date we're processing
                        if required_cols[i] == 'Ngày kiểm định kỳ':
                            row_data[required_cols[i]] = periodic_date_str
                        elif required_cols[i] == 'Thời hạn KĐK':
                            row_data[required_cols[i]] = test_expiry_date_str_current
                        else:
                            row_data[required_cols[i]] = row[col_idx]
                    else:
                        row_data[required_cols[i]] = ""
                
                # Check if expired
                if test_expiry_date.date() < today.date():
                    # Add material item + specific test date for clarity
                    if 'Item' in row_data and 'Tên NVL' in row_data:
                        item_id = row_data.get('Item', '')
                        material_name = row_data.get('Tên NVL', '')
                        
                        # Make a deep copy to avoid modifying the original
                        expired_row_data = row_data.copy()
                        expired_row_data['_test_date_info'] = f"Test date: {periodic_date_str}, Expiry: {test_expiry_date_str_current}"
                        expired_row_data['Status'] = 'Đã hết hạn'
                        rows_expired.append(expired_row_data)
                        
                # Check if expiring soon (within 7 days)
                elif test_expiry_date.date() <= upcoming_expiry_threshold.date():
                    # Make a deep copy to avoid modifying the original
                    expiring_row_data = row_data.copy()
                    expiring_row_data['_test_date_info'] = f"Test date: {periodic_date_str}, Expiry: {test_expiry_date_str_current}"
                    expiring_row_data['Status'] = 'Sắp hết hạn'
                    rows_expiring_soon.append(expiring_row_data)
        
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
        traceback.print_exc()
        return None

# Simplified Excel report creation
def create_excel_file(report_data):
    print("Creating Excel file...")
    
    try:
        # Create temp file path for Excel
        report_date = datetime.today().strftime("%Y%m%d")
        file_path = f"NVL_Periodic_Testing_Report_{report_date}.xlsx"
        
        # Try to determine which Excel engine is available
        excel_engine = 'openpyxl'  # Default fallback
        try:
            import xlsxwriter
            excel_engine = 'xlsxwriter'
            print(f"Using {excel_engine} engine for Excel creation")
        except ImportError:
            print(f"xlsxwriter not found, using {excel_engine} engine instead")
            
        # Create Excel with multiple sheets using ExcelWriter
        with pd.ExcelWriter(file_path, engine=excel_engine) as writer:
            # Process expired items
            if report_data['expired']:
                print(f"Adding {len(report_data['expired'])} expired items")
                expired_df = pd.DataFrame(report_data['expired'])
                if '_test_date_info' in expired_df.columns:
                    expired_df = expired_df.rename(columns={'_test_date_info': 'Ghi chú test'})
                
                # Write to Excel
                expired_df.to_excel(writer, sheet_name='Đã hết hạn', index=False)
                
                # Format header if xlsxwriter is available
                if excel_engine == 'xlsxwriter':
                    workbook = writer.book
                    worksheet = writer.sheets['Đã hết hạn']
                    
                    # Add red background format for header
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#FFC7CE',
                        'font_color': '#9C0006',
                        'border': 1
                    })
                    
                    # Apply header format
                    for col_num, col_name in enumerate(expired_df.columns):
                        worksheet.write(0, col_num, col_name, header_format)
                        # Set column width
                        max_len = max([
                            len(str(x)) for x in expired_df[col_name].tolist() + [col_name]
                        ]) + 2
                        worksheet.set_column(col_num, col_num, max_len)
            
            # Process expiring soon items
            if report_data['expiring_soon']:
                print(f"Adding {len(report_data['expiring_soon'])} expiring soon items")
                expiring_df = pd.DataFrame(report_data['expiring_soon'])
                if '_test_date_info' in expiring_df.columns:
                    expiring_df = expiring_df.rename(columns={'_test_date_info': 'Ghi chú test'})
                
                # Write to Excel
                expiring_df.to_excel(writer, sheet_name='Sắp hết hạn', index=False)
                
                # Format header if xlsxwriter is available
                if excel_engine == 'xlsxwriter':
                    worksheet = writer.sheets['Sắp hết hạn']
                    
                    # Add yellow background format for header
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#FFEB9C',
                        'font_color': '#9C6500',
                        'border': 1
                    })
                    
                    # Apply header format
                    for col_num, col_name in enumerate(expiring_df.columns):
                        worksheet.write(0, col_num, col_name, header_format)
                        # Set column width
                        max_len = max([
                            len(str(x)) for x in expiring_df[col_name].tolist() + [col_name]
                        ]) + 2
                        worksheet.set_column(col_num, col_num, max_len)
            
            # Process missing test date items
            if report_data['missing_test_date']:
                print(f"Adding {len(report_data['missing_test_date'])} missing test date items")
                missing_df = pd.DataFrame(report_data['missing_test_date'])
                
                # Write to Excel
                missing_df.to_excel(writer, sheet_name='Thiếu ngày KĐK', index=False)
                
                # Format header if xlsxwriter is available
                if excel_engine == 'xlsxwriter':
                    worksheet = writer.sheets['Thiếu ngày KĐK']
                    
                    # Add blue background format for header
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#DBEEF4',
                        'font_color': '#2F75B5',
                        'border': 1
                    })
                    
                    # Apply header format
                    for col_num, col_name in enumerate(missing_df.columns):
                        worksheet.write(0, col_num, col_name, header_format)
                        # Set column width
                        max_len = max([
                            len(str(x)) for x in missing_df[col_name].tolist() + [col_name]
                        ]) + 2
                        worksheet.set_column(col_num, col_num, max_len)
            
            # Create a consolidated sheet with all items
            all_rows = []
            for category, items in report_data.items():
                for item in items:
                    item_copy = item.copy()
                    all_rows.append(item_copy)
            
            if all_rows:
                print(f"Adding {len(all_rows)} total items to summary sheet")
                all_df = pd.DataFrame(all_rows)
                if '_test_date_info' in all_df.columns:
                    all_df = all_df.rename(columns={'_test_date_info': 'Ghi chú test'})
                
                # Write to Excel
                all_df.to_excel(writer, sheet_name='Tất cả', index=False)
                
                # Format header if xlsxwriter is available
                if excel_engine == 'xlsxwriter':
                    worksheet = writer.sheets['Tất cả']
                    
                    # Add format for header
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#D9D9D9',
                        'border': 1
                    })
                    
                    # Apply header format
                    for col_num, col_name in enumerate(all_df.columns):
                        worksheet.write(0, col_num, col_name, header_format)
                        # Set column width
                        max_len = max([
                            len(str(x)) for x in all_df[col_name].tolist() + [col_name]
                        ]) + 2
                        worksheet.set_column(col_num, col_num, max_len)
        
        print("Excel file saved successfully")
        return file_path
        
    except Exception as e:
        print(f"Error creating Excel file: {str(e)}")
        traceback.print_exc()
        return None

# Function to send email report using Microsoft Graph API
def send_email_report(report_data):
    """Send email report using Microsoft Graph API (Outlook)"""
    global global_processor
    
    print("Preparing to send email report via Microsoft Graph API...")
    
    # If no data requires attention, exit early
    if not report_data or (
        not report_data['expired'] and 
        not report_data['expiring_soon'] and 
        not report_data['missing_test_date']
    ):
        print("No raw materials require attention. No email sent.")
        return False
    
    try:
        if not global_processor or not global_processor.access_token:
            print("❌ No valid access token for Graph API")
            return False

        expired_rows = report_data['expired']
        expiring_soon_rows = report_data['expiring_soon']
        missing_test_date_rows = report_data['missing_test_date']
        
        # Create Excel file (local file, not using BytesIO)
        excel_path = create_excel_file(report_data)
        if not excel_path:
            print("Failed to create Excel file")
            return False
        
        # Create HTML content with improved styling
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    line-height: 1.6;
                    color: #333;
                    background-color: #f8f9fa;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .header {{
                    background: linear-gradient(135deg, #366092, #4a7bb7);
                    color: white;
                    padding: 20px;
                    border-radius: 8px 8px 0 0;
                    margin: -20px -20px 20px -20px;
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 24px;
                    font-weight: bold;
                }}
                .summary {{
                    background-color: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    margin: 20px 0;
                    border-left: 4px solid #366092;
                }}
                .summary h3 {{
                    color: #366092;
                    margin-top: 0;
                    font-size: 18px;
                }}
                .kpi {{
                    display: inline-block;
                    background: white;
                    padding: 15px;
                    margin: 10px;
                    border-radius: 8px;
                    text-align: center;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                    min-width: 150px;
                }}
                .kpi-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #C00000;
                }}
                .kpi-label {{
                    font-size: 12px;
                    color: #666;
                    margin-top: 5px;
                }}
                table {{ 
                    border-collapse: collapse; 
                    width: 100%; 
                    margin-top: 20px;
                    font-size: 11px;
                }}
                th, td {{ 
                    border: 1px solid #ddd; 
                    padding: 12px 8px; 
                    text-align: left; 
                    vertical-align: top;
                }}
                th {{ 
                    background: linear-gradient(135deg, #366092, #4a7bb7);
                    color: white;
                    font-weight: bold;
                    text-align: center;
                    font-size: 10px;
                }}
                .expired {{ 
                    background-color: #ffcccc;
                    border-left: 4px solid #C00000;
                }}
                .expiring-soon {{ 
                    background-color: #ffeb99;
                    border-left: 4px solid #FFA500;
                }}
                .missing-data {{ 
                    background-color: #cce0ff;
                    border-left: 4px solid #0066CC;
                }}
                .footer {{ 
                    margin-top: 30px; 
                    padding-top: 20px;
                    border-top: 1px solid #e0e0e0;
                    font-size: 12px; 
                    color: #666; 
                    text-align: center;
                }}
                .important {{
                    font-weight: bold;
                    color: #C00000;
                    background-color: #ffebee;
                    padding: 10px;
                    border-radius: 4px;
                    margin: 15px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📋 BÁO CÁO KIỂM ĐỊNH KỲ NVL</h1>
                    <p style="margin: 10px 0 0 0; font-size: 16px;">{datetime.today().strftime("%d/%m/%Y")}</p>
                </div>

                <div class="summary">
                    <h3>📊 TỔNG QUAN TÌNH TRẠNG</h3>
                    <div style="text-align: center;">
                        <div class="kpi">
                            <div class="kpi-value" style="color: #C00000;">{len(expired_rows)}</div>
                            <div class="kpi-label">NVL đã hết hạn KĐK</div>
                        </div>
                        <div class="kpi">
                            <div class="kpi-value" style="color: #FFA500;">{len(expiring_soon_rows)}</div>
                            <div class="kpi-label">NVL sắp hết hạn KĐK<br>(trong 7 ngày)</div>
                        </div>
                        <div class="kpi">
                            <div class="kpi-value" style="color: #0066CC;">{len(missing_test_date_rows)}</div>
                            <div class="kpi-label">NVL thiếu ngày KĐK</div>
                        </div>
                    </div>
                    <div class="important">
                        📊 Một file Excel đã được đính kèm với báo cáo này để tiện lọc và xử lý dữ liệu.
                    </div>
                </div>
        """
        
        # Add expired materials section if any
        if expired_rows:
            html_content += """
            <h3 style="color: #C00000;">🔴 DANH SÁCH NVL ĐÃ HẾT HẠN KIỂM ĐỊNH KỲ:</h3>
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
            <h3 style="color: #FFA500;">🟡 DANH SÁCH NVL SẮP HẾT HẠN KIỂM ĐỊNH KỲ (TRONG 7 NGÀY):</h3>
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
            <h3 style="color: #0066CC;">🔵 DANH SÁCH NVL CHƯA CÓ NGÀY KIỂM ĐỊNH KỲ:</h3>
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
        
        # Add footer
        html_content += f"""
                <div class="footer">
                    <h4>📝 Hướng dẫn xử lý:</h4>
                    <ol>
                        <li><strong>🔴 NVL đã hết hạn:</strong> Cần thực hiện kiểm định kỳ ngay lập tức</li>
                        <li><strong>🟡 NVL sắp hết hạn:</strong> Lên kế hoạch kiểm định trong 7 ngày tới</li>
                        <li><strong>🔵 NVL thiếu thông tin:</strong> Cập nhật ngày kiểm định kỳ vào Google Sheets</li>
                        <li>Vui lòng truy cập Google Sheets để cập nhật thông tin chi tiết</li>
                    </ol>
                    <p><em>⚠️ Email này được tự động tạo bởi hệ thống kiểm định kỳ NVL. Vui lòng không trả lời email này.</em></p>
                    <p>🕒 Thời gian tạo: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """

        # Prepare email data for Graph API
        email_data = {
            "message": {
                "subject": f"📋 Báo cáo kiểm định kỳ NVL - {datetime.today().strftime('%d/%m/%Y')}",
                "body": {
                    "contentType": "HTML",
                    "content": html_content
                },
                "toRecipients": []
            }
        }

        # Add recipients
        recipients = ["hoitkn@msc.masangroup.com", "qanvlmb@msc.masangroup.com", "qakstlmb@msc.masangroup.com", "thangtv@msc.masangroup.com"]
        for recipient in recipients:
            email_data["message"]["toRecipients"].append({
                "emailAddress": {
                    "address": recipient
                }
            })

        # Prepare Excel attachment
        attachments = []
        try:
            with open(excel_path, 'rb') as f:
                excel_data = f.read()
                excel_b64 = base64.b64encode(excel_data).decode('utf-8')

                attachments.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": f"{os.path.basename(excel_path)}",
                    "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "contentBytes": excel_b64
                })
                print(f"Excel file prepared for attachment: {excel_path}")
        except Exception as e:
            print(f"Error preparing Excel attachment: {str(e)}")

        if attachments:
            email_data["message"]["attachments"] = attachments

        # Send email via Graph API
        graph_url = "https://graph.microsoft.com/v1.0/me/sendMail"
        headers = {
            'Authorization': f'Bearer {global_processor.access_token}',
            'Content-Type': 'application/json'
        }

        print(f"📤 Sending email via Graph API to {len(recipients)} recipients...")

        response = requests.post(graph_url, headers=headers, json=email_data, timeout=60)

        if response.status_code == 202:
            print("✅ Email sent successfully via Graph API")
            print(f"✅ Email đã được gửi đến {len(recipients)} người nhận.")
            
            # Clean up the local Excel file after sending
            try:
                os.remove(excel_path)
                print(f"Temporary Excel file removed: {excel_path}")
            except Exception as cleanup_e:
                print(f"Warning: Could not remove temporary Excel file: {str(cleanup_e)}")
            
            return True
        elif response.status_code == 401:
            print("❌ Graph API Authentication Error - Token may have expired")
            print("🔄 Attempting to refresh token...")
            if global_processor.refresh_access_token():
                print("✅ Token refreshed, retrying email send...")
                headers['Authorization'] = f'Bearer {global_processor.access_token}'
                response = requests.post(graph_url, headers=headers, json=email_data, timeout=60)
                if response.status_code == 202:
                    print("✅ Email sent successfully after token refresh")
                    return True
            print("❌ Failed to send email even after token refresh")
            return False
        elif response.status_code == 403:
            print("❌ Graph API Permission Error")
            print("💡 Please ensure Mail.Send permission is granted in Azure App Registration")
            return False
        else:
            print(f"❌ Graph API Error: {response.status_code}")
            print(f"❌ Response: {response.text[:500]}")
            return False

    except Exception as e:
        print(f"❌ Error sending email report via Graph API: {str(e)}")
        traceback.print_exc()
        return False

# Main function to run everything
def run_periodic_testing_monitor():
    global global_processor
    print("Starting raw material periodic testing monitoring...")
    
    try:
        # Initialize Graph API processor for email
        global_processor = GraphAPIProcessor()
        
        # Update periodic testing dates and get report data
        report_data = update_periodic_testing_dates()
        
        # Send email report
        if report_data:
            send_email_report(report_data)
        
        print("Raw material periodic testing monitoring completed.")
        return True
    except Exception as e:
        print(f"Error in periodic testing monitoring: {str(e)}")
        traceback.print_exc()
        return False

# Main execution code
if __name__ == "__main__":
    try:
        run_periodic_testing_monitor()
    except Exception as e:
        print(f"Error running periodic testing monitor: {str(e)}")
        traceback.print_exc()
