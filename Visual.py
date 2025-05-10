import pandas as pd
import gspread
import os
import sys
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from datetime import datetime, time

# Define the scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def authenticate():
    """Authentication using OAuth token"""
    try:
        print("Starting OAuth authentication process...")
        creds = None
        
        # Check if token.json exists first
        if os.path.exists('token.json'):
            print("Loading credentials from existing token.json file")
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # Otherwise create it from the environment variable
        elif os.environ.get('GOOGLE_TOKEN_JSON'):
            print("Creating token.json from GOOGLE_TOKEN_JSON environment variable")
            with open('token.json', 'w') as f:
                f.write(os.environ.get('GOOGLE_TOKEN_JSON'))
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        else:
            print("Error: No token.json file or GOOGLE_TOKEN_JSON environment variable found")
            sys.exit(1)
        
        # Refresh token if expired
        if creds and creds.expired and creds.refresh_token:
            print("Token expired, refreshing...")
            creds.refresh(Request())
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
                
        # Return authorized client
        return gspread.authorize(creds)
    
    except Exception as e:
        print(f"Authentication error: {str(e)}")
        sys.exit(1)

def get_week_number(date):
    """Extract week number from date"""
    if pd.isna(date) or date is None:
        return None
    return date.isocalendar()[1]

def get_month_number(date):
    """Extract month number from date"""
    if pd.isna(date) or date is None:
        return None
    return date.month

def standardize_date(date_str):
    """Convert date string to datetime object"""
    try:
        if isinstance(date_str, str):
            # Handle DD/MM/YYYY format
            if '/' in date_str:
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y', dayfirst=True)
                except:
                    pass
            # Try pandas default parsing with dayfirst=True
            return pd.to_datetime(date_str, dayfirst=True)
        return pd.to_datetime(date_str)
    except:
        return None

def parse_hour(hour_str):
    """Extract hour from hour string"""
    if pd.isna(hour_str) or not isinstance(hour_str, str):
        return None
    
    # Clean the input
    hour_str = hour_str.lower().strip()
    
    # Handle different formats
    if 'h' in hour_str:
        try:
            # Extract hour part before 'h'
            hour_part = hour_str.split('h')[0]
            return int(hour_part)
        except:
            pass
    
    # Handle format like '14:00'
    if ':' in hour_str:
        try:
            hour_part = hour_str.split(':')[0]
            return int(hour_part)
        except:
            pass
    
    # Try direct conversion if it's just a number
    try:
        return int(hour_str)
    except:
        return None

def determine_shift(hour):
    """Determine shift (Ca) based on hour"""
    if hour is None:
        return None
    
    if 6 <= hour < 14:
        return 1
    elif 14 <= hour < 22:
        return 2
    else:
        return 3

def get_target_tv(line):
    """Determine Target TV based on Line number"""
    if pd.isna(line):
        return None
    
    try:
        line_num = float(line)
        if 1 <= line_num <= 6:
            return 0.29
        elif 7 <= line_num <= 8:
            return 2.19
        else:
            return None
    except (ValueError, TypeError):
        return None

def format_as_table(worksheet):
    """Format worksheet as a table for Power BI"""
    try:
        # Get the number of rows and columns
        rows = worksheet.row_count
        cols = worksheet.col_count
        
        # Define the range for formatting
        range_name = f'A1:{chr(64 + cols)}{rows}'
        
        # Apply table formatting
        worksheet.format(range_name, {
            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
            "horizontalAlignment": "CENTER",
            "textFormat": {"bold": True}
        })
        
        # Format header row
        worksheet.format("A1:Z1", {
            "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8},
            "textFormat": {"bold": True}
        })
        
        # Add alternating row colors for better readability
        # This isn't a true "table" but makes it more table-like for export
        for i in range(2, rows + 1, 2):
            worksheet.format(f'A{i}:{chr(64 + cols)}{i}', {
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
        
        print("Applied table formatting to worksheet")
    except Exception as e:
        print(f"Warning: Could not apply table formatting: {str(e)}")

def main():
    print("Starting Google Sheets data processing...")
    
    # Authenticate and connect to Google Sheets
    gc = authenticate()
    
    # Open the source spreadsheet (ID AQL)
    source_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit')
    
    # Open the destination spreadsheet
    destination_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1sb7Wz26CVkyUfWUE7NQmWm7_Byhw9eAHPArIUnn3iDA/edit')

    try:
        # Get the ID AQL worksheet data
        id_aql_worksheet = source_sheet.worksheet('ID AQL')
        id_aql_data = id_aql_worksheet.get_all_records()
        id_aql_df = pd.DataFrame(id_aql_data)
        
        # Get the defect code mapping from AQL gói sheet
        aql_goi_worksheet = source_sheet.worksheet('AQL gói')
        aql_goi_data = aql_goi_worksheet.get_all_records()
        aql_goi_df = pd.DataFrame(aql_goi_data)
        
        # Get the defect code mapping from AQL Tô ly sheet
        aql_to_ly_worksheet = source_sheet.worksheet('AQL Tô ly')
        aql_to_ly_data = aql_to_ly_worksheet.get_all_records()
        aql_to_ly_df = pd.DataFrame(aql_to_ly_data)
        
        print(f"Retrieved {len(id_aql_df)} ID AQL records, {len(aql_goi_df)} AQL gói records, and {len(aql_to_ly_df)} AQL Tô ly records")
    
    except Exception as e:
        print(f"Error retrieving worksheet data: {str(e)}")
        sys.exit(1)

    # Convert 'Line' to numeric if it's not already
    id_aql_df['Line'] = pd.to_numeric(id_aql_df['Line'], errors='coerce')

    # Standardize defect codes (clean up any leading/trailing spaces)
    id_aql_df['Defect code'] = id_aql_df['Defect code'].astype(str).str.strip()
    aql_goi_df['Defect code'] = aql_goi_df['Defect code'].astype(str).str.strip()
    aql_to_ly_df['Defect code'] = aql_to_ly_df['Defect code'].astype(str).str.strip()

    # Standardize dates
    id_aql_df['Ngày SX_std'] = id_aql_df['Ngày SX'].apply(standardize_date)
    
    # Extract week and month
    id_aql_df['Tuần'] = id_aql_df['Ngày SX_std'].apply(get_week_number)
    id_aql_df['Tháng'] = id_aql_df['Ngày SX_std'].apply(get_month_number)
    
    # Extract hour and determine shift (Ca)
    id_aql_df['hour'] = id_aql_df['Giờ'].apply(parse_hour)
    id_aql_df['Ca'] = id_aql_df['hour'].apply(determine_shift)
    
    # Add Target TV column based on Line
    id_aql_df['Target TV'] = id_aql_df['Line'].apply(get_target_tv)
    
    # Create defect name mapping dictionaries
    goi_defect_map = dict(zip(aql_goi_df['Defect code'], aql_goi_df['Defect name']))
    to_ly_defect_map = dict(zip(aql_to_ly_df['Defect code'], aql_to_ly_df['Defect name']))
    
    # Function to map defect code to defect name based on the Line value
    def map_defect_name(row):
        if pd.isna(row['Line']) or pd.isna(row['Defect code']) or row['Defect code'] == 'nan':
            return None
        
        try:
            line = float(row['Line'])
            defect_code = str(row['Defect code']).strip()
            
            if 1 <= line <= 6:
                return goi_defect_map.get(defect_code, None)
            elif 7 <= line <= 8:
                return to_ly_defect_map.get(defect_code, None)
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    # Apply the mapping
    id_aql_df['Defect name'] = id_aql_df.apply(map_defect_name, axis=1)
    
    # Create the new dataframe with required columns
    try:
        new_df = id_aql_df[[
            'Ngày SX', 'Tuần', 'Tháng', 'Sản phẩm', 'Item', 'Giờ', 'Ca', 'Line', 'MĐG', 
            'SL gói lỗi sau xử lý', 'Defect code', 'Defect name', 'Số lượng hold ( gói/thùng)',
            'Target TV', 'QA', 'Tên Trưởng ca'
        ]].copy()
    except KeyError as e:
        print(f"Error: Missing column in source data: {e}")
        print(f"Available columns: {id_aql_df.columns.tolist()}")
        sys.exit(1)
    
    # Sort by Ngày SX (newest first)
    new_df = new_df.sort_values(by='Ngày SX', ascending=False)
    
    # Save to the destination spreadsheet
    try:
        # Check if the "Processed_Data" worksheet exists in the destination sheet
        try:
            processed_worksheet = destination_sheet.worksheet('Processed_Data')
            processed_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # Create a new worksheet if it doesn't exist
            processed_worksheet = destination_sheet.add_worksheet(
                title='Processed_Data',
                rows=new_df.shape[0]+1,
                cols=new_df.shape[1]
            )

        # Convert DataFrame to list of lists for Google Sheets
        # Handle NaN values by converting to empty strings
        new_df_cleaned = new_df.fillna('')
        data_to_write = [new_df_cleaned.columns.tolist()] + new_df_cleaned.values.tolist()

        # Update the worksheet
        processed_worksheet.update('A1', data_to_write)
        print(f"Successfully wrote {len(data_to_write)-1} rows to the destination sheet, sorted by Ngày SX (newest first)")
        
        # Format the worksheet as a table
        format_as_table(processed_worksheet)

    except Exception as e:
        print(f"Error writing to destination sheet: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
