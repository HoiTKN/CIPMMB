import pandas as pd
import re
from datetime import datetime, time
import gspread
import os
import sys
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Define the scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def authenticate():
    """Authentication using OAuth token - exactly matching your other scripts"""
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

def extract_correct_date(text):
    """Extract the correct Ngày SX from Nội dung phản hồi"""
    if not isinstance(text, str):
        return None
    
    # Pattern to find "Ngày SX: DD/MM/YYYY"
    pattern = r'Ngày SX:\s*(\d{1,2}/\d{1,2}/\d{4})'
    match = re.search(pattern, text)
    
    if match:
        return match.group(1)  # Return the date exactly as it appears in the text
    
    return None

def extract_production_info(text):
    if not isinstance(text, str):
        return None, None, None

    # More flexible patterns to handle variations
    patterns = [
        # Pattern for "(HH:MM DD)" where DD is line number and machine number (two digits)
        r'Nơi SX: I-MBP \((\d{2}:\d{2})\s+(\d)(\d)I\s*\)',
        r'Nơi SX: I-MBP \((\d{2}:\d{2})\s+(\d)(\d)\s*I\s*\)',
        # Pattern for "(HH:MM D)" where D is just line number (single digit)
        r'Nơi SX: I-MBP \((\d{2}:\d{2})\s+(\d)I\s*\)',
        # Pattern for "(HH:MM DI)" where D is just line number (single digit) 
        r'Nơi SX: I-MBP \((\d{2}:\d{2})\s+(\d)I\)',
        # Pattern with optional machine
        r'Nơi SX: I-MBP \((\d{2}:\d{2})\s+(\d+)(?:(\d))?I?\s*\)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            production_time = match.group(1)  # HH:MM format
            
            # For patterns with optional machine
            if len(match.groups()) == 3:
                line = match.group(2)             # First digit or full number
                machine = match.group(3) if match.group(3) else None  # Second digit or None
            else:
                # For patterns with just line
                line = match.group(2)
                machine = None
            
            return production_time, line, machine
        
    return None, None, None

def standardize_date(date_str):
    try:
        if isinstance(date_str, str):
            # Handle DD-MMM-YYYY format (e.g., "4-Apr-2025")
            if '-' in date_str:
                try:
                    for fmt in ['%d-%b-%Y', '%d-%B-%Y', '%d-%b-%y', '%d-%B-%y']:
                        try:
                            return pd.to_datetime(date_str, format=fmt)
                        except:
                            pass
                except:
                    pass

            # Handle DD/MM/YYYY format
            if '/' in date_str:
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y', dayfirst=True)
                except:
                    pass

            # Last resort: Let pandas try to detect with dayfirst=True
            return pd.to_datetime(date_str, dayfirst=True)

        return pd.to_datetime(date_str)
    except:
        return None

def clean_item_code(item_code):
    if pd.isna(item_code) or item_code == '':
        return ''
    
    # Convert to string, remove spaces, and standardize
    item_str = str(item_code).strip()
    return item_str

def parse_time(time_str):
    if pd.isna(time_str) or time_str == '':
        return None
    
    time_str = str(time_str).strip().lower()
    
    try:
        # Handle HH:MM format
        if ':' in time_str:
            hours, minutes = map(int, time_str.split(':'))
            return time(hours, minutes)
        
        # Handle "22h" format
        elif 'h' in time_str:
            hours = int(time_str.replace('h', ''))
            return time(hours, 0)
        
        # Try to parse as simple hour
        else:
            try:
                hours = int(time_str)
                return time(hours, 0)
            except:
                return None
    except:
        return None

def round_to_2hour(t):
    if t is None:
        return None
    
    hour = t.hour
    # Round down to nearest even hour
    rounded_hour = (hour // 2) * 2
    return time(rounded_hour, 0)

def determine_shift(time_obj):
    if time_obj is None:
        return None
    
    # Create time boundaries
    shift1_start = time(6, 30)
    shift1_end = time(14, 30)
    shift2_end = time(22, 30)
    
    if shift1_start <= time_obj < shift1_end:
        return "Shift 1: 6:30-14:30"
    elif shift1_end <= time_obj < shift2_end:
        return "Shift 2: 14:30-22:30"
    else:
        return "Shift 3: 22:30-6:30"

def find_qa_and_leader(row, aql_data):
    if pd.isna(row['Ngày SX_std']) or row['Item_clean'] == '' or row['Giờ_time'] is None:
        return None, None, "Missing data"
    
    # 1. Filter AQL data for the same date, item, and line
    matching_rows = aql_data[
        (aql_data['Ngày SX_std'] == row['Ngày SX_std']) & 
        (aql_data['Item_clean'] == row['Item_clean']) &
        (aql_data['Line'] == row['Line_extracted'])
    ]
    
    if matching_rows.empty:
        return None, None, "No matches for date+item+line"
    
    # 2. Get the complaint hour and determine which 2-hour intervals to check
    complaint_hour = row['Giờ_time'].hour
    complaint_minute = row['Giờ_time'].minute
    
    # Determine which QA check hours to look at
    if complaint_minute == 0 and complaint_hour % 2 == 0:
        prev_hour = complaint_hour
        next_hour = (complaint_hour + 2) % 24
    else:
        prev_hour = (complaint_hour // 2) * 2
        next_hour = (prev_hour + 2) % 24
    
    # 3. Find QA records at these times
    prev_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == prev_hour and x.minute == 0)]
    next_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == next_hour and x.minute == 0)]
    
    debug_info = f"Complaint at {complaint_hour}:{complaint_minute}, checking {prev_hour}h and {next_hour}h"
    
    # 4. Apply the matching rules
    # 4a. First, check if there's data for the preceding hour
    if not prev_check.empty:
        prev_qa = prev_check.iloc[0].get('QA ') if 'QA ' in prev_check.columns else None
        prev_leader = None
        for col in ['Tên Trường ca', 'Trưởng ca']:
            if col in prev_check.columns:
                prev_leader = prev_check.iloc[0].get(col)
                if prev_leader is not None:
                    break
        
        # 4b. Check if there's data for the next hour
        if not next_check.empty:
            next_qa = next_check.iloc[0].get('QA ') if 'QA ' in next_check.columns else None
            next_leader = None
            for col in ['Tên Trường ca', 'Trưởng ca']:
                if col in next_check.columns:
                    next_leader = next_check.iloc[0].get(col)
                    if next_leader is not None:
                        break
            
            # 4c. If both QA and leader are the same, use them
            if prev_qa == next_qa and prev_leader == next_leader:
                return prev_qa, prev_leader, f"{debug_info} | Same QA and leader for both times"
        
        # 4d. Determine based on shift if we need to
        shift = row['Shift']
        
        # For times between 22:30-23:59, we use the next hour's QA (from 0h)
        if shift == "Shift 3: 22:30-6:30" and complaint_hour >= 22:
            if not next_check.empty:
                return next_qa, next_leader, f"{debug_info} | Using next hour (0h) based on Shift 3 rule"
        
        # For all other cases, use the preceding hour's QA
        return prev_qa, prev_leader, f"{debug_info} | Using previous hour QA"
    
    # If no data for preceding hour, try next hour
    elif not next_check.empty:
        next_qa = next_check.iloc[0].get('QA ') if 'QA ' in next_check.columns else None
        next_leader = None
        for col in ['Tên Trường ca', 'Trưởng ca']:
            if col in next_check.columns:
                next_leader = next_check.iloc[0].get(col)
                if next_leader is not None:
                    break
        return next_qa, next_leader, f"{debug_info} | Only next hour data available"
    
    # If no data for either hour, look for any data for same date, item, line
    if not matching_rows.empty:
        closest_row = None
        min_diff = float('inf')
        
        for _, aql_row in matching_rows.iterrows():
            if aql_row['Giờ_time'] is not None:
                aql_minutes = aql_row['Giờ_time'].hour * 60 + aql_row['Giờ_time'].minute
                complaint_minutes = complaint_hour * 60 + complaint_minute
                diff = abs(complaint_minutes - aql_minutes)
                
                if diff < min_diff:
                    min_diff = diff
                    closest_row = aql_row
        
        if closest_row is not None:
            closest_qa = closest_row.get('QA ') if 'QA ' in aql_data.columns else None
            closest_leader = None
            for col in ['Tên Trường ca', 'Trưởng ca']:
                if col in aql_data.columns:
                    closest_leader = closest_row.get(col)
                    if closest_leader is not None:
                        break
            return closest_qa, closest_leader, f"{debug_info} | Using closest time match"
    
    return None, None, f"{debug_info} | No matching QA records found"

def main():
    print("Starting Google Sheets integration...")
    
    # Authenticate and connect to Google Sheets
    gc = authenticate()
    
    # Open the source spreadsheets - use your actual spreadsheet URLs
    knkh_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1vbx_XlnuMzLdkRJkmGRv_kOqf74LU0aGEy5SJRs1LqU/edit')
    aql_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit')

    # Open the destination spreadsheet
    destination_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit')

    # Get the worksheet data
    knkh_worksheet = knkh_sheet.worksheet('KNKH')
    knkh_data = knkh_worksheet.get_all_records()
    knkh_df = pd.DataFrame(knkh_data)

    aql_worksheet = aql_sheet.worksheet('ID AQL')
    aql_data = aql_worksheet.get_all_records()
    aql_df = pd.DataFrame(aql_data)

    print(f"Retrieved {len(knkh_df)} KNKH records and {len(aql_df)} AQL records")

    # NEW: Extract correct Ngày SX from Nội dung phản hồi and ALWAYS replace the Ngày SX column
    knkh_df['Ngày SX_extracted'] = knkh_df['Nội dung phản hồi'].apply(extract_correct_date)
    
    # Replace the original Ngày SX with the extracted one when available, keeping the exact format
    knkh_df['Ngày SX'] = knkh_df.apply(
        lambda row: row['Ngày SX_extracted'] if row['Ngày SX_extracted'] is not None else row['Ngày SX'], 
        axis=1
    )

    # Standardize dates first for filtering
    knkh_df['Ngày SX_std'] = knkh_df['Ngày SX'].apply(standardize_date)
    aql_df['Ngày SX_std'] = aql_df['Ngày SX'].apply(standardize_date)
    
    # Create filter date (December 1, 2024)
    filter_date = pd.to_datetime('2024-12-01')
    
    # Filter both DataFrames to only include data from December 1, 2024 onwards
    knkh_df = knkh_df[knkh_df['Ngày SX_std'] >= filter_date]
    aql_df = aql_df[aql_df['Ngày SX_std'] >= filter_date]
    
    print(f"After date filtering: {len(knkh_df)} KNKH records and {len(aql_df)} AQL records")

    # Apply data processing steps
    # Step 2: Extract time, line, and machine information
    knkh_df[['Giờ_extracted', 'Line_extracted', 'Máy_extracted']] = knkh_df['Nội dung phản hồi'].apply(
        lambda x: pd.Series(extract_production_info(x))
    )

    # Convert to appropriate data types
    knkh_df['Line_extracted'] = pd.to_numeric(knkh_df['Line_extracted'], errors='coerce')
    knkh_df['Máy_extracted'] = pd.to_numeric(knkh_df['Máy_extracted'], errors='coerce')

    # Step 3: Standardize the receipt date
    knkh_df['Ngày tiếp nhận_std'] = knkh_df['Ngày tiếp nhận'].apply(standardize_date)

    # Step 4: Clean item codes
    knkh_df['Item_clean'] = knkh_df['Item'].apply(clean_item_code)
    aql_df['Item_clean'] = aql_df['Item'].apply(clean_item_code)

    # Step 5: Parse time
    knkh_df['Giờ_time'] = knkh_df['Giờ_extracted'].apply(parse_time)
    aql_df['Giờ_time'] = aql_df['Giờ'].apply(parse_time)

    # Round time to 2-hour intervals
    knkh_df['Giờ_rounded'] = knkh_df['Giờ_time'].apply(round_to_2hour)

    # Step 6: Determine shift
    knkh_df['Shift'] = knkh_df['Giờ_time'].apply(determine_shift)

    # Step 7: Match QA and leader
    knkh_df['QA_matched'] = None
    knkh_df['Tên Trưởng ca_matched'] = None
    knkh_df['debug_info'] = None

    print("Starting matching process...")
    for idx, row in knkh_df.iterrows():
        qa, leader, debug_info = find_qa_and_leader(row, aql_df)
        knkh_df.at[idx, 'QA_matched'] = qa
        knkh_df.at[idx, 'Tên Trưởng ca_matched'] = leader
        knkh_df.at[idx, 'debug_info'] = debug_info
    print("Matching process complete")

    # Create the joined dataframe with all required columns
    filtered_knkh_df = knkh_df.copy()
    joined_df = filtered_knkh_df[[
        'Mã ticket', 'Ngày tiếp nhận', 'Tỉnh', 'Ngày SX', 'Sản phẩm/Dịch vụ',
        'Số lượng (ly/hộp/chai/gói/hủ)', 'Nội dung phản hồi', 'Item', 'Tên sản phẩm',
        'SL pack/ cây lỗi', 'Tên lỗi', 'Line_extracted', 'Máy_extracted', 'Giờ_extracted',
        'QA_matched', 'Tên Trưởng ca_matched', 'Shift', 'Ngày tiếp nhận_std'
    ]].copy()

    # Rename columns for clarity
    joined_df.rename(columns={
        'Line_extracted': 'Line',
        'Máy_extracted': 'Máy',
        'Giờ_extracted': 'Giờ',
        'QA_matched': 'QA',
        'Tên Trưởng ca_matched': 'Tên Trưởng ca'
    }, inplace=True)

    # Sort by Mã ticket from largest to smallest
    joined_df = joined_df.sort_values(by='Mã ticket', ascending=False)

    # Remove the standardized date column as it's not needed for output
    joined_df = joined_df.drop(columns=['Ngày tiếp nhận_std'])

    # Save to the destination spreadsheet
    try:
        # Check if the "Integrated_Data" worksheet exists in the destination sheet
        try:
            integrated_worksheet = destination_sheet.worksheet('Integrated_Data')
            integrated_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # Create a new worksheet if it doesn't exist
            integrated_worksheet = destination_sheet.add_worksheet(
                title='Integrated_Data',
                rows=joined_df.shape[0]+1,
                cols=joined_df.shape[1]
            )

        # Convert DataFrame to list of lists for Google Sheets
        # Handle NaN values by converting to empty strings
        joined_df_cleaned = joined_df.fillna('')
        data_to_write = [joined_df_cleaned.columns.tolist()] + joined_df_cleaned.values.tolist()

        # Update the worksheet - FIXED METHOD
        integrated_worksheet.update('A1', data_to_write)
        print(f"Successfully wrote {len(data_to_write)-1} rows to the destination sheet, sorted by Mã ticket (largest to smallest)")

    except Exception as e:
        print(f"Error writing to destination sheet: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
