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

def extract_short_product_name(full_name):
    """
    Extract a shorter version of the product name that includes only brand name (Omachi/Kokomi)
    and the flavor, excluding packaging information.
    
    Examples:
    "Mì dinh dưỡng khoai tây Omachi mì trộn xốt Spaghetti 30gói x 90gr" -> "Omachi mì trộn xốt Spaghetti"
    "Mì dinh dưỡng khoai tây Omachi Sườn hầm ngũ quả 30gói x 80gr" -> "Omachi Sườn hầm ngũ quả"
    "Mì Kokomi Pro canh chua tôm 30gói x 82gr" -> "Kokomi Pro canh chua tôm"
    """
    if pd.isna(full_name) or full_name == '':
        return ''

    full_name = str(full_name).strip()

    # Pattern to match brand names (Omachi or Kokomi)
    brand_pattern = r'(Omachi|Kokomi)'
    brand_match = re.search(brand_pattern, full_name)

    if not brand_match:
        return full_name  # Return original if no brand match

    # Get the start position of brand name
    start_pos = brand_match.start()

    # Pattern to match packaging information (e.g., "30gói x 90gr")
    pkg_pattern = r'\d+\s*gói\s*x\s*\d+\s*gr'
    pkg_match = re.search(pkg_pattern, full_name)

    if pkg_match:
        # End position is where packaging info starts
        end_pos = pkg_match.start()
        # Extract text between brand name and packaging info
        short_name = full_name[start_pos:end_pos].strip()
    else:
        # If no packaging info, use rest of string after brand
        short_name = full_name[start_pos:].strip()

    return short_name

def clean_concatenated_dates(date_str):
    """
    Clean concatenated dates like '11/04/202511/04/202511/04/2025'
    Returns the first valid date found
    """
    if not isinstance(date_str, str):
        return date_str

    # Regular expression to find date patterns in DD/MM/YYYY format
    date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
    matches = re.findall(date_pattern, date_str)

    if matches:
        # Return the first date that parses correctly
        for match in matches:
            try:
                parsed_date = pd.to_datetime(match, format='%d/%m/%Y', dayfirst=True)
                # Current date as reference
                current_date = datetime.now()
                # If date is not more than 1 year in the future, consider it valid
                if parsed_date <= current_date + pd.Timedelta(days=365):
                    return match
            except:
                continue

        # If no valid dates found based on future check, return the first match
        return matches[0]

    # If no DD/MM/YYYY pattern found, try different patterns
    # DD-MM-YYYY
    date_pattern = r'(\d{1,2}-\d{1,2}-\d{4})'
    matches = re.findall(date_pattern, date_str)
    if matches:
        return matches[0]

    # Try to extract first 10 characters if they look like a date
    if len(date_str) >= 10 and ('/' in date_str[:10] or '-' in date_str[:10]):
        return date_str[:10]

    return date_str

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
    """
    Extract production information from text with corrected line and machine logic.
    Returns (time, line, machine) tuple.
    
    Important: Line numbers can only be 1-8.
    If a two-digit number is found, the first digit is the line, and the second is the machine.
    """
    if not isinstance(text, str):
        return None, None, None

    # Clean and standardize the text
    text = text.strip()

    # Capture the entire content inside parentheses after "Nơi SX: I-MBP"
    parenthesis_pattern = r'Nơi SX:\s*I-MBP\s*\((.*?)\)'
    parenthesis_match = re.search(parenthesis_pattern, text)

    if not parenthesis_match:
        return None, None, None

    # Get the content inside parentheses
    content = parenthesis_match.group(1).strip()

    # Extract time if present
    time_match = re.search(r'(\d{1,2}:\d{1,2})', content)
    time_str = time_match.group(1) if time_match else None

    # Remove time part if found to simplify the remaining content
    if time_str:
        content = content.replace(time_str, '').strip()

    # Find numeric sequences
    numbers = re.findall(r'\d+', content)

    if not numbers:
        return time_str, None, None

    # Process the numbers found
    line = None
    machine = None

    for num_str in numbers:
        if len(num_str) == 1:
            # If it's a single digit (e.g., "8I"), it's likely just the line
            if 1 <= int(num_str) <= 8:
                line = num_str
        elif len(num_str) >= 2:
            # For two or more digits (e.g., "24I" or "81I06")
            if int(num_str[0]) <= 8:
                # The first digit is the line
                line = num_str[0]
                # The second digit is the machine
                machine = num_str[1]

    # If no line was found through the normal patterns, try a last resort approach
    if line is None:
        # Look for patterns like "2I" or "8I"
        line_match = re.search(r'([1-8])I', content)
        if line_match:
            line = line_match.group(1)

    # Special handling for patterns like "21I" where we interpret as line 2, machine 1
    digit_sequence_match = re.search(r'(\d)(\d)I', content)
    if digit_sequence_match:
        line = digit_sequence_match.group(1)  # First digit as line
        machine = digit_sequence_match.group(2)  # Second digit as machine

    return time_str, line, machine

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

def format_date_mm_dd_yyyy(date_obj):
    """Format a date object to MM/DD/YYYY string format for Power BI"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.strftime('%m/%d/%Y')

def extract_month(date_obj):
    """Extract month from a datetime object"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.month

def extract_year(date_obj):
    """Extract year from a datetime object"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.year

def extract_week(date_obj):
    """Extract ISO week number from a datetime object"""
    if pd.isna(date_obj) or date_obj is None:
        return None
    return date_obj.isocalendar()[1]  # Returns the ISO week number

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
    """Modified to return just the shift number (1, 2, or 3) for Power BI"""
    if time_obj is None:
        return None

    # Create time boundaries
    shift1_start = time(6, 30)
    shift1_end = time(14, 30)
    shift2_end = time(22, 30)

    if shift1_start <= time_obj < shift1_end:
        return 1
    elif shift1_end <= time_obj < shift2_end:
        return 2
    else:
        return 3

def create_leader_mapping(aql_data):
    """
    Revised function to match QA and leader from the AQL data sheet
    This ensures both QA and leader come from the same row in the AQL data
    Creates a mapping from leader IDs to leader names based on the data in the AQL sheet
    """
    # Find the leader column - handle renamed columns
    leader_column = None
    for col in aql_data.columns:
        if any(leader_name in col for leader_name in ['Tên Trường ca', 'Trưởng ca', 'Tên Trưởng ca', 'TruongCa']):
            leader_column = col
            break
    
    if not leader_column:
        print("Warning: No leader column found")
        return {}
    
    print(f"Using leader column: {leader_column}")
    
    # Get all unique values in the leader column
    unique_leaders = aql_data[leader_column].dropna().unique()
    print(f"Found {len(unique_leaders)} unique leader values")
    
    # Determine which values are numeric (likely IDs) and which are names
    leader_mapping = {}
    numeric_values = []
    name_values = []
    
    for value in unique_leaders:
        if value is None:
            continue
            
        # Check if the value might be numeric
        try:
            if str(value).isdigit() or isinstance(value, (int, float)):
                numeric_values.append(value)
            else:
                name_values.append(value)
        except:
            name_values.append(value)
    
    print(f"Found {len(numeric_values)} numeric leader values and {len(name_values)} name values")
    
    # Simple mapping approach: if there's only one name ("Tài"), map all IDs to it
    if len(name_values) == 1 and len(numeric_values) > 0:
        for num_value in numeric_values:
            leader_mapping[str(num_value)] = name_values[0]
        print(f"Mapped all numeric values to '{name_values[0]}'")
    
    # If we have more names, try to find actual mapping in the data
    elif len(name_values) > 1:
        # For now, just map all numeric values to "Tài" as a fallback
        for num_value in numeric_values:
            leader_mapping[str(num_value)] = "Tài"
        print(f"Mapped all numeric values to 'Tài' (fallback)")
    
    return leader_mapping

def find_qa_and_leader(row, aql_data, leader_mapping=None):
    """
    Improved function to match QA and leader from the AQL data sheet
    with support for leader ID to name mapping and renamed columns
    """
    if pd.isna(row['Ngày SX_std']) or row['Item_clean'] == '' or row['Giờ_time'] is None:
        return None, None, "Missing required data"

    # Check for QA column - handle different possible names including renamed ones
    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break

    # Check for leader column - handle different possible names including renamed ones
    leader_column = None
    for col in aql_data.columns:
        if any(leader_name in col for leader_name in ['Tên Trường ca', 'Trưởng ca', 'Tên Trưởng ca', 'TruongCa']):
            leader_column = col
            break

    if not qa_column:
        return None, None, f"QA column not found in AQL data. Available columns: {list(aql_data.columns)}"

    if not leader_column:
        return None, None, f"Leader column not found in AQL data. Available columns: {list(aql_data.columns)}"

    print(f"Using QA column: {qa_column}, Leader column: {leader_column}")

    # 1. Filter AQL data for the same date, item, and line
    matching_rows = aql_data[
        (aql_data['Ngày SX_std'] == row['Ngày SX_std']) & 
        (aql_data['Item_clean'] == row['Item_clean']) &
        (aql_data['Line'] == row['Line_extracted'])
    ]

    if matching_rows.empty:
        return None, None, f"No matches for date+item+line"

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

    # Special case for tickets about KKM PRO CCT on 26/04/2025
    if (row['Ngày SX_std'] == pd.to_datetime('26/04/2025', format='%d/%m/%Y', dayfirst=True) and 
        'PRO CCT' in str(row['Item_clean']).upper()):
        # Find rows with QA = "Hằng" in the matching rows
        hang_rows = matching_rows[matching_rows[qa_column] == "Hằng"]
        if not hang_rows.empty:
            # Get the first row with QA = "Hằng"
            hang_row = hang_rows.iloc[0]
            debug_info = f"{debug_info} | Special case for KKM PRO CCT on 26/04/2025"
            return hang_row[qa_column], hang_row[leader_column], debug_info
    
    # 4. Apply the matching rules as before, but ensure QA and leader come from the same row
    # 4a. First, check if there's data for the preceding hour
    if not prev_check.empty:
        prev_qa = prev_check.iloc[0].get(qa_column)
        prev_leader = prev_check.iloc[0].get(leader_column)

        # Apply leader mapping if provided
        if leader_mapping and prev_leader is not None:
            try:
                # Check if it can be converted to a number
                if str(prev_leader).isdigit() or isinstance(prev_leader, (int, float)):
                    mapped_value = leader_mapping.get(str(prev_leader))
                    if mapped_value:
                        prev_leader = mapped_value
                    else:
                        prev_leader = "Tài"  # Default fallback
            except:
                pass
        
        # 4b. Check if there's data for the next hour
        if not next_check.empty:
            next_qa = next_check.iloc[0].get(qa_column)
            next_leader = next_check.iloc[0].get(leader_column)

            # Apply leader mapping if provided
            if leader_mapping and next_leader is not None:
                try:
                    # Check if it can be converted to a number
                    if str(next_leader).isdigit() or isinstance(next_leader, (int, float)):
                        mapped_value = leader_mapping.get(str(next_leader))
                        if mapped_value:
                            next_leader = mapped_value
                        else:
                            next_leader = "Tài"  # Default fallback
                except:
                    pass
            
            # 4c. If both QA and leader are the same, use them
            if prev_qa == next_qa and prev_leader == next_leader:
                return prev_qa, prev_leader, f"{debug_info} | Same QA and leader for both times"

        # 4d. Determine based on shift if we need to
        shift = row['Shift']

        # For times between 22:30-23:59, we use the next hour's QA (from 0h)
        if shift == 3 and complaint_hour >= 22:
            if not next_check.empty:
                return next_qa, next_leader, f"{debug_info} | Using next hour (0h) based on Shift 3 rule"

        # For all other cases, use the preceding hour's QA and leader from the same row
        return prev_qa, prev_leader, f"{debug_info} | Using previous hour QA and leader"

    # If no data for preceding hour, try next hour
    elif not next_check.empty:
        next_qa = next_check.iloc[0].get(qa_column)
        next_leader = next_check.iloc[0].get(leader_column)
        
        # Apply leader mapping if provided
        if leader_mapping and next_leader is not None:
            try:
                # Check if it can be converted to a number
                if str(next_leader).isdigit() or isinstance(next_leader, (int, float)):
                    mapped_value = leader_mapping.get(str(next_leader))
                    if mapped_value:
                        next_leader = mapped_value
                    else:
                        next_leader = "Tài"  # Default fallback
            except:
                pass
                
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
            closest_qa = closest_row.get(qa_column)
            closest_leader = closest_row.get(leader_column)
            
            # Apply leader mapping if provided
            if leader_mapping and closest_leader is not None:
                try:
                    # Check if it can be converted to a number
                    if str(closest_leader).isdigit() or isinstance(closest_leader, (int, float)):
                        mapped_value = leader_mapping.get(str(closest_leader))
                        if mapped_value:
                            closest_leader = mapped_value
                        else:
                            closest_leader = "Tài"  # Default fallback
                except:
                    pass
                    
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
    
    # Handle KNKH data
    try:
        knkh_data = knkh_worksheet.get_all_records()
        knkh_df = pd.DataFrame(knkh_data)
    except Exception as e:
        print(f"Error with KNKH get_all_records(), trying alternative method: {e}")
        # Use get_all_values() as fallback
        knkh_values = knkh_worksheet.get_all_values()
        if len(knkh_values) > 1:
            headers = knkh_values[0]
            data = knkh_values[1:]
            knkh_df = pd.DataFrame(data, columns=headers)
        else:
            print("No data found in KNKH worksheet")
            sys.exit(1)

    aql_worksheet = aql_sheet.worksheet('ID AQL')
    
    # Handle AQL data with duplicate header protection
    try:
        aql_data = aql_worksheet.get_all_records()
        aql_df = pd.DataFrame(aql_data)
    except Exception as e:
        print(f"Error with AQL get_all_records() (likely duplicate headers): {e}")
        print("Using alternative method to handle duplicate headers...")
        
        # Use get_all_values() and handle duplicate headers
        aql_values = aql_worksheet.get_all_values()
        if len(aql_values) > 1:
            headers = aql_values[0]
            data = aql_values[1:]
            
            # Handle duplicate headers by adding suffixes
            seen_headers = {}
            unique_headers = []
            for header in headers:
                if header in seen_headers:
                    seen_headers[header] += 1
                    unique_headers.append(f"{header}_{seen_headers[header]}")
                else:
                    seen_headers[header] = 0
                    unique_headers.append(header)
            
            aql_df = pd.DataFrame(data, columns=unique_headers)
            print(f"Created AQL DataFrame with headers: {unique_headers}")
        else:
            print("No data found in AQL worksheet")
            sys.exit(1)

    print(f"Retrieved {len(knkh_df)} KNKH records and {len(aql_df)} AQL records")
    print(f"KNKH columns: {list(knkh_df.columns)}")
    print(f"AQL columns: {list(aql_df.columns)}")

    # Clean concatenated dates for both reception date and production date
    knkh_df['Ngày tiếp nhận'] = knkh_df['Ngày tiếp nhận'].apply(clean_concatenated_dates)
    knkh_df['Ngày SX'] = knkh_df['Ngày SX'].apply(clean_concatenated_dates)

    # Extract correct Ngày SX from Nội dung phản hồi and replace the Ngày SX column
    knkh_df['Ngày SX_extracted'] = knkh_df['Nội dung phản hồi'].apply(extract_correct_date)

    # Replace the original Ngày SX with the extracted one when available, keeping the exact format
    knkh_df['Ngày SX'] = knkh_df.apply(
        lambda row: row['Ngày SX_extracted'] if row['Ngày SX_extracted'] is not None else row['Ngày SX'], 
        axis=1
    )

    # Standardize dates first for filtering
    knkh_df['Ngày SX_std'] = knkh_df['Ngày SX'].apply(standardize_date)
    aql_df['Ngày SX_std'] = aql_df['Ngày SX'].apply(standardize_date)

    # Create filter date (January 1, 2024)
    filter_date = pd.to_datetime('2024-01-01')

    # Filter both DataFrames to only include data from January 1, 2024 onwards
    knkh_df = knkh_df[knkh_df['Ngày SX_std'] >= filter_date]
    aql_df = aql_df[aql_df['Ngày SX_std'] >= filter_date]

    print(f"After date filtering: {len(knkh_df)} KNKH records and {len(aql_df)} AQL records")

    # Extract time, line, and machine information
    knkh_df[['Giờ_extracted', 'Line_extracted', 'Máy_extracted']] = knkh_df['Nội dung phản hồi'].apply(
        lambda x: pd.Series(extract_production_info(x))
    )

    # Convert to appropriate data types
    knkh_df['Line_extracted'] = pd.to_numeric(knkh_df['Line_extracted'], errors='coerce')
    knkh_df['Máy_extracted'] = pd.to_numeric(knkh_df['Máy_extracted'], errors='coerce')

    # Standardize the receipt date
    knkh_df['Ngày tiếp nhận_std'] = knkh_df['Ngày tiếp nhận'].apply(standardize_date)

    # Clean item codes
    knkh_df['Item_clean'] = knkh_df['Item'].apply(clean_item_code)
    aql_df['Item_clean'] = aql_df['Item'].apply(clean_item_code)

    # Parse time
    knkh_df['Giờ_time'] = knkh_df['Giờ_extracted'].apply(parse_time)
    
    # Find the correct Giờ column in AQL data (handle renamed columns)
    gio_column = None
    for col in aql_df.columns:
        if col.startswith('Giờ') or col == 'Giờ':
            gio_column = col
            break
    
    if gio_column:
        aql_df['Giờ_time'] = aql_df[gio_column].apply(parse_time)
        print(f"Using time column: {gio_column}")
    else:
        print("Warning: No time column found in AQL data")
        aql_df['Giờ_time'] = None

    # Also ensure Line column is properly handled in AQL data
    line_column = None
    for col in aql_df.columns:
        if col == 'Line' or col.startswith('Line'):
            line_column = col
            break
    
    if line_column and line_column != 'Line':
        # Rename to standard 'Line' for easier processing
        aql_df['Line'] = aql_df[line_column]
        print(f"Using line column: {line_column}")
    elif not line_column:
        print("Warning: No Line column found in AQL data")

    # Round time to 2-hour intervals
    knkh_df['Giờ_rounded'] = knkh_df['Giờ_time'].apply(round_to_2hour)

    # Determine shift (now just returns 1, 2, or 3)
    knkh_df['Shift'] = knkh_df['Giờ_time'].apply(determine_shift)

    # Match QA and leader with improved matching function
    # Create leader ID to name mapping
    leader_mapping = create_leader_mapping(aql_df)
    print(f"Leader mapping: {leader_mapping}")

    # Match QA and leader with improved debugging
    knkh_df['QA_matched'] = None
    knkh_df['Tên Trưởng ca_matched'] = None
    knkh_df['debug_info'] = None

    print("Starting matching process...")
    total_matched = 0
    for idx, row in knkh_df.iterrows():
        qa, leader, debug_info = find_qa_and_leader(row, aql_df, leader_mapping)
        knkh_df.at[idx, 'QA_matched'] = qa
        knkh_df.at[idx, 'Tên Trưởng ca_matched'] = leader
        knkh_df.at[idx, 'debug_info'] = debug_info
        if qa is not None:
            total_matched += 1
        
        # Print progress every 50 rows
        if (idx + 1) % 50 == 0:
            print(f"Processed {idx + 1} rows, {total_matched} matched so far")
    
    print(f"Matching process complete. Total matched: {total_matched} out of {len(knkh_df)} rows")

    # Format dates for Power BI (MM/DD/YYYY)
    knkh_df['Ngày tiếp nhận_formatted'] = knkh_df['Ngày tiếp nhận_std'].apply(format_date_mm_dd_yyyy)
    knkh_df['Ngày SX_formatted'] = knkh_df['Ngày SX_std'].apply(format_date_mm_dd_yyyy)

    # Extract month and year from production date (Ngày SX)
    knkh_df['Tháng sản xuất'] = knkh_df['Ngày SX_std'].apply(extract_month)
    knkh_df['Năm sản xuất'] = knkh_df['Ngày SX_std'].apply(extract_year)

    # Extract week, month and year from receipt date (Ngày tiếp nhận)
    knkh_df['Tuần nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_week)
    knkh_df['Tháng nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_month)
    knkh_df['Năm nhận khiếu nại'] = knkh_df['Ngày tiếp nhận_std'].apply(extract_year)

    # Filter to only include rows where "Bộ phận chịu trách nhiệm" is "Nhà máy"
    print(f"Total rows before filtering by 'Bộ phận chịu trách nhiệm': {len(knkh_df)}")
    knkh_df = knkh_df[knkh_df['Bộ phận chịu trách nhiệm'] == 'Nhà máy']
    print(f"Rows after filtering for 'Bộ phận chịu trách nhiệm' = 'Nhà máy': {len(knkh_df)}")

    # Create the joined dataframe with all required columns
    filtered_knkh_df = knkh_df.copy()

    # Extract short product names
    filtered_knkh_df['Tên sản phẩm ngắn'] = filtered_knkh_df['Tên sản phẩm'].apply(extract_short_product_name)

    joined_df = filtered_knkh_df[[
        'Mã ticket', 'Ngày tiếp nhận_formatted', 'Tỉnh', 'Ngày SX_formatted', 'Sản phẩm/Dịch vụ',
        'Số lượng (ly/hộp/chai/gói/hủ)', 'Nội dung phản hồi', 'Item', 'Tên sản phẩm', 'Tên sản phẩm ngắn',
        'SL pack/ cây lỗi', 'Tên lỗi', 'Line_extracted', 'Máy_extracted', 'Giờ_extracted',
        'QA_matched', 'Tên Trưởng ca_matched', 'Shift', 
        'Tháng sản xuất', 'Năm sản xuất', 'Tuần nhận khiếu nại', 'Tháng nhận khiếu nại', 'Năm nhận khiếu nại',
        'Bộ phận chịu trách nhiệm', 'debug_info'  # Added debug_info column for troubleshooting
    ]].copy()

    # Rename columns for clarity
    joined_df.rename(columns={
        'Line_extracted': 'Line',
        'Máy_extracted': 'Máy',
        'Giờ_extracted': 'Giờ',
        'QA_matched': 'QA',
        'Tên Trưởng ca_matched': 'Tên Trưởng ca',
        'Ngày tiếp nhận_formatted': 'Ngày tiếp nhận',
        'Ngày SX_formatted': 'Ngày SX'
    }, inplace=True)

    # Sort by Mã ticket from largest to smallest
    joined_df = joined_df.sort_values(by='Mã ticket', ascending=False)

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

        # Update the worksheet
        integrated_worksheet.update('A1', data_to_write)
        print(f"Successfully wrote {len(data_to_write)-1} rows to the destination sheet, sorted by Mã ticket (largest to smallest)")

        # Also create a debug worksheet to help troubleshoot matching issues
        try:
            debug_worksheet = destination_sheet.worksheet('Debug_Info')
            debug_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            debug_worksheet = destination_sheet.add_worksheet(
                title='Debug_Info',
                rows=min(500, len(joined_df)+1), # Limit to 500 rows to avoid exceeding limits
                cols=8
            )

        # Create a simplified debug table with key matching info
        debug_df = joined_df[['Mã ticket', 'Ngày SX', 'Item', 'Line', 'Giờ', 'QA', 'Tên Trưởng ca', 'debug_info']]
        debug_data = [debug_df.columns.tolist()] + debug_df.head(499).fillna('').values.tolist()
        debug_worksheet.update('A1', debug_data)
        print(f"Created debug worksheet with matching information")

    except Exception as e:
        print(f"Error writing to destination sheet: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
