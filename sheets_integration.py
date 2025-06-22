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
    Extract production information from text with improved line and machine logic.
    Returns (time, line, machine) tuple.
    
    FIXED: Now handles spaces around colons in time patterns like "23 :12"
    
    Handles patterns like:
    - "21:17 22I" -> time="21:17", line="2", machine="2"
    - "23 :12 23I" -> time="23:12", line="2", machine="3" (NEW FIX)
    - "Nơi SX: I-MBP (8I06)" -> line="8", machine=None
    - "Nơi SX: I-MBP (13:19 23)" -> time="13:19", line="2", machine="3"
    - "Nơi SX: I-MBP (14:27 21 I )" -> time="14:27", line="2", machine="1"
    - "Nơi SX: I-MBP (22:51 24 I )" -> time="22:51", line="2", machine="4"
    
    NEW IMPROVEMENT: Now handles 2-digit codes after time where:
    - First digit = line number (1-8)
    - Second digit = machine number (0-9)
    """
    if not isinstance(text, str):
        return None, None, None

    # Clean and standardize the text
    text = text.strip()
    
    # FIXED: Extract time first (anywhere in the text) - now handles spaces around colon
    time_match = re.search(r'(\d{1,2}\s*:\s*\d{1,2})', text)
    time_str = None
    if time_match:
        # Clean up the time string by removing spaces
        raw_time = time_match.group(1)
        time_str = re.sub(r'\s*:\s*', ':', raw_time)  # Replace " : " or " :" or ": " with ":"

    # Try to find line and machine info in different patterns
    line = None
    machine = None

    # Pattern 1: Look for content inside parentheses after "Nơi SX: I-MBP"
    parenthesis_pattern = r'Nơi SX:\s*I-MBP\s*\((.*?)\)'
    parenthesis_match = re.search(parenthesis_pattern, text)
    
    if parenthesis_match:
        content = parenthesis_match.group(1).strip()
        
        # NEW PATTERN: Look for 2-digit numbers after time (like "13:19 23" or "14:27 21")
        if time_str:
            # Look for pattern: time followed by space and 2-digit number
            # Use the cleaned time_str for pattern matching
            time_number_pattern = rf'{re.escape(time_str)}\s+(\d{{2}})'
            time_number_match = re.search(time_number_pattern, content)
            if time_number_match:
                digits = time_number_match.group(1)
                first_digit = int(digits[0])
                second_digit = int(digits[1])
                
                # Check if first digit is valid line number (1-8)
                if 1 <= first_digit <= 8:
                    line = str(first_digit)
                    machine = str(second_digit)
                    return time_str, line, machine
            
            # ALSO try with the original raw time pattern to catch cases like "23 :12 23"
            if raw_time != time_str:
                raw_time_pattern = rf'{re.escape(raw_time)}\s+(\d{{2}})'
                raw_time_match = re.search(raw_time_pattern, content)
                if raw_time_match:
                    digits = raw_time_match.group(1)
                    first_digit = int(digits[0])
                    second_digit = int(digits[1])
                    
                    # Check if first digit is valid line number (1-8)
                    if 1 <= first_digit <= 8:
                        line = str(first_digit)
                        machine = str(second_digit)
                        return time_str, line, machine
        
        # EXISTING PATTERN: Look for patterns like "8I06", "21I", "2I", etc.
        # Remove time part if found to simplify processing
        if time_str:
            content_for_i_pattern = content.replace(time_str, '').strip()
            # Also remove the raw time pattern if different
            if time_match and time_match.group(1) != time_str:
                content_for_i_pattern = content_for_i_pattern.replace(time_match.group(1), '').strip()
        else:
            content_for_i_pattern = content
        
        line_machine_match = re.search(r'(\d+)I', content_for_i_pattern)
        if line_machine_match:
            digits = line_machine_match.group(1)
            if len(digits) == 1 and 1 <= int(digits) <= 8:
                line = digits
            elif len(digits) >= 2:
                first_digit = int(digits[0])
                if 1 <= first_digit <= 8:
                    line = digits[0]
                    if len(digits) >= 2:
                        machine = digits[1]
    
    # Pattern 2: If no parentheses, look for patterns like "22I" directly in text
    if line is None:
        # Look for patterns like "22I", "8I", "21I" anywhere in the text
        line_pattern = r'(\d+)I(?!\w)'  # \d+I not followed by word character
        line_matches = re.findall(line_pattern, text)
        
        for match in line_matches:
            if len(match) == 1 and 1 <= int(match) <= 8:
                line = match
                break
            elif len(match) >= 2:
                first_digit = int(match[0])
                if 1 <= first_digit <= 8:
                    line = match[0]
                    if len(match) >= 2:
                        machine = match[1]
                    break
    
    # Pattern 3: Look for "Nơi SX: MBP" and then search around it
    if line is None and "Nơi SX: MBP" in text:
        # Find position of "Nơi SX: MBP" and look around it
        mbp_pos = text.find("Nơi SX: MBP")
        surrounding_text = text[max(0, mbp_pos-20):mbp_pos+50]
        
        # First try the new 2-digit pattern in surrounding text
        if time_str:
            time_number_pattern = rf'{re.escape(time_str)}\s+(\d{{2}})'
            time_number_match = re.search(time_number_pattern, surrounding_text)
            if time_number_match:
                digits = time_number_match.group(1)
                first_digit = int(digits[0])
                second_digit = int(digits[1])
                
                if 1 <= first_digit <= 8:
                    line = str(first_digit)
                    machine = str(second_digit)
                    return time_str, line, machine
        
        # Fall back to looking for "I" patterns in surrounding text
        line_pattern = r'(\d+)I'
        line_match = re.search(line_pattern, surrounding_text)
        if line_match:
            digits = line_match.group(1)
            if len(digits) == 1 and 1 <= int(digits) <= 8:
                line = digits
            elif len(digits) >= 2:
                first_digit = int(digits[0])
                if 1 <= first_digit <= 8:
                    line = digits[0]
                    if len(digits) >= 2:
                        machine = digits[1]

    return time_str, line, machine

def standardize_date(date_str):
    """Improved date standardization with explicit format handling"""
    try:
        if isinstance(date_str, str):
            date_str = date_str.strip()
            
            # Handle DD/MM/YYYY format specifically
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                return pd.to_datetime(date_str, format='%d/%m/%Y')
            
            # Handle MM/DD/YYYY format specifically  
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                # Try DD/MM/YYYY first (since dayfirst=True)
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y')
                except:
                    # Fall back to MM/DD/YYYY
                    return pd.to_datetime(date_str, format='%m/%d/%Y')
            
            # Handle DD-MMM-YYYY format (e.g., "4-Apr-2025")
            if '-' in date_str:
                for fmt in ['%d-%b-%Y', '%d-%B-%Y', '%d-%b-%y', '%d-%B-%y']:
                    try:
                        return pd.to_datetime(date_str, format=fmt)
                    except:
                        continue

            # Last resort: Let pandas try to detect, but suppress warnings
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
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
    Creates a mapping from leader codes to leader names based on the actual data in the AQL sheet
    by examining what Tên Trưởng ca values appear next to each QA in the same rows
    IMPORTANT: Uses "Tên Trưởng ca" column (with names) not "Trưởng ca" column (with codes)
    """
    # Find the leader NAME column specifically (Tên Trưởng ca) - prioritize this over codes
    leader_name_column = None
    leader_code_column = None
    
    for col in aql_data.columns:
        col_lower = col.lower()
        if 'tên trưởng ca' in col_lower or 'ten truong ca' in col_lower:
            leader_name_column = col
        elif ('trưởng ca' in col_lower or 'truong ca' in col_lower) and 'tên' not in col_lower:
            leader_code_column = col
    
    # Find the QA column
    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break
    
    print(f"Found columns:")
    print(f"  QA column: {qa_column}")
    print(f"  Leader NAME column (Tên Trưởng ca): {leader_name_column}")
    print(f"  Leader CODE column (Trưởng ca): {leader_code_column}")
    
    # Use the name column if available, otherwise fall back to code column
    if leader_name_column:
        leader_column = leader_name_column
        print(f"✓ Using leader NAME column: {leader_column}")
    elif leader_code_column:
        leader_column = leader_code_column
        print(f"⚠ Using leader CODE column: {leader_column} (names not found)")
    else:
        print("❌ No leader column found")
        return {}
    
    if not qa_column:
        print("❌ No QA column found")
        return {}
    
    # Create mapping by examining actual data relationships
    leader_mapping = {}
    
    # Get unique combinations of QA and leader from the data
    qa_leader_combinations = aql_data[[qa_column, leader_column]].dropna().drop_duplicates()
    
    print(f"\nFound {len(qa_leader_combinations)} unique QA-Leader combinations:")
    for idx, row in qa_leader_combinations.iterrows():
        qa_val = row[qa_column]
        leader_val = row[leader_column]
        print(f"  QA: '{qa_val}' -> Leader: '{leader_val}'")
        
        # Store the mapping (keep original values since we're now using the name column)
        leader_mapping[str(leader_val)] = str(leader_val)
    
    # If we're using the name column, we don't need complex mapping
    # If we're using the code column, we might need to create mappings from codes to names
    if leader_name_column:
        print(f"\n✓ Using actual names from {leader_name_column}, no mapping needed")
    else:
        print(f"\n⚠ Using codes from {leader_code_column}, might need mapping logic")
    
    print(f"Final leader mapping: {leader_mapping}")
    return leader_mapping

def find_qa_and_leader(row, aql_data, leader_mapping=None):
    """
    Improved function to match QA and leader from the AQL data sheet
    with better debugging, data type handling, and night shift date adjustment
    """
    if pd.isna(row['Ngày SX_std']) or row['Item_clean'] == '' or row['Giờ_time'] is None:
        return None, None, "Missing required data"

    # Check for QA column - handle different possible names including renamed ones
    qa_column = None
    for col in aql_data.columns:
        if col == 'QA' or col.startswith('QA'):
            qa_column = col
            break

    # Check for leader column - prioritize "Tên Trưởng ca" (names) over "Trưởng ca" (codes)
    leader_name_column = None
    leader_code_column = None
    
    for col in aql_data.columns:
        col_lower = col.lower()
        if 'tên trưởng ca' in col_lower or 'ten truong ca' in col_lower:
            leader_name_column = col
        elif ('trưởng ca' in col_lower or 'truong ca' in col_lower) and 'tên' not in col_lower:
            leader_code_column = col
    
    # Use the name column if available, otherwise fall back to code column
    if leader_name_column:
        leader_column = leader_name_column
    elif leader_code_column:
        leader_column = leader_code_column
    else:
        leader_column = None

    if not qa_column:
        return None, None, f"QA column not found in AQL data. Available columns: {list(aql_data.columns)}"

    if not leader_column:
        return None, None, f"Leader column not found in AQL data. Available columns: {list(aql_data.columns)}"
    
    # Debug info about which columns we're using
    debug_parts = []
    if leader_name_column:
        debug_parts.append(f"Using QA column: {qa_column}, Leader NAME column: {leader_column}")
    else:
        debug_parts.append(f"Using QA column: {qa_column}, Leader CODE column: {leader_column}")

    # Convert Line_extracted to proper integer for comparison
    complaint_line = row['Line_extracted']
    if pd.isna(complaint_line):
        return None, None, "Missing line information"
    
    try:
        complaint_line = int(float(complaint_line))  # Handle cases where it might be stored as float
    except (ValueError, TypeError):
        return None, None, f"Invalid line value: {complaint_line}"

    # **NEW: Handle night shift date adjustment**
    complaint_hour = row['Giờ_time'].hour
    complaint_minute = row['Giờ_time'].minute
    search_date = row['Ngày SX_std']
    
    # If complaint is in early morning hours (0:00 to 6:30) and in shift 3,
    # we should look at the previous day's AQL data
    if complaint_hour < 6 or (complaint_hour == 6 and complaint_minute < 30):
        if row['Shift'] == 3:
            search_date = search_date - pd.Timedelta(days=1)
            date_adjusted = True
        else:
            date_adjusted = False
    else:
        date_adjusted = False

    # Debug information
    debug_parts = []
    if date_adjusted:
        debug_parts.append(f"NIGHT SHIFT ADJUSTMENT: Looking for: Date={search_date.strftime('%d/%m/%Y')} (adjusted from {row['Ngày SX_std'].strftime('%d/%m/%Y')}), Item={row['Item_clean']}, Line={complaint_line}")
    else:
        debug_parts.append(f"Looking for: Date={search_date.strftime('%d/%m/%Y')}, Item={row['Item_clean']}, Line={complaint_line}")

    # 1. Filter AQL data for the same date and item first (using potentially adjusted date)
    date_item_matches = aql_data[
        (aql_data['Ngày SX_std'] == search_date) & 
        (aql_data['Item_clean'] == row['Item_clean'])
    ]
    
    debug_parts.append(f"Date+Item matches: {len(date_item_matches)}")
    
    if date_item_matches.empty:
        # Try with date only to see if date matching works
        date_only_matches = aql_data[aql_data['Ngày SX_std'] == search_date]
        debug_parts.append(f"Date-only matches: {len(date_only_matches)}")
        
        # Try with item only to see if item matching works
        item_only_matches = aql_data[aql_data['Item_clean'] == row['Item_clean']]
        debug_parts.append(f"Item-only matches: {len(item_only_matches)}")
        
        return None, None, " | ".join(debug_parts)

    # 2. Now filter by line - both should be numeric now
    matching_rows = date_item_matches[date_item_matches['Line'] == complaint_line]
    
    debug_parts.append(f"Date+Item+Line matches: {len(matching_rows)}")
    
    if matching_rows.empty:
        # Show available lines for this date+item combination
        available_lines = date_item_matches['Line'].dropna().unique()
        debug_parts.append(f"Available lines for this date+item: {sorted([x for x in available_lines if pd.notna(x)])}")
        return None, None, " | ".join(debug_parts)

    # 3. Determine which QA check hours to look at
    if complaint_minute == 0 and complaint_hour % 2 == 0:
        prev_hour = complaint_hour
        next_hour = (complaint_hour + 2) % 24
    else:
        prev_hour = (complaint_hour // 2) * 2
        next_hour = (prev_hour + 2) % 24

    debug_parts.append(f"Complaint at {complaint_hour}:{complaint_minute:02d}, checking {prev_hour}h and {next_hour}h")

    # 4. Find QA records at these times
    prev_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == prev_hour and x.minute == 0)]
    next_check = matching_rows[matching_rows['Giờ_time'].apply(lambda x: x is not None and x.hour == next_hour and x.minute == 0)]

    debug_parts.append(f"Prev hour ({prev_hour}h) records: {len(prev_check)}, Next hour ({next_hour}h) records: {len(next_check)}")

    # Show available times for debugging
    available_times = matching_rows[matching_rows['Giờ_time'].notna()]['Giờ_time'].apply(lambda x: f"{x.hour}:{x.minute:02d}").unique()
    debug_parts.append(f"Available times: {sorted(available_times)}")

    # Special case for tickets about KKM PRO CCT on 26/04/2025
    if (search_date == pd.to_datetime('26/04/2025', format='%d/%m/%Y') and 
        'PRO CCT' in str(row['Item_clean']).upper()):
        # Find rows with QA = "Hằng" in the matching rows
        hang_rows = matching_rows[matching_rows[qa_column] == "Hằng"]
        if not hang_rows.empty:
            # Get the first row with QA = "Hằng"
            hang_row = hang_rows.iloc[0]
            debug_parts.append("Special case for KKM PRO CCT on 26/04/2025")
            leader_value = hang_row[leader_column]
            # Apply leader mapping
            if leader_mapping and leader_value is not None:
                mapped_leader = leader_mapping.get(str(leader_value), leader_value)
            else:
                mapped_leader = leader_value
            return hang_row[qa_column], mapped_leader, " | ".join(debug_parts)
    
    # 5. Apply the matching rules
    # 5a. First, check if there's data for the preceding hour
    if not prev_check.empty:
        prev_qa = prev_check.iloc[0].get(qa_column)
        prev_leader = prev_check.iloc[0].get(leader_column)

        # Apply leader mapping if provided
        if leader_mapping and prev_leader is not None:
            prev_leader = leader_mapping.get(str(prev_leader), prev_leader)
        
        # 5b. Check if there's data for the next hour
        if not next_check.empty:
            next_qa = next_check.iloc[0].get(qa_column)
            next_leader = next_check.iloc[0].get(leader_column)

            # Apply leader mapping if provided
            if leader_mapping and next_leader is not None:
                next_leader = leader_mapping.get(str(next_leader), next_leader)
            
            # 5c. If both QA and leader are the same, use them
            if prev_qa == next_qa and prev_leader == next_leader:
                debug_parts.append(f"Same QA ({prev_qa}) and leader ({prev_leader}) for both {prev_hour}h and {next_hour}h")
                return prev_qa, prev_leader, " | ".join(debug_parts)

        # 5d. Determine based on shift if we need to
        shift = row['Shift']

        # For times between 22:30-23:59, we use the next hour's QA (from 0h)
        if shift == 3 and complaint_hour >= 22:
            if not next_check.empty:
                debug_parts.append(f"Using next hour ({next_hour}h) QA ({next_qa}) and leader ({next_leader}) based on Shift 3 rule")
                return next_qa, next_leader, " | ".join(debug_parts)

        # For all other cases, use the preceding hour's QA and leader from the same row
        debug_parts.append(f"Using previous hour ({prev_hour}h) QA ({prev_qa}) and leader ({prev_leader})")
        return prev_qa, prev_leader, " | ".join(debug_parts)

    # If no data for preceding hour, try next hour
    elif not next_check.empty:
        next_qa = next_check.iloc[0].get(qa_column)
        next_leader = next_check.iloc[0].get(leader_column)
        
        # Apply leader mapping if provided
        if leader_mapping and next_leader is not None:
            next_leader = leader_mapping.get(str(next_leader), next_leader)
        
        debug_parts.append(f"Only next hour ({next_hour}h) data available - QA ({next_qa}) and leader ({next_leader})")
        return next_qa, next_leader, " | ".join(debug_parts)

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
                closest_leader = leader_mapping.get(str(closest_leader), closest_leader)
            
            closest_time = f"{closest_row['Giờ_time'].hour}:{closest_row['Giờ_time'].minute:02d}"
            debug_parts.append(f"Using closest time match at {closest_time} - QA ({closest_qa}) and leader ({closest_leader})")
            return closest_qa, closest_leader, " | ".join(debug_parts)

    debug_parts.append("No matching QA records found")
    return None, None, " | ".join(debug_parts)

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

    # Extract time, line, and machine information using improved function
    knkh_df[['Giờ_extracted', 'Line_extracted', 'Máy_extracted']] = knkh_df['Nội dung phản hồi'].apply(
        lambda x: pd.Series(extract_production_info(x))
    )

    # Test the extraction for specific patterns mentioned by user
    test_texts = [
        "Nơi SX: I-MBP (13:19 23)",
        "Nơi SX: I-MBP (14:27 21 I )",
        "Nơi SX: I-MBP (22:51 24 I )",
        "21:17 22I",
        "Nơi SX: I-MBP (23 :12 23I)"  # NEW TEST CASE from user
    ]
    
    print("\nTesting improved extraction function (including the fix for spaces around colon):")
    for test_text in test_texts:
        test_time, test_line, test_machine = extract_production_info(test_text)
        print(f"'{test_text}' -> Time={test_time}, Line={test_line}, Machine={test_machine}")

    # Convert to appropriate data types
    knkh_df['Line_extracted'] = pd.to_numeric(knkh_df['Line_extracted'], errors='coerce')
    knkh_df['Máy_extracted'] = pd.to_numeric(knkh_df['Máy_extracted'], errors='coerce')
    
    # Debug: Show some extraction results
    print("\nSample production info extractions:")
    for idx in knkh_df.head(5).index:
        row = knkh_df.loc[idx]
        print(f"Ticket {row['Mã ticket']}: '{row['Giờ_extracted']}' -> Line: {row['Line_extracted']}, Machine: {row['Máy_extracted']}")
    print()

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

    # FIXED: Convert AQL Line column to numeric BEFORE any sorting operations
    if 'Line' in aql_df.columns:
        aql_df['Line'] = pd.to_numeric(aql_df['Line'], errors='coerce')
        print(f"Converted Line column to numeric")

    # Debug: Check AQL data sample to understand structure
    print("\nAQL Data Sample:")
    if len(aql_df) > 0:
        print(f"Columns: {list(aql_df.columns)}")
        sample_aql = aql_df.head(3)
        for idx, row in sample_aql.iterrows():
            print(f"Row {idx}: Date={row.get('Ngày SX')}, Item={row.get('Item')}, Line={row.get('Line')}, Time={row.get(gio_column) if gio_column else 'N/A'}")
        print()
    
    # Debug: Check data types - NOW SAFE because Line is already converted to numeric
    print("Data type checking:")
    print(f"AQL Line column type: {aql_df['Line'].dtype if 'Line' in aql_df.columns else 'N/A'}")
    
    # FIXED: Safe sorting - only include non-null numeric values
    if 'Line' in aql_df.columns:
        valid_lines = aql_df['Line'].dropna()
        if len(valid_lines) > 0:
            print(f"AQL Line unique values: {sorted(valid_lines.unique())}")
        else:
            print("AQL Line unique values: No valid numeric values found")
    else:
        print("AQL Line unique values: N/A")
    print()

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
    
    # Show some sample matches for debugging
    matched_rows = knkh_df[knkh_df['QA_matched'].notna()]
    if len(matched_rows) > 0:
        print("\nSample matched records:")
        for idx in matched_rows.head(3).index:
            row = knkh_df.loc[idx]
            print(f"Ticket {row['Mã ticket']}: Date={row['Ngày SX']}, Item={row['Item']}, Line={row['Line_extracted']}, Time={row['Giờ_extracted']} -> QA={row['QA_matched']}, Leader={row['Tên Trưởng ca_matched']}")
    
    unmatched_rows = knkh_df[knkh_df['QA_matched'].isna()]
    if len(unmatched_rows) > 0:
        print(f"\nSample unmatched records ({len(unmatched_rows)} total):")
        for idx in unmatched_rows.head(5).index:  # Show more unmatched records
            row = knkh_df.loc[idx]
            print(f"Ticket {row['Mã ticket']}: Date={row['Ngày SX']}, Item={row['Item']}, Line={row['Line_extracted']}, Time={row['Giờ_extracted']}")
            print(f"  Debug: {row['debug_info']}")
            print()

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
