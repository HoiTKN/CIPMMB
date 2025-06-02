import pandas as pd
import gspread
import os
import sys
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from datetime import datetime, time

# Define the scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def parse_mdg_values(mdg_str):
    """
    Parse MĐG values that can be single values or comma-separated values like '1,2' or '3,4'
    Returns a list of integer MĐG values
    """
    if pd.isna(mdg_str) or mdg_str is None:
        return []
    
    try:
        # Convert to string and clean up
        mdg_str = str(mdg_str).strip()
        
        # Check if it contains comma (multiple values)
        if ',' in mdg_str:
            # Split by comma and convert each to int
            mdg_values = []
            for value in mdg_str.split(','):
                try:
                    mdg_val = int(float(value.strip()))
                    mdg_values.append(mdg_val)
                except (ValueError, TypeError):
                    continue
            return mdg_values
        else:
            # Single value
            mdg_val = int(float(mdg_str))
            return [mdg_val]
    except (ValueError, TypeError):
        return []

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

def get_sheet_data_robust(worksheet, sheet_name="Unknown"):
    """
    Robust function to get data from worksheet, handling duplicate or empty headers
    """
    try:
        print(f"Attempting to get data from sheet: {sheet_name}")
        
        # First, try the normal method
        try:
            data = worksheet.get_all_records()
            print(f"Successfully retrieved data from {sheet_name} using get_all_records()")
            return data
        except Exception as e:
            print(f"get_all_records() failed for {sheet_name}: {str(e)}")
            print(f"Trying alternative method...")
            
        # Alternative method: get all values and handle headers manually
        all_values = worksheet.get_all_values()
        if not all_values:
            print(f"Warning: No data found in {sheet_name}")
            return []
            
        # Get headers and clean them up
        headers = all_values[0]
        data_rows = all_values[1:]
        
        # Clean up headers - handle duplicates and empty values
        cleaned_headers = []
        header_counts = {}
        
        for i, header in enumerate(headers):
            # Convert to string and strip whitespace
            header = str(header).strip()
            
            # Handle empty headers
            if header == '' or header == 'nan':
                header = f'Column_{i+1}'
            
            # Handle duplicates by adding a suffix
            original_header = header
            counter = 1
            while header in header_counts:
                header = f"{original_header}_{counter}"
                counter += 1
            
            header_counts[header] = 1
            cleaned_headers.append(header)
        
        print(f"Cleaned headers for {sheet_name}: {cleaned_headers}")
        
        # Convert to list of dictionaries
        data = []
        for row in data_rows:
            # Ensure row has same length as headers
            while len(row) < len(cleaned_headers):
                row.append('')
            
            # Create dictionary for this row
            row_dict = {}
            for i, header in enumerate(cleaned_headers):
                row_dict[header] = row[i] if i < len(row) else ''
            
            data.append(row_dict)
        
        print(f"Successfully processed {len(data)} rows from {sheet_name}")
        return data
        
    except Exception as e:
        print(f"Error retrieving data from {sheet_name}: {str(e)}")
        return []

def get_day_of_month(date):
    """Extract day from date"""
    if pd.isna(date) or date is None:
        return None
    return date.day

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
        print("Applying table formatting...")
        time.sleep(2)  # Add delay to avoid rate limits
        
        # Get the number of rows and columns
        rows = worksheet.row_count
        cols = worksheet.col_count
        
        # Only format header row to minimize API calls
        worksheet.format("A1:Z1", {
            "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}
        })
        
        print("Successfully applied header formatting")
        
    except Exception as e:
        print(f"Warning: Could not apply table formatting (this is non-critical): {str(e)}")
        # Continue without formatting - this won't affect the data

def create_mapping_key_with_hour_logic(row, sample_id_df):
    """Create a mapping key considering extended shift logic and MĐG grouping based on actual working hours"""
    try:
        # Standardize the date
        date_std = standardize_date(row['Ngày SX'])
        if date_std is None:
            return None
        
        date_key = date_std.strftime('%d/%m/%Y')
        
        # Parse hour from the main AQL data
        hour = parse_hour(row.get('Giờ', ''))
        if hour is None:
            return None
            
        line = int(float(row['Line'])) if pd.notna(row['Line']) else None
        
        # IMPROVED: Handle comma-separated MĐG values
        mdg_values = parse_mdg_values(row.get('MĐG', ''))
        if not mdg_values:
            return None
        
        if line is None:
            return None
        
        # Determine which shift codes to look for based on the hour
        # Priority order: normal shift first, then extended shifts as fallback
        possible_shift_codes = []
        
        if 6 <= hour < 14:  # Ca 1 time (6h-14h)
            possible_shift_codes = [1, 14]  # Ca 1 first, then Ca 14 as fallback
        elif 14 <= hour < 18:  # Ca 2 time but also covered by Ca 14 (14h-18h)
            possible_shift_codes = [2, 14]  # Ca 2 first, then Ca 14 as fallback
        elif 18 <= hour < 22:  # Ca 2 time but also covered by Ca 34 (18h-22h)
            possible_shift_codes = [2, 34]  # Ca 2 first, then Ca 34 as fallback
        elif 22 <= hour <= 23:  # Ca 3 time (22h-23h)
            possible_shift_codes = [3, 34]  # Ca 3 first, then Ca 34 as fallback
        elif 0 <= hour < 6:  # Ca 3 time (0h-6h)
            possible_shift_codes = [3, 34]  # Ca 3 first, then Ca 34 as fallback
        else:
            # Fallback to normal shift determination
            ca = determine_shift(hour)
            if ca:
                possible_shift_codes = [ca]
        
        # IMPROVED: Handle multiple MĐG values
        # For each MĐG value from the parsed list, determine lookup values
        all_lookup_mdg_values = set()
        for mdg in mdg_values:
            mdg_lookup_values = []
            if mdg == 2:
                # MĐG 2 should look for MĐG 1 in sample sheet (since MĐG 1 covers 1,2)
                mdg_lookup_values = [1, 2]  # Try 1 first, then 2 as fallback
            elif mdg == 4:
                # MĐG 4 should look for MĐG 3 in sample sheet (since MĐG 3 covers 3,4)
                mdg_lookup_values = [3, 4]  # Try 3 first, then 4 as fallback
            else:
                mdg_lookup_values = [mdg]
            
            all_lookup_mdg_values.update(mdg_lookup_values)
        
        # Try to find a match in sample_id_df for any combination of shift codes and MĐG values
        for shift_code in possible_shift_codes:
            for lookup_mdg in all_lookup_mdg_values:
                # Check if there's a matching record in sample_id_df
                try:
                    matching_records = sample_id_df[
                        (sample_id_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_key) &
                        (sample_id_df['Ca'].astype(str).str.strip() == str(shift_code)) &
                        (sample_id_df['Line'].astype(str).str.strip() == str(line)) &
                        (sample_id_df['MĐG'].astype(str).str.strip() == str(lookup_mdg))
                    ]
                    
                    if not matching_records.empty:
                        # Return the key using the first MĐG value from the original data
                        return (date_key, shift_code, line, mdg_values[0])
                except Exception as e:
                    continue
        
        return None
        
    except (ValueError, TypeError, KeyError):
        return None

def create_simple_mapping_key(row):
    """Create mapping keys for sample_id_df records, handling MĐG grouping logic"""
    try:
        date_std = standardize_date(row['Ngày SX'])
        if date_std is None:
            return []
        
        date_key = date_std.strftime('%d/%m/%Y')
        ca = int(float(row['Ca'])) if pd.notna(row['Ca']) else None
        line = int(float(row['Line'])) if pd.notna(row['Line']) else None
        
        # IMPROVED: Handle comma-separated MĐG values in sample data
        mdg_values = parse_mdg_values(row.get('MĐG', ''))
        if not mdg_values:
            return []
        
        if ca is None or line is None:
            return []
        
        # Handle MĐG grouping logic for each MĐG value
        keys = []
        for mdg in mdg_values:
            if mdg == 1:
                # MĐG 1 covers both MĐG 1 and MĐG 2
                keys.append((date_key, ca, line, 1))
                keys.append((date_key, ca, line, 2))
            elif mdg == 3:
                # MĐG 3 covers both MĐG 3 and MĐG 4
                keys.append((date_key, ca, line, 3))
                keys.append((date_key, ca, line, 4))
            else:
                # For other MĐG values, use as-is
                keys.append((date_key, ca, line, mdg))
            
        # Validate that all keys are properly formed tuples
        validated_keys = []
        for key in keys:
            if isinstance(key, tuple) and len(key) == 4:
                validated_keys.append(key)
            else:
                print(f"Warning: Invalid key format generated: {key}")
                
        return validated_keys
        
    except (ValueError, TypeError, KeyError) as e:
        print(f"Warning: Error in create_simple_mapping_key: {e}")
        return []

def expand_dataframe_for_multiple_mdg(df):
    """
    Expand dataframe rows that have comma-separated MĐG values into separate rows
    """
    expanded_rows = []
    
    for _, row in df.iterrows():
        mdg_values = parse_mdg_values(row.get('MĐG', ''))
        
        if len(mdg_values) <= 1:
            # Single or no MĐG value, keep row as-is
            expanded_rows.append(row)
        else:
            # Multiple MĐG values, create separate row for each
            for mdg_val in mdg_values:
                new_row = row.copy()
                new_row['MĐG'] = mdg_val
                new_row['MĐG_Original'] = row['MĐG']  # Keep original value for reference
                expanded_rows.append(new_row)
    
    return pd.DataFrame(expanded_rows)

def find_representative_production_data(sample_date, sample_ca, sample_line, sample_mdg, existing_aql_df):
    """
    Find representative production data (Sản phẩm, Item, Giờ, etc.) for a given date/shift/line/MĐG combination
    """
    try:
        # Convert sample data to matching format
        date_str = sample_date.strftime('%d/%m/%Y') if sample_date else None
        if not date_str:
            return None
            
        ca_str = str(sample_ca).strip()
        line_str = str(sample_line).strip()
        mdg_str = str(sample_mdg).strip()
        
        # Look for matching production records in existing AQL data
        # Priority 1: Exact match on date, shift, line, MĐG
        matching_records = existing_aql_df[
            (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
            (existing_aql_df['Ca'].astype(str).str.strip() == ca_str) &
            (existing_aql_df['Line'].astype(str).str.strip() == line_str) &
            (existing_aql_df['MĐG'].astype(str).str.strip() == mdg_str)
        ]
        
        # Priority 2: If no exact MĐG match, try MĐG grouping logic
        if matching_records.empty:
            try:
                mdg_val = int(float(sample_mdg))
                # Handle MĐG grouping (1 covers 1,2 and 3 covers 3,4)
                if mdg_val == 2:
                    # Look for MĐG 1 records
                    matching_records = existing_aql_df[
                        (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
                        (existing_aql_df['Ca'].astype(str).str.strip() == ca_str) &
                        (existing_aql_df['Line'].astype(str).str.strip() == line_str) &
                        (existing_aql_df['MĐG'].astype(str).str.strip() == '1')
                    ]
                elif mdg_val == 4:
                    # Look for MĐG 3 records
                    matching_records = existing_aql_df[
                        (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
                        (existing_aql_df['Ca'].astype(str).str.strip() == ca_str) &
                        (existing_aql_df['Line'].astype(str).str.strip() == line_str) &
                        (existing_aql_df['MĐG'].astype(str).str.strip() == '3')
                    ]
            except:
                pass
        
        # Priority 3: If still no match, try same date and shift (any line/MĐG)
        if matching_records.empty:
            matching_records = existing_aql_df[
                (existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str) &
                (existing_aql_df['Ca'].astype(str).str.strip() == ca_str)
            ]
        
        # Priority 4: If still no match, try same date (any shift)
        if matching_records.empty:
            matching_records = existing_aql_df[
                existing_aql_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_str
            ]
        
        # Priority 5: If still no match, try same line (any date)
        if matching_records.empty:
            matching_records = existing_aql_df[
                existing_aql_df['Line'].astype(str).str.strip() == line_str
            ]
        
        # Return the first matching record if found
        if not matching_records.empty:
            return matching_records.iloc[0]
        else:
            return None
            
    except Exception as e:
        print(f"Error finding representative production data: {e}")
        return None

def main():
    print("Starting Google Sheets data processing...")
    
    # Authenticate and connect to Google Sheets
    gc = authenticate()
    
    # Open the source spreadsheet (ID AQL)
    source_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit')
    
    # Open the new sample ID sheet for VHM and % Hao hụt OPP data
    sample_id_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/10R0Os96Ckwfiagbe-SbEut6FOyoMZZNmBc-HjdGKqwU/edit')
    
    # Open the destination spreadsheet
    destination_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1sb7Wz26CVkyUfWUE7NQmWm7_Byhw9eAHPArIUnn3iDA/edit')

    try:
        # Get the ID AQL worksheet data using robust method
        id_aql_worksheet = source_sheet.worksheet('ID AQL')
        id_aql_data = get_sheet_data_robust(id_aql_worksheet, 'ID AQL')
        id_aql_df = pd.DataFrame(id_aql_data)
        
        # IMPROVED: Expand rows with comma-separated MĐG values
        print(f"Original rows before MĐG expansion: {len(id_aql_df)}")
        id_aql_df = expand_dataframe_for_multiple_mdg(id_aql_df)
        print(f"Rows after MĐG expansion: {len(id_aql_df)}")
        
        # Get the defect code mapping from AQL gói sheet using robust method
        aql_goi_worksheet = source_sheet.worksheet('AQL gói')
        aql_goi_data = get_sheet_data_robust(aql_goi_worksheet, 'AQL gói')
        aql_goi_df = pd.DataFrame(aql_goi_data)
        
        # Get the defect code mapping from AQL Tô ly sheet using robust method
        aql_to_ly_worksheet = source_sheet.worksheet('AQL Tô ly')
        aql_to_ly_data = get_sheet_data_robust(aql_to_ly_worksheet, 'AQL Tô ly')
        aql_to_ly_df = pd.DataFrame(aql_to_ly_data)
        
        # Get the sample ID data for VHM and % Hao hụt OPP mapping using robust method
        sample_id_worksheet = sample_id_sheet.get_worksheet(0)  # First worksheet
        sample_id_data = get_sheet_data_robust(sample_id_worksheet, 'Sample ID (First worksheet)')
        sample_id_df = pd.DataFrame(sample_id_data)
        
        print(f"Retrieved {len(id_aql_df)} ID AQL records, {len(aql_goi_df)} AQL gói records, {len(aql_to_ly_df)} AQL Tô ly records, and {len(sample_id_df)} Sample ID records")
    
    except Exception as e:
        print(f"Error retrieving worksheet data: {str(e)}")
        sys.exit(1)

    # Check if required columns exist in dataframes
    required_columns_check = {
        'ID AQL': ['Line', 'Defect code', 'Ngày SX', 'Giờ', 'MĐG'],
        'AQL gói': ['Defect code', 'Defect name'],
        'AQL Tô ly': ['Defect code', 'Defect name'],
        'Sample ID': ['Ngày SX', 'Ca', 'Line', 'MĐG', 'VHM', '% Hao hụt OPP']
    }
    
    dataframes = {
        'ID AQL': id_aql_df,
        'AQL gói': aql_goi_df,
        'AQL Tô ly': aql_to_ly_df,
        'Sample ID': sample_id_df
    }
    
    for sheet_name, required_cols in required_columns_check.items():
        df = dataframes[sheet_name]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing columns in {sheet_name}: {missing_cols}")
            print(f"Available columns in {sheet_name}: {df.columns.tolist()}")

    # Convert 'Line' to numeric if it's not already
    if 'Line' in id_aql_df.columns:
        id_aql_df['Line'] = pd.to_numeric(id_aql_df['Line'], errors='coerce')
    else:
        print("Warning: 'Line' column not found in ID AQL data")
        id_aql_df['Line'] = None

    # Standardize defect codes (clean up any leading/trailing spaces)
    for df_name, df in [('ID AQL', id_aql_df), ('AQL gói', aql_goi_df), ('AQL Tô ly', aql_to_ly_df)]:
        if 'Defect code' in df.columns:
            df['Defect code'] = df['Defect code'].astype(str).str.strip()
        else:
            print(f"Warning: 'Defect code' column not found in {df_name} data")

    # Standardize dates
    if 'Ngày SX' in id_aql_df.columns:
        id_aql_df['Ngày SX_std'] = id_aql_df['Ngày SX'].apply(standardize_date)
        
        # Extract date, week and month
        id_aql_df['Ngày'] = id_aql_df['Ngày SX_std'].apply(get_day_of_month)
        id_aql_df['Tuần'] = id_aql_df['Ngày SX_std'].apply(get_week_number)
        id_aql_df['Tháng'] = id_aql_df['Ngày SX_std'].apply(get_month_number)
    else:
        print("Warning: 'Ngày SX' column not found in ID AQL data")
        id_aql_df['Ngày'] = None
        id_aql_df['Tuần'] = None
        id_aql_df['Tháng'] = None
    
    # Extract hour and determine shift (Ca)
    if 'Giờ' in id_aql_df.columns:
        id_aql_df['hour'] = id_aql_df['Giờ'].apply(parse_hour)
        id_aql_df['Ca'] = id_aql_df['hour'].apply(determine_shift)
    else:
        print("Warning: 'Giờ' column not found in ID AQL data")
        id_aql_df['hour'] = None
        id_aql_df['Ca'] = None
    
    # Add Target TV column based on Line
    id_aql_df['Target TV'] = id_aql_df['Line'].apply(get_target_tv)
    
    # Create defect name mapping dictionaries
    goi_defect_map = {}
    to_ly_defect_map = {}
    
    if 'Defect code' in aql_goi_df.columns and 'Defect name' in aql_goi_df.columns:
        goi_defect_map = dict(zip(aql_goi_df['Defect code'], aql_goi_df['Defect name']))
    
    if 'Defect code' in aql_to_ly_df.columns and 'Defect name' in aql_to_ly_df.columns:
        to_ly_defect_map = dict(zip(aql_to_ly_df['Defect code'], aql_to_ly_df['Defect name']))
    
    # Function to map defect code to defect name based on the Line value
    def map_defect_name(row):
        if pd.isna(row.get('Line')) or pd.isna(row.get('Defect code')) or str(row.get('Defect code')) == 'nan':
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
    
    # Create mapping dictionary for VHM and % Hao hụt OPP from sample_id_df
    print("Creating VHM and % Hao hụt OPP mapping with MĐG grouping logic...")
    vhm_mapping = {}
    hao_hut_mapping = {}
    
    # Create mapping from sample_id_df with MĐG grouping
    for _, row in sample_id_df.iterrows():
        keys = create_simple_mapping_key(row)  # Returns a list of keys
        vhm_value = row.get('VHM', '')
        hao_hut_value = row.get('% Hao hụt OPP', '')
        
        # Only process if we got valid keys
        if keys:  # Check if keys list is not empty
            for key in keys:  # Iterate through each key in the list
                if isinstance(key, tuple) and len(key) == 4:  # Validate key format
                    vhm_mapping[key] = vhm_value
                    hao_hut_mapping[key] = hao_hut_value
                    
            # Debug output for MĐG grouping
            if len(keys) > 1:
                try:
                    mdg_values = parse_mdg_values(row.get('MĐG', ''))
                    date_str = standardize_date(row['Ngày SX']).strftime('%d/%m/%Y') if standardize_date(row['Ngày SX']) else 'Unknown'
                    line = int(float(row['Line'])) if pd.notna(row['Line']) else 'Unknown'
                    ca = int(float(row['Ca'])) if pd.notna(row['Ca']) else 'Unknown'
                    
                    if 1 in mdg_values:
                        print(f"Mapped MĐG 1 to cover MĐG 1,2 for {date_str}, Ca {ca}, Line {line}, VHM: {vhm_value}")
                    elif 3 in mdg_values:
                        print(f"Mapped MĐG 3 to cover MĐG 3,4 for {date_str}, Ca {ca}, Line {line}, VHM: {vhm_value}")
                except Exception as e:
                    print(f"Debug output error: {e}")
    
    print(f"Created {len(vhm_mapping)} mapping entries for VHM and % Hao hụt OPP")
    
    # Function to get VHM based on hour logic
    def get_vhm(row):
        key = create_mapping_key_with_hour_logic(row, sample_id_df)
        result = vhm_mapping.get(key, '') if key else ''
        
        # Debug output for specific cases
        if result and str(row.get('Giờ', '')).strip() in ['12h', '12:00', '12']:
            hour = parse_hour(row.get('Giờ', ''))
            print(f"Debug: Hour {hour} -> Key: {key} -> VHM: {result}")
            
        return result
    
    # Function to get % Hao hụt OPP based on hour logic
    def get_hao_hut_opp(row):
        key = create_mapping_key_with_hour_logic(row, sample_id_df)
        return hao_hut_mapping.get(key, '') if key else ''
    
    # Apply VHM and % Hao hụt OPP mapping to existing AQL data
    print("Applying VHM mapping to existing AQL data...")
    id_aql_df['VHM'] = id_aql_df.apply(get_vhm, axis=1)
    id_aql_df['% Hao hụt OPP'] = id_aql_df.apply(get_hao_hut_opp, axis=1)
    
    # Debug: Show mapping statistics for AQL data
    vhm_mapped_count = (id_aql_df['VHM'] != '').sum()
    hao_hut_mapped_count = (id_aql_df['% Hao hụt OPP'] != '').sum()
    print(f"Successfully mapped VHM for {vhm_mapped_count} out of {len(id_aql_df)} AQL records")
    print(f"Successfully mapped % Hao hụt OPP for {hao_hut_mapped_count} out of {len(id_aql_df)} AQL records")
    
    # Create the new dataframe with required columns for existing AQL data
    required_output_columns = [
        'Ngày SX', 'Ngày', 'Tuần', 'Tháng', 'Sản phẩm', 'Item', 'Giờ', 'Ca', 'Line', 'MĐG', 
        'SL gói lỗi sau xử lý', 'Defect code', 'Defect name', 'Số lượng hold ( gói/thùng)',
        'Target TV', 'VHM', '% Hao hụt OPP', 'QA', 'Tên Trưởng ca'
    ]
    
    # Add MĐG_Original column if it exists
    if 'MĐG_Original' in id_aql_df.columns:
        required_output_columns.append('MĐG_Original')
    
    # Ensure all required columns exist
    for col in required_output_columns:
        if col not in id_aql_df.columns:
            if col in ['VHM', '% Hao hụt OPP', 'Target TV', 'Ca', 'Ngày', 'Tuần', 'Tháng', 'Defect name']:
                id_aql_df[col] = ''
            else:
                print(f"Warning: Column '{col}' not found and will be skipped.")
    
    # Filter to available columns
    available_columns = [col for col in required_output_columns if col in id_aql_df.columns]
    existing_aql_df = id_aql_df[available_columns].copy()
    
    # REVISED COMPREHENSIVE VHM INCLUSION LOGIC
    print("Creating comprehensive dataset with ALL existing defect records plus zero-defect VHM records...")
    
    # Start with ALL existing AQL records (both with and without VHM)
    comprehensive_rows = []
    
    # Step 1: Add ALL existing AQL records to the comprehensive dataset
    print(f"Step 1: Adding all {len(existing_aql_df)} existing AQL records...")
    for _, aql_row in existing_aql_df.iterrows():
        comprehensive_rows.append(aql_row)
    
    print(f"Added {len(existing_aql_df)} existing AQL records (both with and without VHM)")
    
    # Step 2: Identify VHMs that don't have any defect records and create zero-defect records for them
    print("Step 2: Identifying VHMs without defect records...")
    
    # Get all unique VHMs from sample_id_df
    all_vhms = set(sample_id_df['VHM'].dropna().unique())
    
    # Get all VHMs that already have defect records (non-zero quantities)
    existing_vhms_with_defects = set(existing_aql_df[
        (existing_aql_df['VHM'] != '') & 
        (existing_aql_df['VHM'].notna()) &
        (existing_aql_df['Số lượng hold ( gói/thùng)'].notna()) & 
        (existing_aql_df['Số lượng hold ( gói/thùng)'] != '') &
        (pd.to_numeric(existing_aql_df['Số lượng hold ( gói/thùng)'], errors='coerce') > 0)
    ]['VHM'].unique())
    
    # VHMs that need zero-defect records
    vhms_needing_zero_defect_records = all_vhms - existing_vhms_with_defects
    
    print(f"Total VHMs in sample data: {len(all_vhms)}")
    print(f"VHMs already with defect records: {len(existing_vhms_with_defects)}")
    print(f"VHMs needing zero-defect records: {len(vhms_needing_zero_defect_records)}")
    
    # Step 3: Create zero-defect records for VHMs that don't have any defect records
    if vhms_needing_zero_defect_records:
        print("Step 3: Creating zero-defect records for VHMs without defects...")
        
        for vhm_name in vhms_needing_zero_defect_records:
            try:
                # Find the sample record for this VHM
                vhm_sample_records = sample_id_df[sample_id_df['VHM'] == vhm_name]
                
                if vhm_sample_records.empty:
                    continue
                
                # Use the first sample record for this VHM
                sample_row = vhm_sample_records.iloc[0]
                
                # Get basic sample information
                sample_date = standardize_date(sample_row.get('Ngày SX', ''))
                if sample_date is None:
                    continue
                    
                sample_ca = sample_row.get('Ca', '')
                sample_line = sample_row.get('Line', '')
                sample_mdg = sample_row.get('MĐG', '')
                sample_vhm_value = sample_row.get('VHM', '')
                sample_hao_hut = sample_row.get('% Hao hụt OPP', '')
                
                print(f"Creating zero-defect record for VHM: {vhm_name}")
                
                # Find representative production data for this VHM's shift/line/MĐG
                representative_data = find_representative_production_data(
                    sample_date, sample_ca, sample_line, sample_mdg, existing_aql_df
                )
                
                # Create representative record with complete production information
                representative_record = {}
                
                # Set basic production information from sample data
                representative_record['Ngày SX'] = sample_row.get('Ngày SX', '')
                representative_record['Ngày'] = sample_date.day if sample_date else ''
                representative_record['Tuần'] = sample_date.isocalendar()[1] if sample_date else ''
                representative_record['Tháng'] = sample_date.month if sample_date else ''
                representative_record['Ca'] = sample_ca
                representative_record['Line'] = sample_line
                representative_record['MĐG'] = sample_mdg
                representative_record['VHM'] = sample_vhm_value
                representative_record['% Hao hụt OPP'] = sample_hao_hut
                
                # Use representative production data if found
                if representative_data is not None:
                    representative_record['Sản phẩm'] = representative_data.get('Sản phẩm', '')
                    representative_record['Item'] = representative_data.get('Item', '')
                    representative_record['Giờ'] = representative_data.get('Giờ', '')
                    representative_record['SL gói lỗi sau xử lý'] = representative_data.get('SL gói lỗi sau xử lý', '')
                    representative_record['Defect code'] = representative_data.get('Defect code', '')
                    representative_record['Defect name'] = representative_data.get('Defect name', '')
                    representative_record['QA'] = representative_data.get('QA', '')
                    representative_record['Tên Trưởng ca'] = representative_data.get('Tên Trưởng ca', '')
                    print(f"  Found representative data: {representative_data.get('Sản phẩm', 'N/A')} - {representative_data.get('Item', 'N/A')}")
                else:
                    # Fallback values if no representative data found
                    representative_record['Sản phẩm'] = ''
                    representative_record['Item'] = ''
                    representative_record['Giờ'] = ''
                    representative_record['SL gói lỗi sau xử lý'] = ''
                    representative_record['Defect code'] = ''
                    representative_record['Defect name'] = ''
                    representative_record['QA'] = ''
                    representative_record['Tên Trưởng ca'] = ''
                    print(f"  No representative data found, using empty values")
                
                # Set zero defect information (this is the key - quantity = 0)
                representative_record['Số lượng hold ( gói/thùng)'] = 0
                
                # Set target TV based on line
                try:
                    line_num = float(sample_line) if sample_line else None
                    if line_num and 1 <= line_num <= 6:
                        representative_record['Target TV'] = 0.29
                    elif line_num and 7 <= line_num <= 8:
                        representative_record['Target TV'] = 2.19
                    else:
                        representative_record['Target TV'] = ''
                except:
                    representative_record['Target TV'] = ''
                
                # Fill any remaining columns with empty values
                for col in available_columns:
                    if col not in representative_record:
                        representative_record[col] = ''
                
                comprehensive_rows.append(pd.Series(representative_record))
                print(f"  Successfully created zero-defect record for VHM: {vhm_name}")
                
            except Exception as e:
                print(f"Error creating zero-defect record for VHM {vhm_name}: {e}")
                continue
    
    # Create comprehensive dataframe
    if comprehensive_rows:
        # Convert all rows to DataFrame
        comprehensive_df = pd.DataFrame(comprehensive_rows)
        
        # Ensure column order matches available_columns
        comprehensive_df = comprehensive_df.reindex(columns=available_columns, fill_value='')
        
        print(f"Comprehensive dataset created with {len(comprehensive_df)} total records")
        
        # Show statistics
        total_records = len(comprehensive_df)
        records_with_vhm = len(comprehensive_df[
            (comprehensive_df['VHM'] != '') & 
            (comprehensive_df['VHM'].notna())
        ])
        records_without_vhm = total_records - records_with_vhm
        unique_vhms = len(comprehensive_df['VHM'].dropna().unique())
        
        print(f"Final dataset statistics:")
        print(f"  Total records: {total_records}")
        print(f"  Records with VHM: {records_with_vhm}")
        print(f"  Records without VHM: {records_without_vhm}")
        print(f"  Unique VHMs: {unique_vhms}")
        
        # Show VHM distribution
        vhm_counts = comprehensive_df[comprehensive_df['VHM'] != '']['VHM'].value_counts()
        print("VHM distribution in final dataset:")
        for vhm, count in vhm_counts.head(10).items():
            defect_count = len(comprehensive_df[
                (comprehensive_df['VHM'] == vhm) & 
                (pd.to_numeric(comprehensive_df['Số lượng hold ( gói/thùng)'], errors='coerce') > 0)
            ])
            zero_defect_count = count - defect_count
            print(f"  {vhm}: {count} records ({defect_count} with defects, {zero_defect_count} zero-defect)")
        
        if len(vhm_counts) > 10:
            print(f"  ... and {len(vhm_counts) - 10} more VHMs")
        
        new_df = comprehensive_df
    else:
        print("Warning: No comprehensive records created, falling back to existing AQL data")
        new_df = existing_aql_df
    
    # Sort by date (newest first)
    if 'Ngày SX' in new_df.columns:
        new_df['Ngày SX_for_sort'] = new_df['Ngày SX'].apply(standardize_date)
        new_df = new_df.sort_values(by='Ngày SX_for_sort', ascending=False, na_position='last')
        new_df = new_df.drop(columns=['Ngày SX_for_sort'])
        print(f"Data sorted by Ngày SX (newest to oldest)")
    else:
        print("Warning: Cannot sort by Ngày SX as column is missing")
    
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
        new_df_cleaned = new_df.fillna('').infer_objects(copy=False)
        data_to_write = [new_df_cleaned.columns.tolist()] + new_df_cleaned.values.tolist()

        # Update the worksheet - use new parameter order
        processed_worksheet.update(values=data_to_write, range_name='A1')
        print(f"Successfully wrote {len(data_to_write)-1} rows to the destination sheet, sorted by Ngày SX (newest first)")
        print(f"Added VHM and % Hao hụt OPP columns based on Ngày SX, Ca, Line, MĐG mapping")
        print(f"Improved handling of comma-separated MĐG values (e.g., '1,2' or '3,4')")
        print(f"Comprehensive dataset includes ALL defect records (with and without VHM) plus zero-defect VHM records")
        
        # Format the worksheet as a table
        format_as_table(processed_worksheet)

    except Exception as e:
        print(f"Error writing to destination sheet: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
