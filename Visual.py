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
        mdg = int(float(row['MĐG'])) if pd.notna(row['MĐG']) else None
        
        if line is None or mdg is None:
            return None
        
        # Determine which shift codes to look for based on the hour
        possible_shift_codes = []
        
        if 6 <= hour < 18:  # 6am to 6pm - matches Ca 14
            possible_shift_codes = [14, 1, 2]  # Check Ca 14 first, then normal shifts
        elif 18 <= hour <= 23 or 0 <= hour < 6:  # 6pm to 6am - matches Ca 34
            possible_shift_codes = [34, 2, 3]  # Check Ca 34 first, then normal shifts
        else:
            # Fallback to normal shift determination
            ca = determine_shift(hour)
            if ca:
                possible_shift_codes = [ca]
        
        # Handle MĐG grouping - determine which MĐG values to look for in sample sheet
        mdg_lookup_values = []
        if mdg == 2:
            # MĐG 2 should look for MĐG 1 in sample sheet (since MĐG 1 covers 1,2)
            mdg_lookup_values = [1, 2]  # Try 1 first, then 2 as fallback
        elif mdg == 4:
            # MĐG 4 should look for MĐG 3 in sample sheet (since MĐG 3 covers 3,4)
            mdg_lookup_values = [3, 4]  # Try 3 first, then 4 as fallback
        else:
            mdg_lookup_values = [mdg]
        
        # Try to find a match in sample_id_df for any combination of shift codes and MĐG values
        for shift_code in possible_shift_codes:
            for lookup_mdg in mdg_lookup_values:
                # Check if there's a matching record in sample_id_df
                try:
                    matching_records = sample_id_df[
                        (sample_id_df['Ngày SX'].apply(lambda x: standardize_date(x).strftime('%d/%m/%Y') if standardize_date(x) else None) == date_key) &
                        (sample_id_df['Ca'].astype(str).str.strip() == str(shift_code)) &
                        (sample_id_df['Line'].astype(str).str.strip() == str(line)) &
                        (sample_id_df['MĐG'].astype(str).str.strip() == str(lookup_mdg))
                    ]
                    
                    if not matching_records.empty:
                        # Return the key that will be used for lookup (using original mdg from AQL data)
                        return (date_key, shift_code, line, mdg)
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
        mdg = int(float(row['MĐG'])) if pd.notna(row['MĐG']) else None
        
        if ca is None or line is None or mdg is None:
            return []
        
        # Handle MĐG grouping logic
        keys = []
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
        
        # Get the sample ID data for VHM and % Hao hụt OPP mapping
        # Assuming the data is in the first worksheet (Table1)
        sample_id_worksheet = sample_id_sheet.get_worksheet(0)  # First worksheet
        sample_id_data = sample_id_worksheet.get_all_records()
        sample_id_df = pd.DataFrame(sample_id_data)
        
        print(f"Retrieved {len(id_aql_df)} ID AQL records, {len(aql_goi_df)} AQL gói records, {len(aql_to_ly_df)} AQL Tô ly records, and {len(sample_id_df)} Sample ID records")
    
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
    
    # Extract date, week and month
    id_aql_df['Ngày'] = id_aql_df['Ngày SX_std'].apply(get_day_of_month)
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
                    mdg_original = int(float(row['MĐG'])) if pd.notna(row['MĐG']) else None
                    date_str = standardize_date(row['Ngày SX']).strftime('%d/%m/%Y') if standardize_date(row['Ngày SX']) else 'Unknown'
                    line = int(float(row['Line'])) if pd.notna(row['Line']) else 'Unknown'
                    ca = int(float(row['Ca'])) if pd.notna(row['Ca']) else 'Unknown'
                    
                    if mdg_original == 1:
                        print(f"Mapped MĐG 1 to cover MĐG 1,2 for {date_str}, Ca {ca}, Line {line}, VHM: {vhm_value}")
                    elif mdg_original == 3:
                        print(f"Mapped MĐG 3 to cover MĐG 3,4 for {date_str}, Ca {ca}, Line {line}, VHM: {vhm_value}")
                except Exception as e:
                    print(f"Debug output error: {e}")
    
    print(f"Created {len(vhm_mapping)} mapping entries for VHM and % Hao hụt OPP")
    
    # Function to get VHM based on hour logic
    def get_vhm(row):
        key = create_mapping_key_with_hour_logic(row, sample_id_df)
        return vhm_mapping.get(key, '') if key else ''
    
    # Function to get % Hao hụt OPP based on hour logic
    def get_hao_hut_opp(row):
        key = create_mapping_key_with_hour_logic(row, sample_id_df)
        return hao_hut_mapping.get(key, '') if key else ''
    
    # Add VHM and % Hao hụt OPP columns to the main dataframe
    print("Applying VHM and % Hao hụt OPP mapping to main data with MĐG grouping...")
    id_aql_df['VHM'] = id_aql_df.apply(get_vhm, axis=1)
    id_aql_df['% Hao hụt OPP'] = id_aql_df.apply(get_hao_hut_opp, axis=1)
    
    # Debug: Show mapping statistics
    vhm_mapped_count = (id_aql_df['VHM'] != '').sum()
    hao_hut_mapped_count = (id_aql_df['% Hao hụt OPP'] != '').sum()
    print(f"Successfully mapped VHM for {vhm_mapped_count} out of {len(id_aql_df)} records")
    print(f"Successfully mapped % Hao hụt OPP for {hao_hut_mapped_count} out of {len(id_aql_df)} records")
    
    # Debug: Show MĐG grouping mapping examples
    mdg_2_mapped = ((id_aql_df['MĐG'] == 2) & (id_aql_df['VHM'] != '')).sum()
    mdg_4_mapped = ((id_aql_df['MĐG'] == 4) & (id_aql_df['VHM'] != '')).sum()
    total_mdg_2 = (id_aql_df['MĐG'] == 2).sum()
    total_mdg_4 = (id_aql_df['MĐG'] == 4).sum()
    
    if total_mdg_2 > 0:
        print(f"MĐG 2 mapping: {mdg_2_mapped}/{total_mdg_2} records mapped (looking for MĐG 1 in sample sheet)")
    if total_mdg_4 > 0:
        print(f"MĐG 4 mapping: {mdg_4_mapped}/{total_mdg_4} records mapped (looking for MĐG 3 in sample sheet)")
    
    # Create the new dataframe with required columns (including new VHM and % Hao hụt OPP columns)
    try:
        new_df = id_aql_df[[
            'Ngày SX', 'Ngày', 'Tuần', 'Tháng', 'Sản phẩm', 'Item', 'Giờ', 'Ca', 'Line', 'MĐG', 
            'SL gói lỗi sau xử lý', 'Defect code', 'Defect name', 'Số lượng hold ( gói/thùng)',
            'Target TV', 'VHM', '% Hao hụt OPP', 'QA', 'Tên Trưởng ca'
        ]].copy()
    except KeyError as e:
        print(f"Error: Missing column in source data: {e}")
        print(f"Available columns: {id_aql_df.columns.tolist()}")
        sys.exit(1)
    
    # Filter to only include rows where 'Số lượng hold ( gói/thùng)' is not empty
    print(f"Total rows before filtering: {len(new_df)}")
    new_df = new_df[new_df['Số lượng hold ( gói/thùng)'].notna() & (new_df['Số lượng hold ( gói/thùng)'] != '')]
    print(f"Rows after filtering for non-empty 'Số lượng hold ( gói/thùng)': {len(new_df)}")
    
    # Sort by Ngày SX (newest first) - convert to datetime for proper sorting
    new_df['Ngày SX_for_sort'] = new_df['Ngày SX'].apply(standardize_date)
    new_df = new_df.sort_values(by='Ngày SX_for_sort', ascending=False, na_position='last')
    new_df = new_df.drop(columns=['Ngày SX_for_sort'])  # Remove the temporary sorting column
    
    print(f"Data sorted by Ngày SX (newest to oldest)")
    
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
        print(f"Added VHM and % Hao hụt OPP columns based on Ngày SX, Ca, Line, MĐG mapping")
        
        # Format the worksheet as a table
        format_as_table(processed_worksheet)

    except Exception as e:
        print(f"Error writing to destination sheet: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
