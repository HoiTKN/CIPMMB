import pandas as pd
import json
import os
from datetime import datetime
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

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
            return None
        
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
        return None

def create_knowledge_base():
    """Create knowledge base from integrated data"""
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            print("Failed to authenticate with Google Sheets")
            return False
        
        # Open the Google Sheet with integrated data
        sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet and get the worksheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            
            # Try to get the "Integrated_Data" worksheet
            try:
                worksheet = spreadsheet.worksheet('Integrated_Data')
                print(f"Connected to: {spreadsheet.title} - Integrated_Data")
            except gspread.exceptions.WorksheetNotFound:
                # Fall back to first worksheet if Integrated_Data doesn't exist
                worksheet = spreadsheet.get_worksheet(0)
                print(f"'Integrated_Data' worksheet not found. Using '{worksheet.title}' instead.")
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            if df.empty:
                print("No data found in worksheet")
                return False
            
            # Basic data cleaning
            # Convert date columns to datetime if needed
            if "Ngày SX" in df.columns:
                try:
                    df["Ngày SX"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
                except Exception as e:
                    print(f"Could not process date column: {e}")
            
            # Make sure numeric columns are properly typed
            if "SL pack/ cây lỗi" in df.columns:
                df["SL pack/ cây lỗi"] = pd.to_numeric(df["SL pack/ cây lỗi"], errors='coerce').fillna(0)
            
            # Ensure Line column is converted to string for consistent filtering
            if "Line" in df.columns:
                df["Line"] = df["Line"].astype(str)
            
            # Ensure Máy column is converted to string
            if "Máy" in df.columns:
                df["Máy"] = df["Máy"].astype(str)
            
            # Create structured knowledge base
            complaints_data = df.to_dict('records')
            
            knowledge_base = {
                "complaints": complaints_data,
                "metadata": {
                    "last_updated": datetime.now().isoformat(),
                    "total_complaints": len(complaints_data),
                    "date_range": {
                        "start": df['Ngày SX'].min().strftime('%Y-%m-%d') if "Ngày SX" in df.columns and not df["Ngày SX"].isna().all() else None,
                        "end": df['Ngày SX'].max().strftime('%Y-%m-%d') if "Ngày SX" in df.columns and not df["Ngày SX"].isna().all() else None
                    },
                    "products": df['Tên sản phẩm'].unique().tolist() if "Tên sản phẩm" in df.columns else [],
                    "defect_types": df['Tên lỗi'].unique().tolist() if "Tên lỗi" in df.columns else [],
                    "lines": df['Line'].unique().tolist() if "Line" in df.columns else []
                }
            }
            
            # Save knowledge base to file
            with open('complaint_knowledge_base.json', 'w') as f:
                json.dump(knowledge_base, f, ensure_ascii=False, indent=2)
                
            print(f"Knowledge base created with {len(df)} complaints")
            return True
                
        except Exception as e:
            print(f"Error accessing spreadsheet: {str(e)}")
            return False
        
    except Exception as e:
        print(f"Error creating knowledge base: {str(e)}")
        return False

if __name__ == "__main__":
    create_knowledge_base()
