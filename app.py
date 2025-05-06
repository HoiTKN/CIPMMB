import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import gspread
import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from plotly.subplots import make_subplots

# Set page configuration with improved styling
st.set_page_config(
    page_title="FMCG Quality Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to improve the look and feel
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #1E3A8A;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #64748b;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
    }
    .stDataFrame {
        border-radius: 10px !important;
        overflow: hidden;
    }
    .stDataFrame table {
        border-collapse: collapse;
        width: 100%;
    }
    .stDataFrame th {
        background-color: #1E3A8A !important;
        color: white !important;
        font-weight: 600;
        padding: 12px !important;
    }
    .stDataFrame td {
        padding: 10px !important;
    }
    .stDataFrame tr:nth-child(even) {
        background-color: #f8f9fa;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    .insight-card {
        background-color: #f0f7ff;
        border-left: 5px solid #3b82f6;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
    }
    .insight-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1e40af;
        margin-bottom: 8px;
    }
    .insight-content {
        color: #334155;
        font-size: 0.95rem;
    }
    .warning-card {
        background-color: #fff1f2;
        border-left: 5px solid #e11d48;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
    }
    .warning-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #be123c;
        margin-bottom: 8px;
    }
    .tab-container {
        margin-top: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: white;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1E3A8A;
        color: white;
    }
    /* Hide authentication status expander */
    div[data-testid="stExpander"] > div[role="button"]:has(div:contains("Authentication Status")) {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Define the scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authentication function - Modified to hide the debug expander
def authenticate():
    """Authentication using OAuth token"""
    try:
        # Authentication expander is hidden via CSS
        debug_expander = st.expander("Authentication Status", expanded=False)
        
        with debug_expander:
            creds = None
            
            # Check if token.json exists first
            if os.path.exists('token.json'):
                st.success("✅ Found token.json file")
                try:
                    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
                except Exception as e:
                    st.error(f"Error loading token.json: {e}")
            # Otherwise create it from the environment variable or Streamlit secrets
            elif 'GOOGLE_TOKEN_JSON' in os.environ:
                st.success("✅ Found GOOGLE_TOKEN_JSON in environment variables")
                try:
                    token_info = os.environ.get('GOOGLE_TOKEN_JSON')
                    with open('token.json', 'w') as f:
                        f.write(token_info)
                    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
                except Exception as e:
                    st.error(f"Error loading from environment variable: {e}")
            elif 'GOOGLE_TOKEN_JSON' in st.secrets:
                st.success("✅ Found GOOGLE_TOKEN_JSON in Streamlit secrets")
                try:
                    token_info = st.secrets['GOOGLE_TOKEN_JSON']
                    with open('token.json', 'w') as f:
                        f.write(token_info)
                    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
                except Exception as e:
                    st.error(f"Error loading from Streamlit secrets: {e}")
            else:
                st.error("❌ No token.json file or GOOGLE_TOKEN_JSON found")
                return None
            
            # Refresh token if expired
            if creds and creds.expired and creds.refresh_token:
                st.info("🔄 Token expired, refreshing...")
                try:
                    creds.refresh(Request())
                    with open('token.json', 'w') as token:
                        token.write(creds.to_json())
                        st.success("✅ Token refreshed and saved")
                except Exception as e:
                    st.error(f"Error refreshing token: {e}")
                    
            # Return authorized client
            if creds:
                return gspread.authorize(creds)
            else:
                return None
    
    except Exception as e:
        st.error(f"❌ Authentication error: {str(e)}")
        return None

# Function to load complaint data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_complaint_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Failed to authenticate with Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (complaint data)
        sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet and get the worksheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            connection_status.success(f"✅ Successfully opened complaints spreadsheet: {spreadsheet.title}")
            
            # Try to get the "Integrated_Data" worksheet
            try:
                worksheet = spreadsheet.worksheet('Integrated_Data')
                connection_status.success(f"✅ Connected to: {spreadsheet.title} - Integrated_Data")
            except gspread.exceptions.WorksheetNotFound:
                # Fall back to first worksheet if Integrated_Data doesn't exist
                worksheet = spreadsheet.get_worksheet(0)
                connection_status.warning(f"⚠️ 'Integrated_Data' worksheet not found. Using '{worksheet.title}' instead.")
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Basic data cleaning
            # Convert date columns to datetime if needed
            if "Ngày SX" in df.columns:
                try:
                    df["Ngày SX"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
                    df["Production_Month"] = df["Ngày SX"].dt.strftime("%m/%Y")
                    df["Production_Date"] = df["Ngày SX"]
                except Exception as e:
                    connection_status.warning(f"⚠️ Could not process date column: {e}")
            
            # Make sure numeric columns are properly typed
            if "SL pack/ cây lỗi" in df.columns:
                df["SL pack/ cây lỗi"] = pd.to_numeric(df["SL pack/ cây lỗi"], errors='coerce').fillna(0)
            
            # Ensure Line column is converted to string for consistent filtering
            if "Line" in df.columns:
                df["Line"] = df["Line"].astype(str)
            
            # Ensure Máy column is converted to string
            if "Máy" in df.columns:
                df["Máy"] = df["Máy"].astype(str)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error accessing complaint spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading complaint data: {str(e)}")
        return pd.DataFrame()

# Function to load AQL data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aql_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Failed to authenticate with Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (AQL data)
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            connection_status.success(f"✅ Successfully opened AQL spreadsheet: {spreadsheet.title}")
            
            # Get the ID AQL worksheet
            try:
                worksheet = spreadsheet.worksheet('ID AQL')
                connection_status.success(f"✅ Connected to: {spreadsheet.title} - ID AQL")
            except gspread.exceptions.WorksheetNotFound:
                connection_status.error(f"❌ 'ID AQL' worksheet not found")
                return pd.DataFrame()
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Basic data cleaning
            # Convert date columns to datetime if needed
            if "Ngày SX" in df.columns:
                try:
                    df["Ngày SX"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
                    df["Production_Month"] = df["Ngày SX"].dt.strftime("%m/%Y")
                    df["Production_Date"] = df["Ngày SX"]
                except Exception as e:
                    connection_status.warning(f"⚠️ Could not process date column: {e}")
            
            # Make sure numeric columns are properly typed
            if "Số lượng hold ( gói/thùng)" in df.columns:
                df["Số lượng hold ( gói/thùng)"] = pd.to_numeric(df["Số lượng hold ( gói/thùng)"], errors='coerce').fillna(0)
            
            # Ensure Line column is converted to string for consistent filtering
            if "Line" in df.columns:
                df["Line"] = df["Line"].astype(str)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error accessing AQL spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading AQL data: {str(e)}")
        return pd.DataFrame()

# Function to load AQL gói data - NEW
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aql_goi_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Failed to authenticate with Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (AQL data)
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            
            # Get the AQL gói worksheet
            try:
                worksheet = spreadsheet.worksheet('AQL gói')
                connection_status.success(f"✅ Connected to: {spreadsheet.title} - AQL gói")
            except gspread.exceptions.WorksheetNotFound:
                connection_status.error(f"❌ 'AQL gói' worksheet not found")
                return pd.DataFrame()
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error accessing AQL gói spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading AQL gói data: {str(e)}")
        return pd.DataFrame()

# Function to load AQL Tô ly data - NEW
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aql_to_ly_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Failed to authenticate with Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (AQL data)
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            
            # Get the AQL Tô ly worksheet
            try:
                worksheet = spreadsheet.worksheet('AQL Tô ly')
                connection_status.success(f"✅ Connected to: {spreadsheet.title} - AQL Tô ly")
            except gspread.exceptions.WorksheetNotFound:
                connection_status.error(f"❌ 'AQL Tô ly' worksheet not found")
                return pd.DataFrame()
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error accessing AQL Tô ly spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading AQL Tô ly data: {str(e)}")
        return pd.DataFrame()

# Function to load production data (Sản lượng)
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_production_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Failed to authenticate with Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (AQL data - same spreadsheet, different worksheet)
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            connection_status.success(f"✅ Successfully opened production spreadsheet: {spreadsheet.title}")
            
            # Get the Sản lượng worksheet
            try:
                worksheet = spreadsheet.worksheet('Sản lượng')
                connection_status.success(f"✅ Connected to: {spreadsheet.title} - Sản lượng")
            except gspread.exceptions.WorksheetNotFound:
                connection_status.error(f"❌ 'Sản lượng' worksheet not found")
                return pd.DataFrame()
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Basic data cleaning
            # Convert date columns to datetime if needed
            if "Ngày" in df.columns:
                try:
                    df["Ngày"] = pd.to_datetime(df["Ngày"], format="%d/%m/%Y", errors='coerce')
                    df["Production_Month"] = df["Ngày"].dt.strftime("%m/%Y")
                    df["Production_Date"] = df["Ngày"]
                except Exception as e:
                    connection_status.warning(f"⚠️ Could not process date column: {e}")
            
            # Make sure numeric columns are properly typed
            if "Sản lượng" in df.columns:
                df["Sản lượng"] = pd.to_numeric(df["Sản lượng"], errors='coerce').fillna(0)
            
            # Ensure Line column is converted to string for consistent filtering
            if "Line" in df.columns:
                df["Line"] = df["Line"].astype(str)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error accessing production spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading production data: {str(e)}")
        return pd.DataFrame()

# Function to calculate TEM VÀNG - UPDATED with new matching logic
def calculate_tem_vang(aql_df, production_df):
    """Calculate TEM VÀNG by matching production data with AQL data"""
    try:
        # Check if dataframes are empty
        if aql_df.empty or production_df.empty:
            st.error("❌ Cannot calculate TEM VÀNG - missing data")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Define function to map time to shift
        def map_time_to_shift(time_str):
            try:
                # Extract hour from time string (format expected: "HH:MM")
                if pd.isna(time_str):
                    return None
                
                hour = int(time_str.split(':')[0])
                
                # Map hour to shift
                if 6 <= hour < 14:
                    return "1"
                elif 14 <= hour < 22:
                    return "2"
                else:  # 22-6
                    return "3"
            except:
                return None
        
        # Add shift column to AQL data based on Giờ
        if "Giờ" in aql_copy.columns:
            aql_copy["Shift"] = aql_copy["Giờ"].apply(map_time_to_shift)
        else:
            st.warning("⚠️ Missing 'Giờ' column in AQL data for shift mapping")
            return pd.DataFrame()
        
        # Perform the matching and aggregation
        tem_vang_data = []
        
        # Group production data by Date, Line, Shift, Leader
        if all(col in prod_copy.columns for col in ["Ngày", "Line", "Ca", "Người phụ trách"]):
            prod_groups = prod_copy.groupby(["Ngày", "Line", "Ca", "Người phụ trách"])
            
            for (prod_date, prod_line, prod_shift, prod_leader), prod_group in prod_groups:
                # Find matching AQL records
                matching_aql = aql_copy[
                    (aql_copy["Ngày SX"] == prod_date) &
                    (aql_copy["Line"] == prod_line) &
                    (aql_copy["Shift"] == prod_shift) &
                    (aql_copy["Tên Trưởng ca"] == prod_leader)
                ]
                
                # If matches found, calculate TEM VÀNG
                total_production = prod_group["Sản lượng"].sum()
                total_hold = matching_aql["Số lượng hold ( gói/thùng)"].sum() if not matching_aql.empty else 0
                
                if total_production > 0:
                    tem_vang_percent = (total_hold / total_production) * 100
                    
                    tem_vang_data.append({
                        "Date": prod_date,
                        "Line": prod_line,
                        "Shift": prod_shift,
                        "Leader": prod_leader,
                        "Production_Volume": total_production,
                        "Hold_Quantity": total_hold,
                        "TEM_VANG": tem_vang_percent,
                        "Production_Month": prod_date.strftime("%m/%Y") if isinstance(prod_date, datetime) else pd.to_datetime(prod_date).strftime("%m/%Y")
                    })
        else:
            st.warning("⚠️ Missing required columns in production data for TEM VÀNG calculation")
            return pd.DataFrame()
        
        # Convert to DataFrame
        tem_vang_df = pd.DataFrame(tem_vang_data)
        
        # Ensure Production_Date is properly set for filtering
        tem_vang_df["Production_Date"] = tem_vang_df["Date"]
        
        return tem_vang_df
        
    except Exception as e:
        st.error(f"❌ Error calculating TEM VÀNG: {str(e)}")
        return pd.DataFrame()

# Function to calculate process capability
def calculate_process_capability(tem_vang_df, target=2.18):
    """Calculate process capability metrics for TEM VÀNG by line"""
    try:
        # Check if dataframe is empty
        if tem_vang_df.empty:
            return pd.DataFrame()
        
        # Group by line to calculate process metrics
        process_df = tem_vang_df.groupby("Line").agg({
            "TEM_VANG": ["mean", "std", "count"]
        }).reset_index()
        
        # Flatten multi-index columns
        process_df.columns = ["Line", "Mean_TEM_VANG", "Std_TEM_VANG", "Sample_Count"]
        
        # Calculate Cpk (process capability index)
        def calc_cpk(row):
            if row["Std_TEM_VANG"] > 0:
                # For TEM VÀNG, lower is better, so USL is the target
                # LSL is 0 (can't have negative TEM VÀNG)
                usl = target
                lsl = 0
                
                cpu = (usl - row["Mean_TEM_VANG"]) / (3 * row["Std_TEM_VANG"])
                cpl = (row["Mean_TEM_VANG"] - lsl) / (3 * row["Std_TEM_VANG"])
                
                return min(cpu, cpl)
            else:
                return None
        
        # Add Cpk to the dataframe
        process_df["Cpk"] = process_df.apply(calc_cpk, axis=1)
        
        return process_df
        
    except Exception as e:
        st.error(f"❌ Error calculating process capability: {str(e)}")
        return pd.DataFrame()

# Function to analyze defect patterns - UPDATED with defect name mapping
def analyze_defect_patterns(aql_df, aql_goi_df, aql_to_ly_df):
    """Analyze defect patterns in AQL data with proper defect name mapping"""
    try:
        # Check if dataframe is empty
        if aql_df.empty:
            return {}
        
        # Create copy to avoid modifying original
        df = aql_df.copy()
        
        # Create defect mapping dictionaries
        defect_goi_map = {}
        defect_to_ly_map = {}
        
        # Build mapping from AQL gói
        if not aql_goi_df.empty and "Defect code" in aql_goi_df.columns and "Defect Name" in aql_goi_df.columns:
            for _, row in aql_goi_df.iterrows():
                key = f"{row['Defect code']}-{row['Type']}" if "Type" in aql_goi_df.columns else row["Defect code"]
                defect_goi_map[key] = row["Defect Name"]
        
        # Build mapping from AQL Tô ly
        if not aql_to_ly_df.empty and "Defect code" in aql_to_ly_df.columns and "Defect Name" in aql_to_ly_df.columns:
            for _, row in aql_to_ly_df.iterrows():
                key = f"{row['Defect code']}-{row['Type']}" if "Type" in aql_to_ly_df.columns else row["Defect code"]
                defect_to_ly_map[key] = row["Defect Name"]
        
        # Add Defect Name column to df
        df["Defect_Name"] = None
        
        # Map defect names based on line
        for i, row in df.iterrows():
            line = row["Line"] if "Line" in df.columns else None
            defect_code = row["Defect code"] if "Defect code" in df.columns else None
            defect_type = row["Type"] if "Type" in df.columns else None
            
            if line is not None and defect_code is not None:
                key = f"{defect_code}-{defect_type}" if defect_type is not None else defect_code
                
                # Lines 1-6 use AQL gói
                if pd.notna(line) and int(line) <= 6:
                    df.at[i, "Defect_Name"] = defect_goi_map.get(key, defect_code)
                # Lines 7-8 use AQL Tô ly
                elif pd.notna(line) and int(line) >= 7:
                    df.at[i, "Defect_Name"] = defect_to_ly_map.get(key, defect_code)
        
        # Use Defect_Name for analysis if available, otherwise use code
        defect_col = "Defect_Name" if df["Defect_Name"].notna().any() else "Defect code"
        
        # Group by defect to get frequency
        defect_counts = df.groupby(defect_col).size().reset_index(name="Count")
        defect_counts = defect_counts.sort_values("Count", ascending=False)
        
        # Calculate percentages
        total_defects = defect_counts["Count"].sum()
        defect_counts["Percentage"] = (defect_counts["Count"] / total_defects * 100).round(1)
        defect_counts["Cumulative"] = defect_counts["Percentage"].cumsum()
        
        # Identify top defects (80% by Pareto principle)
        vital_few = defect_counts[defect_counts["Cumulative"] <= 80]
        
        # Group by Line and Defect for line-specific patterns
        line_defects = df.groupby(["Line", defect_col]).size().reset_index(name="Count")
        pivot_line_defects = line_defects.pivot(index="Line", columns=defect_col, values="Count").fillna(0)
        
        # Calculate defect rates by MDG (fixed to use Máy)
        if "Máy" in df.columns:
            mdg_defects = df.groupby(["Line", "Máy", defect_col]).size().reset_index(name="Count")
        else:
            mdg_defects = pd.DataFrame()
        
        # Return the analysis results
        return {
            "defect_counts": defect_counts,
            "vital_few": vital_few,
            "line_defects": line_defects,
            "pivot_line_defects": pivot_line_defects,
            "mdg_defects": mdg_defects,
            "defect_column": defect_col
        }
            
    except Exception as e:
        st.error(f"❌ Error analyzing defect patterns: {str(e)}")
        return {}

# Function to link internal defects with customer complaints
def link_defects_with_complaints(aql_df, complaint_df):
    """Link internal defects (AQL) with customer complaints"""
    try:
        # Check if dataframes are empty
        if aql_df.empty or complaint_df.empty:
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        complaint_copy = complaint_df.copy()
        
        # Standardize date columns for joining
        if "Production_Date" in aql_copy.columns and "Production_Date" in complaint_copy.columns:
            # Group AQL defects by date, line, and defect code
            if "Defect code" in aql_copy.columns and "Line" in aql_copy.columns:
                aql_grouped = aql_copy.groupby(["Production_Date", "Line", "Defect code"]).size().reset_index(name="Defect_Count")
            else:
                st.warning("⚠️ Missing required columns in AQL data for linking")
                return pd.DataFrame()
            
            # Group complaints by date, line, and defect type
            if "Tên lỗi" in complaint_copy.columns and "Line" in complaint_copy.columns:
                # Count unique ticket IDs for each group
                if "Mã ticket" in complaint_copy.columns:
                    complaint_grouped = complaint_copy.groupby(["Production_Date", "Line", "Tên lỗi"])["Mã ticket"].nunique().reset_index(name="Complaint_Count")
                else:
                    complaint_grouped = complaint_copy.groupby(["Production_Date", "Line", "Tên lỗi"]).size().reset_index(name="Complaint_Count")
            else:
                st.warning("⚠️ Missing required columns in complaint data for linking")
                return pd.DataFrame()
            
            # Create mapping between internal defect codes and customer complaint types
            # This mapping should be customized based on your specific defect codes and complaint types
            defect_map = {
                # Example mapping - update with your actual codes
                "NQ-133": "Hở nắp",
                "NQ-124": "Rách OPP",
                "HE-022": "Mất date",
                "HE-023": "Thiếu gia vị",
                "NE-023": "Hở nắp",
                "KK-032": "Dị vật"
            }
            
            # Add mapped complaint type to AQL data
            aql_grouped["Mapped_Complaint_Type"] = aql_grouped["Defect code"].map(defect_map)
            
            # Group AQL data by date, line, and mapped complaint type
            aql_grouped_mapped = aql_grouped.groupby(["Production_Date", "Line", "Mapped_Complaint_Type"])["Defect_Count"].sum().reset_index()
            
            # Rename complaint type column for joining
            complaint_grouped_renamed = complaint_grouped.rename(columns={"Tên lỗi": "Mapped_Complaint_Type"})
            
            # Join AQL and complaint data
            # Use a window of +/- 7 days to account for lag between production and complaint
            linked_defects = pd.DataFrame()
            
            for _, aql_row in aql_grouped_mapped.iterrows():
                prod_date = aql_row["Production_Date"]
                line = aql_row["Line"]
                complaint_type = aql_row["Mapped_Complaint_Type"]
                
                # Skip if complaint type mapping is null
                if pd.isna(complaint_type):
                    continue
                
                # Find complaints within the window
                date_min = prod_date - timedelta(days=1)  # 1 day before production
                date_max = prod_date + timedelta(days=14)  # Up to 14 days after production
                
                matching_complaints = complaint_grouped_renamed[
                    (complaint_grouped_renamed["Production_Date"] >= date_min) &
                    (complaint_grouped_renamed["Production_Date"] <= date_max) &
                    (complaint_grouped_renamed["Line"] == line) &
                    (complaint_grouped_renamed["Mapped_Complaint_Type"] == complaint_type)
                ]
                
                if not matching_complaints.empty:
                    total_complaints = matching_complaints["Complaint_Count"].sum()
                    
                    linked_row = pd.DataFrame({
                        "Production_Date": [prod_date],
                        "Line": [line],
                        "Defect_Type": [complaint_type],
                        "Internal_Defect_Count": [aql_row["Defect_Count"]],
                        "Customer_Complaint_Count": [total_complaints],
                        "Defect_to_Complaint_Ratio": [aql_row["Defect_Count"] / total_complaints if total_complaints > 0 else float('inf')]
                    })
                    
                    linked_defects = pd.concat([linked_defects, linked_row], ignore_index=True)
            
            return linked_defects
            
        else:
            st.warning("⚠️ Missing date columns for linking defects with complaints")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"❌ Error linking defects with complaints: {str(e)}")
        return pd.DataFrame()

# Load the data - UPDATED to include new data sources
@st.cache_data(ttl=600)  # Cache the combined data for 10 minutes
def load_all_data():
    """Load and prepare all required data"""
    
    # Load raw data
    complaint_df = load_complaint_data()
    aql_df = load_aql_data()
    production_df = load_production_data()
    
    # Load defect mapping data - NEW
    aql_goi_df = load_aql_goi_data()
    aql_to_ly_df = load_aql_to_ly_data()
    
    # Calculate TEM VÀNG
    tem_vang_df = calculate_tem_vang(aql_df, production_df)
    
    # Calculate process capability
    if not tem_vang_df.empty:
        process_capability_df = calculate_process_capability(tem_vang_df)
    else:
        process_capability_df = pd.DataFrame()
    
    # Analyze defect patterns - UPDATED to use defect mapping
    defect_patterns = analyze_defect_patterns(aql_df, aql_goi_df, aql_to_ly_df)
    
    # Link defects with complaints
    linked_defects_df = link_defects_with_complaints(aql_df, complaint_df)
    
    return {
        "complaint_data": complaint_df,
        "aql_data": aql_df,
        "production_data": production_df,
        "aql_goi_data": aql_goi_df,
        "aql_to_ly_data": aql_to_ly_df,
        "tem_vang_data": tem_vang_df,
        "process_capability": process_capability_df,
        "defect_patterns": defect_patterns,
        "linked_defects": linked_defects_df
    }

# Title and description
st.markdown('<div class="main-header">FMCG Quality Analytics Dashboard</div>', unsafe_allow_html=True)
st.markdown("Comprehensive quality analytics for production monitoring and customer satisfaction")

# Load all data
data = load_all_data()

# Check if key dataframes are empty
if data["aql_data"].empty or data["production_data"].empty:
    st.warning("⚠️ Missing essential data. Please check your Google Sheet connection.")
    # Still continue rendering with available data

# Create a sidebar for filters
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #1E3A8A;'>Filters</h2>", unsafe_allow_html=True)
    
    # Initialize filtered dataframes
    filtered_aql_df = data["aql_data"].copy()
    filtered_complaint_df = data["complaint_data"].copy()
    filtered_tem_vang_df = data["tem_vang_data"].copy()
    
    # NEW - Date range filter
    if not data["aql_data"].empty and "Production_Date" in data["aql_data"].columns:
        try:
            # Get min and max dates from data
            min_date = data["aql_data"]["Production_Date"].min().date()
            max_date = data["aql_data"]["Production_Date"].max().date()
            
            st.subheader("📅 Date Range")
            # Create date range selector
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
            with col2:
                end_date = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)
            
            # Apply date filter to dataframes
            if "Production_Date" in filtered_aql_df.columns:
                filtered_aql_df = filtered_aql_df[
                    (filtered_aql_df["Production_Date"].dt.date >= start_date) & 
                    (filtered_aql_df["Production_Date"].dt.date <= end_date)
                ]
            
            if "Production_Date" in filtered_complaint_df.columns:
                filtered_complaint_df = filtered_complaint_df[
                    (filtered_complaint_df["Production_Date"].dt.date >= start_date) & 
                    (filtered_complaint_df["Production_Date"].dt.date <= end_date)
                ]
            
            if "Production_Date" in filtered_tem_vang_df.columns:
                filtered_tem_vang_df = filtered_tem_vang_df[
                    (filtered_tem_vang_df["Production_Date"].dt.date >= start_date) & 
                    (filtered_tem_vang_df["Production_Date"].dt.date <= end_date)
                ]
        except Exception as e:
            st.warning(f"Error in date filter: {e}")
    
    # Month filter - KEEP for backward compatibility
    if not data["tem_vang_data"].empty and "Production_Month" in data["tem_vang_data"].columns:
        try:
            production_months = ["All"] + sorted(data["tem_vang_data"]["Production_Month"].unique().tolist())
            selected_month = st.selectbox("📆 Select Production Month", production_months)
            
            if selected_month != "All":
                filtered_tem_vang_df = filtered_tem_vang_df[filtered_tem_vang_df["Production_Month"] == selected_month]
                
                # Apply to other dataframes
                if "Production_Month" in filtered_aql_df.columns:
                    filtered_aql_df = filtered_aql_df[filtered_aql_df["Production_Month"] == selected_month]
                
                if "Production_Month" in filtered_complaint_df.columns:
                    filtered_complaint_df = filtered_complaint_df[filtered_complaint_df["Production_Month"] == selected_month]
        except Exception as e:
            st.warning(f"Error in month filter: {e}")
    
    # Line filter
    if not data["tem_vang_data"].empty and "Line" in data["tem_vang_data"].columns:
        try:
            lines = ["All"] + sorted(data["tem_vang_data"]["Line"].unique().tolist())
            selected_line = st.selectbox("🏭 Select Production Line", lines)
            
            if selected_line != "All":
                filtered_tem_vang_df = filtered_tem_vang_df[filtered_tem_vang_df["Line"] == selected_line]
                
                # Apply to other dataframes
                if "Line" in filtered_aql_df.columns:
                    filtered_aql_df = filtered_aql_df[filtered_aql_df["Line"] == selected_line]
                
                if "Line" in filtered_complaint_df.columns:
                    filtered_complaint_df = filtered_complaint_df[filtered_complaint_df["Line"] == selected_line]
        except Exception as e:
            st.warning(f"Error in line filter: {e}")
    
    # Product filter
    if not data["complaint_data"].empty and "Tên sản phẩm" in data["complaint_data"].columns:
        try:
            products = ["All"] + sorted(data["complaint_data"]["Tên sản phẩm"].unique().tolist())
            selected_product = st.selectbox("🍜 Select Product", products)
            
            if selected_product != "All":
                filtered_complaint_df = filtered_complaint_df[filtered_complaint_df["Tên sản phẩm"] == selected_product]
                
                # Filter AQL data by item if possible
                if "Tên sản phẩm" in filtered_aql_df.columns:
                    filtered_aql_df = filtered_aql_df[filtered_aql_df["Tên sản phẩm"] == selected_product]
        except Exception as e:
            st.warning(f"Error in product filter: {e}")
    
    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Show last update time
    st.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Add auto-refresh checkbox
    auto_refresh = st.checkbox("⏱️ Enable Auto-Refresh (5m)", value=False)

# Main dashboard layout with tabs for the 3 pages
tab1, tab2, tab3 = st.tabs([
    "📈 Production Quality Analysis", 
    "🔍 Customer Complaint Analysis",
    "🔄 Internal-External Quality Link"
])

# Page 1: Production Quality Analysis (TEM VÀNG and defects by line/MDG)
with tab1:
    st.markdown('<div class="sub-header">Production Quality Overview</div>', unsafe_allow_html=True)
    
    # Key metrics row
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        if not filtered_tem_vang_df.empty:
            avg_tem_vang = filtered_tem_vang_df["TEM_VANG"].mean()
            tem_target = 2.18  # TEM VÀNG target
            tem_delta = avg_tem_vang - tem_target
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Average TEM VÀNG</div>
                <div class="metric-value">{avg_tem_vang:.2f}%</div>
                <div style="color: {'red' if tem_delta > 0 else 'green'};">
                    {f"{tem_delta:.2f}% {'above' if tem_delta > 0 else 'below'} target"}
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Average TEM VÀNG</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)
    
    with metrics_col2:
        if not filtered_tem_vang_df.empty:
            total_hold = filtered_tem_vang_df["Hold_Quantity"].sum()
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Hold Quantity</div>
                <div class="metric-value">{total_hold:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Hold Quantity</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)
    
    with metrics_col3:
        if not data["process_capability"].empty:
            avg_cpk = data["process_capability"]["Cpk"].mean()
            cpk_color = "green" if avg_cpk >= 1.33 else ("orange" if avg_cpk >= 1.0 else "red")
            cpk_status = "Excellent" if avg_cpk >= 1.33 else ("Adequate" if avg_cpk >= 1.0 else "Poor")
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Process Capability</div>
                <div class="metric-value">{avg_cpk:.2f}</div>
                <div style="color: {cpk_color};">
                    {cpk_status} capability
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Process Capability</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)
    
    with metrics_col4:
        if not filtered_aql_df.empty and "Defect code" in filtered_aql_df.columns:
            defect_types = filtered_aql_df["Defect code"].nunique()
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Defect Types</div>
                <div class="metric-value">{defect_types}</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Defect Types</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)
    
    # TEM VÀNG Analysis
    st.markdown('<div class="sub-header">TEM VÀNG Analysis</div>', unsafe_allow_html=True)
    
    tem_col1, tem_col2 = st.columns(2)
    
    with tem_col1:
        # TEM VÀNG trend over time
        if not filtered_tem_vang_df.empty:
            try:
                # Group by date to get daily average TEM VÀNG
                daily_tem_vang = filtered_tem_vang_df.groupby("Date")[["TEM_VANG", "Hold_Quantity"]].mean().reset_index()
                
                # Sort by date
                daily_tem_vang = daily_tem_vang.sort_values("Date")
                
                # Create figure
                fig = go.Figure()
                
                # Add TEM VÀNG line
                fig.add_trace(go.Scatter(
                    x=daily_tem_vang["Date"],
                    y=daily_tem_vang["TEM_VANG"],
                    mode="lines+markers",
                    name="TEM VÀNG",
                    line=dict(color="royalblue", width=2),
                    marker=dict(size=6)
                ))
                
                # Add target line
                fig.add_hline(
                    y=2.18,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Target (2.18%)"
                )
                
                # Update layout
                fig.update_layout(
                    title="TEM VÀNG Trend Over Time",
                    xaxis_title="Date",
                    yaxis_title="TEM VÀNG (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating TEM VÀNG trend chart: {str(e)}")
    
    with tem_col2:
        # TEM VÀNG by line
        if not filtered_tem_vang_df.empty:
            try:
                # Group by line to get average TEM VÀNG per line
                line_tem_vang = filtered_tem_vang_df.groupby("Line")[["TEM_VANG", "Hold_Quantity"]].mean().reset_index()
                
                # Sort by TEM VÀNG value
                line_tem_vang = line_tem_vang.sort_values("TEM_VANG", ascending=False)
                
                # Create figure
                fig = go.Figure()
                
                # Add TEM VÀNG bars
                fig.add_trace(go.Bar(
                    x=line_tem_vang["Line"],
                    y=line_tem_vang["TEM_VANG"],
                    name="TEM VÀNG",
                    marker_color="royalblue",
                    text=line_tem_vang["TEM_VANG"].round(2).astype(str) + "%",
                    textposition="auto"
                ))
                
                # Add target line
                fig.add_hline(
                    y=2.18,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Target (2.18%)"
                )
                
                # Update layout
                fig.update_layout(
                    title="TEM VÀNG by Production Line",
                    xaxis_title="Line",
                    yaxis_title="TEM VÀNG (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating TEM VÀNG by line chart: {str(e)}")
    
    # Process Capability Analysis
    st.markdown('<div class="sub-header">Process Capability Analysis</div>', unsafe_allow_html=True)
    
    if not data["process_capability"].empty:
        # Create a table with process capability data
        cap_df = data["process_capability"].copy()
        
        # Add color coding for Cpk values
        def color_cpk(val):
            if val >= 1.33:
                return 'background-color: #d4edda'  # Light green
            elif val >= 1.0:
                return 'background-color: #fff3cd'  # Light yellow
            else:
                return 'background-color: #f8d7da'  # Light red
        
        # Apply styling
        styled_cap_df = cap_df.style.applymap(color_cpk, subset=['Cpk'])
        
        # Format numeric columns
        formatted_cap_df = styled_cap_df.format({
            'Mean_TEM_VANG': '{:.2f}%',
            'Std_TEM_VANG': '{:.2f}%',
            'Cpk': '{:.2f}'
        })
        
        # Rename columns for display
        cap_df.columns = ["Line", "Mean TEM VÀNG (%)", "Std Dev (%)", "Sample Count", "Cpk"]
        
        # Display the table
        st.dataframe(formatted_cap_df, use_container_width=True, height=200)
        
        # Add interpretation
        st.markdown("""
        <div class="insight-card">
            <div class="insight-title">Process Capability Interpretation</div>
            <div class="insight-content">
                <ul>
                    <li><strong>Cpk ≥ 1.33</strong>: Excellent capability - Process is well controlled and within specifications</li>
                    <li><strong>1.00 ≤ Cpk < 1.33</strong>: Adequate capability - Process meets specifications but has room for improvement</li>
                    <li><strong>Cpk < 1.00</strong>: Poor capability - Process is not capable of consistently meeting specifications</li>
                </ul>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("⚠️ Process capability data not available. Unable to calculate Cpk values.")
    
    # Defect Analysis by Line and MDG
    st.markdown('<div class="sub-header">Defect Analysis by Line and MDG</div>', unsafe_allow_html=True)
    
    defect_col1, defect_col2 = st.columns(2)
    
    with defect_col1:
        # Pareto chart of defects
        if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
            try:
                defect_counts = data["defect_patterns"]["defect_counts"]
                defect_col = data["defect_patterns"].get("defect_column", "Defect code")
                
                # Create Pareto chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add bars for defect counts
                fig.add_trace(
                    go.Bar(
                        x=defect_counts[defect_col],
                        y=defect_counts["Count"],
                        name="Defect Count",
                        marker_color="steelblue"
                    ),
                    secondary_y=False
                )
                
                # Add line for cumulative percentage
                fig.add_trace(
                    go.Scatter(
                        x=defect_counts[defect_col],
                        y=defect_counts["Cumulative"],
                        name="Cumulative %",
                        mode="lines+markers",
                        marker=dict(color="firebrick"),
                        line=dict(color="firebrick", width=2)
                    ),
                    secondary_y=True
                )
                
                # Add 80% reference line
                fig.add_hline(
                    y=80,
                    line_dash="dash",
                    line_color="green",
                    annotation_text="80% of Defects",
                    secondary_y=True
                )
                
                # Update layout
                fig.update_layout(
                    title="Pareto Analysis of Defects",
                    xaxis_title="Defect",
                    height=350,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                # Set y-axes titles
                fig.update_yaxes(title_text="Defect Count", secondary_y=False)
                fig.update_yaxes(title_text="Cumulative %", secondary_y=True)
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add Pareto analysis insight
                if "vital_few" in data["defect_patterns"]:
                    vital_few = data["defect_patterns"]["vital_few"]
                    
                    st.markdown(f"""
                    <div class="insight-card">
                        <div class="insight-title">Pareto Analysis Insight</div>
                        <div class="insight-content">
                            <p>{len(vital_few)} defect types ({len(vital_few)/len(defect_counts)*100:.0f}% of all types) account for 80% of all defects.</p>
                            <p>Focus quality improvement efforts on: {', '.join(vital_few[defect_col].tolist()[:5])}</p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error creating Pareto chart: {str(e)}")
    
    with defect_col2:
        # Defects by line heatmap
        if "defect_patterns" in data and "pivot_line_defects" in data["defect_patterns"]:
            try:
                pivot_df = data["defect_patterns"]["pivot_line_defects"]
                
                if not pivot_df.empty:
                    # Create heatmap
                    fig = px.imshow(
                        pivot_df,
                        labels=dict(x="Defect", y="Line", color="Count"),
                        x=pivot_df.columns,
                        y=pivot_df.index,
                        color_continuous_scale="YlOrRd",
                        aspect="auto"
                    )
                    
                    # Update layout
                    fig.update_layout(
                        title="Defect Distribution by Line",
                        height=350,
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("⚠️ No defect data available for heatmap visualization")
            except Exception as e:
                st.error(f"Error creating defect heatmap: {str(e)}")
    
    # MDG Analysis - UPDATED to use data from ID AQL
    st.markdown('<div class="sub-header">MDG (Machine) Analysis</div>', unsafe_allow_html=True)
    
    if "defect_patterns" in data and "mdg_defects" in data["defect_patterns"] and not data["defect_patterns"]["mdg_defects"].empty:
        try:
            mdg_defects = data["defect_patterns"]["mdg_defects"].copy()
            defect_col = data["defect_patterns"].get("defect_column", "Defect code")
            
            # Group by Line and MDG to get total defects
            line_mdg_summary = mdg_defects.groupby(["Line", "Máy"])["Count"].sum().reset_index()
            
            # Create bar chart
            fig = px.bar(
                line_mdg_summary,
                x="Máy",
                y="Count",
                color="Line",
                title="Defects by MDG (Machine) and Line",
                labels={"Máy": "MDG (Machine)", "Count": "Defect Count"},
                barmode="group"
            )
            
            # Update layout
            fig.update_layout(
                height=400,
                margin=dict(l=40, r=40, t=40, b=40)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display top MDG-defect combinations
            st.markdown("#### Top MDG-Defect Combinations")
            
            # Group by Line, MDG, and Defect code
            top_mdg_defects = mdg_defects.sort_values("Count", ascending=False).head(10)
            
            # Create a styled dataframe
            st.dataframe(top_mdg_defects, use_container_width=True, height=250)
            
        except Exception as e:
            st.error(f"Error in MDG analysis: {str(e)}")
    else:
        st.warning("⚠️ MDG analysis data not available. Check if 'Máy' column exists in ID AQL sheet.")

# Page 2: Customer Complaint Analysis - UPDATED with line filter and improved visualizations
with tab2:
    st.markdown('<div class="sub-header">Customer Complaint Overview</div>', unsafe_allow_html=True)
    
    # Check if complaint dataframe is empty
    if filtered_complaint_df.empty:
        st.warning("⚠️ No complaint data available for analysis")
    else:
        # NEW - Add Line filter for complaint data specifically
        if "Line" in filtered_complaint_df.columns:
            complaint_lines = ["All"] + sorted(filtered_complaint_df["Line"].unique().tolist())
            selected_complaint_line = st.selectbox("🏭 Select Production Line for Complaint Analysis", complaint_lines)
            
            if selected_complaint_line != "All":
                filtered_complaint_df = filtered_complaint_df[filtered_complaint_df["Line"] == selected_complaint_line]
        
        # Key metrics row
        comp_col1, comp_col2, comp_col3, comp_col4 = st.columns(4)
        
        with comp_col1:
            if "Mã ticket" in filtered_complaint_df.columns:
                total_complaints = filtered_complaint_df["Mã ticket"].nunique()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Total Complaints</div>
                    <div class="metric-value">{total_complaints}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Missing 'Mã ticket' column")
        
        with comp_col2:
            if "SL pack/ cây lỗi" in filtered_complaint_df.columns:
                total_defective_packs = filtered_complaint_df["SL pack/ cây lỗi"].sum()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Defective Packs</div>
                    <div class="metric-value">{total_defective_packs:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Missing 'SL pack/ cây lỗi' column")
        
        with comp_col3:
            if "Tỉnh" in filtered_complaint_df.columns:
                total_provinces = filtered_complaint_df["Tỉnh"].nunique()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Affected Provinces</div>
                    <div class="metric-value">{total_provinces}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Missing 'Tỉnh' column")
        
        with comp_col4:
            if "Tên lỗi" in filtered_complaint_df.columns:
                total_defect_types = filtered_complaint_df["Tên lỗi"].nunique()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Defect Types</div>
                    <div class="metric-value">{total_defect_types}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Missing 'Tên lỗi' column")
        
        # Complaint Analysis
        st.markdown('<div class="sub-header">Complaint Analysis</div>', unsafe_allow_html=True)
        
        comp_col1, comp_col2 = st.columns(2)
        
        with comp_col1:
            if "Tên sản phẩm" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by product
                    product_complaints = filtered_complaint_df.groupby("Tên sản phẩm").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Sort by complaint count
                    product_complaints = product_complaints.sort_values("Mã ticket", ascending=False).head(10)
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Add bars for complaints
                    fig.add_trace(go.Bar(
                        y=product_complaints["Tên sản phẩm"],
                        x=product_complaints["Mã ticket"],
                        name="Complaints",
                        orientation='h',
                        marker_color='firebrick',
                        text=product_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Top 10 Products by Complaints",
                        xaxis_title="Complaint Count",
                        yaxis_title="Product",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating complaints by product chart: {str(e)}")
            else:
                st.warning("Missing required columns for product chart")
        
        with comp_col2:
            # IMPROVED - Changed pie chart to horizontal bar chart for better readability
            if "Tên lỗi" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by defect type
                    defect_complaints = filtered_complaint_df.groupby("Tên lỗi").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Calculate percentages
                    defect_complaints["Complaint %"] = (defect_complaints["Mã ticket"] / defect_complaints["Mã ticket"].sum() * 100).round(1)
                    
                    # Sort by count for better visualization
                    defect_complaints = defect_complaints.sort_values("Mã ticket", ascending=False)
                    
                    # Create horizontal bar chart instead of pie chart for better readability
                    fig = go.Figure()
                    
                    # Add horizontal bars
                    fig.add_trace(go.Bar(
                        y=defect_complaints["Tên lỗi"],
                        x=defect_complaints["Mã ticket"],
                        orientation='h',
                        marker_color='firebrick',
                        text=defect_complaints["Complaint %"].astype(str) + "%",
                        textposition="auto"
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Complaints by Defect Type",
                        xaxis_title="Number of Complaints",
                        yaxis_title="Defect Type",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating complaints by defect type chart: {str(e)}")
            else:
                st.warning("Missing required columns for defect chart")
        
        # Complaint Timeline and Production Analysis
        st.markdown('<div class="sub-header">Complaint Timeline Analysis</div>', unsafe_allow_html=True)
        
        time_col1, time_col2 = st.columns(2)
        
        with time_col1:
            if "Production_Date" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by date
                    date_complaints = filtered_complaint_df.groupby("Production_Date").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Sort by date
                    date_complaints = date_complaints.sort_values("Production_Date")
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Add line for complaints
                    fig.add_trace(go.Scatter(
                        x=date_complaints["Production_Date"],
                        y=date_complaints["Mã ticket"],
                        name="Complaints",
                        mode="lines+markers",
                        line=dict(color="royalblue", width=2),
                        marker=dict(size=6)
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Complaint Trend Over Time",
                        xaxis_title="Production Date",
                        yaxis_title="Complaint Count",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating complaint timeline chart: {str(e)}")
            else:
                st.warning("Missing date column for timeline chart")
        
        with time_col2:
            if "Line" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by line
                    line_complaints = filtered_complaint_df.groupby("Line").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Sort by complaint count
                    line_complaints = line_complaints.sort_values("Mã ticket", ascending=False)
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Add bars for complaints
                    fig.add_trace(go.Bar(
                        x=line_complaints["Line"],
                        y=line_complaints["Mã ticket"],
                        name="Complaints",
                        marker_color="navy",
                        text=line_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Add a secondary y-axis for defective packs
                    fig.add_trace(go.Scatter(
                        x=line_complaints["Line"],
                        y=line_complaints["SL pack/ cây lỗi"],
                        name="Defective Packs",
                        mode="markers",
                        marker=dict(size=12, color="firebrick"),
                        yaxis="y2"
                    ))
                    
                    # Update layout with secondary y-axis
                    fig.update_layout(
                        title="Complaints by Production Line",
                        xaxis_title="Production Line",
                        yaxis_title="Complaint Count",
                        yaxis2=dict(
                            title="Defective Packs",
                            anchor="x",
                            overlaying="y",
                            side="right"
                        ),
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=40),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating complaints by line chart: {str(e)}")
            else:
                st.warning("Missing Line column for line chart")
        
        # Geographic Distribution of Complaints
        st.markdown('<div class="sub-header">Geographic Distribution</div>', unsafe_allow_html=True)
        
        if "Tỉnh" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
            try:
                # Group by province
                province_complaints = filtered_complaint_df.groupby("Tỉnh").agg({
                    "Mã ticket": "nunique",
                    "SL pack/ cây lỗi": "sum"
                }).reset_index()
                
                # Sort by complaint count
                province_complaints = province_complaints.sort_values("Mã ticket", ascending=False)
                
                # Create figure
                fig = px.bar(
                    province_complaints.head(15),  # Top 15 provinces
                    x="Tỉnh",
                    y="Mã ticket",
                    color="SL pack/ cây lỗi",
                    title="Top Provinces by Complaint Count",
                    labels={"Tỉnh": "Province", "Mã ticket": "Complaint Count", "SL pack/ cây lỗi": "Defective Packs"},
                    color_continuous_scale="Viridis"
                )
                
                # Update layout
                fig.update_layout(
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=100),
                    xaxis_tickangle=-45
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Calculate percentages for top provinces
                top_provinces = province_complaints.head(5)
                total_complaints = province_complaints["Mã ticket"].sum()
                top_provinces["Percentage"] = (top_provinces["Mã ticket"] / total_complaints * 100).round(1)
                
                # Display insight
                st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">Geographic Insight</div>
                    <div class="insight-content">
                        <p>The top 5 provinces account for {top_provinces['Percentage'].sum():.1f}% of all complaints.</p>
                        <p>Top province ({top_provinces.iloc[0]['Tỉnh']}) has {top_provinces.iloc[0]['Percentage']:.1f}% of all complaints.</p>
                        <p>Consider targeted quality improvement programs in these high-complaint regions.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error creating geographic distribution chart: {str(e)}")
        else:
            st.warning("Missing province column for geographic analysis")
        
        # Personnel Analysis
        st.markdown('<div class="sub-header">Production Personnel Analysis</div>', unsafe_allow_html=True)
        
        personnel_col1, personnel_col2 = st.columns(2)
        
        with personnel_col1:
            if "QA" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by QA
                    qa_complaints = filtered_complaint_df.groupby("QA").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Remove NaN values
                    qa_complaints = qa_complaints.dropna(subset=["QA"])
                    
                    # Sort by complaint count
                    qa_complaints = qa_complaints.sort_values("Mã ticket", ascending=False)
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Add bars for complaints
                    fig.add_trace(go.Bar(
                        x=qa_complaints["QA"],
                        y=qa_complaints["Mã ticket"],
                        name="Complaints",
                        marker_color="purple",
                        text=qa_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Complaints by QA Personnel",
                        xaxis_title="QA Personnel",
                        yaxis_title="Complaint Count",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=80),
                        xaxis_tickangle=-45
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating complaints by QA chart: {str(e)}")
            else:
                st.warning("Missing QA column for personnel analysis")
        
        with personnel_col2:
            if "Tên Trưởng ca" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by shift leader
                    leader_complaints = filtered_complaint_df.groupby("Tên Trưởng ca").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Remove NaN values
                    leader_complaints = leader_complaints.dropna(subset=["Tên Trưởng ca"])
                    
                    # Sort by complaint count
                    leader_complaints = leader_complaints.sort_values("Mã ticket", ascending=False)
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Add bars for complaints
                    fig.add_trace(go.Bar(
                        x=leader_complaints["Tên Trưởng ca"],
                        y=leader_complaints["Mã ticket"],
                        name="Complaints",
                        marker_color="darkred",
                        text=leader_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Complaints by Shift Leader",
                        xaxis_title="Shift Leader",
                        yaxis_title="Complaint Count",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=80),
                        xaxis_tickangle=-45
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating complaints by shift leader chart: {str(e)}")
            else:
                st.warning("Missing shift leader column for personnel analysis")
        
        # Complaint Details Table
        st.markdown('<div class="sub-header">Complaint Details</div>', unsafe_allow_html=True)
        
        try:
            # Create a display dataframe with key columns
            if not filtered_complaint_df.empty:
                display_cols = [
                    "Mã ticket", "Ngày tiếp nhận", "Tỉnh", "Ngày SX", "Tên sản phẩm",
                    "SL pack/ cây lỗi", "Tên lỗi", "Line", "QA", "Tên Trưởng ca"
                ]
                
                # Only include columns that exist in the dataframe
                display_cols = [col for col in display_cols if col in filtered_complaint_df.columns]
                
                # Create display dataframe
                display_df = filtered_complaint_df[display_cols].copy()
                
                # Sort by most recent complaints first
                if "Ngày tiếp nhận" in display_df.columns:
                    display_df = display_df.sort_values("Ngày tiếp nhận", ascending=False)
                
                # Format dates for display
                for date_col in ["Ngày tiếp nhận", "Ngày SX"]:
                    if date_col in display_df.columns and pd.api.types.is_datetime64_any_dtype(display_df[date_col]):
                        display_df[date_col] = display_df[date_col].dt.strftime("%d/%m/%Y")
                
                # Display the table
                st.dataframe(display_df, use_container_width=True, height=400)
            else:
                st.warning("No complaint data available to display")
        except Exception as e:
            st.error(f"Error displaying complaint details: {str(e)}")

# Page 3: Linking Internal and External Quality
with tab3:
    st.markdown('<div class="sub-header">Internal-External Quality Link Analysis</div>', unsafe_allow_html=True)
    
    # Check if linked defects data is available
    if "linked_defects" in data and not data["linked_defects"].empty:
        # Key metrics row
        link_col1, link_col2, link_col3, link_col4 = st.columns(4)
        
        linked_df = data["linked_defects"].copy()
        
        with link_col1:
            total_linkages = len(linked_df)
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Linkages</div>
                <div class="metric-value">{total_linkages}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with link_col2:
            avg_ratio = linked_df["Defect_to_Complaint_Ratio"].replace([float('inf'), -float('inf')], np.nan).mean()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Avg. Defect:Complaint Ratio</div>
                <div class="metric-value">{avg_ratio:.1f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with link_col3:
            unique_defect_types = linked_df["Defect_Type"].nunique()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Linked Defect Types</div>
                <div class="metric-value">{unique_defect_types}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with link_col4:
            total_lines = linked_df["Line"].nunique()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Affected Lines</div>
                <div class="metric-value">{total_lines}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Defect to Complaint Ratio Analysis
        st.markdown('<div class="sub-header">Defect-to-Complaint Ratio Analysis</div>', unsafe_allow_html=True)
        
        ratio_col1, ratio_col2 = st.columns(2)
        
        with ratio_col1:
            try:
                # Group by defect type
                defect_type_ratios = linked_df.groupby("Defect_Type").agg({
                    "Internal_Defect_Count": "sum",
                    "Customer_Complaint_Count": "sum"
                }).reset_index()
                
                # Calculate overall ratio
                defect_type_ratios["Ratio"] = defect_type_ratios["Internal_Defect_Count"] / defect_type_ratios["Customer_Complaint_Count"]
                
                # Sort by ratio
                defect_type_ratios = defect_type_ratios.sort_values("Ratio")
                
                # Create figure
                fig = go.Figure()
                
                # Add bars for ratio
                fig.add_trace(go.Bar(
                    y=defect_type_ratios["Defect_Type"],
                    x=defect_type_ratios["Ratio"],
                    orientation="h",
                    marker_color="teal",
                    text=defect_type_ratios["Ratio"].round(1),
                    textposition="outside"
                ))
                
                # Update layout
                fig.update_layout(
                    title="Defect-to-Complaint Ratio by Defect Type",
                    xaxis_title="Ratio (Internal Defects : Customer Complaints)",
                    yaxis_title="Defect Type",
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add interpretation
                st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">Ratio Interpretation</div>
                    <div class="insight-content">
                        <p>A higher ratio indicates more internal defects are caught for each customer complaint.</p>
                        <p>A lower ratio suggests that defects are not being effectively caught during production.</p>
                        <p><strong>{defect_type_ratios.iloc[-1]['Defect_Type']}</strong> has the highest ratio ({defect_type_ratios.iloc[-1]['Ratio']:.1f}), indicating effective internal detection.</p>
                        <p><strong>{defect_type_ratios.iloc[0]['Defect_Type']}</strong> has the lowest ratio ({defect_type_ratios.iloc[0]['Ratio']:.1f}), suggesting improvement needed in detection.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error creating ratio analysis chart: {str(e)}")
        
        with ratio_col2:
            try:
                # Group by line
                line_ratios = linked_df.groupby("Line").agg({
                    "Internal_Defect_Count": "sum",
                    "Customer_Complaint_Count": "sum"
                }).reset_index()
                
                # Calculate overall ratio
                line_ratios["Ratio"] = line_ratios["Internal_Defect_Count"] / line_ratios["Customer_Complaint_Count"]
                
                # Create scatter plot
                fig = px.scatter(
                    line_ratios,
                    x="Internal_Defect_Count",
                    y="Customer_Complaint_Count",
                    size="Ratio",
                    color="Line",
                    hover_name="Line",
                    text="Line",
                    title="Internal Defects vs. Customer Complaints by Line"
                )
                
                # Update markers
                fig.update_traces(
                    marker=dict(sizemode="area", sizeref=0.1),
                    textposition="top center"
                )
                
                # Add diagonal reference line (1:1 ratio)
                max_val = max(line_ratios["Internal_Defect_Count"].max(), line_ratios["Customer_Complaint_Count"].max())
                fig.add_trace(go.Scatter(
                    x=[0, max_val],
                    y=[0, max_val],
                    mode="lines",
                    line=dict(color="gray", dash="dash"),
                    name="1:1 Ratio"
                ))
                
                # Update layout
                fig.update_layout(
                    xaxis_title="Internal Defect Count",
                    yaxis_title="Customer Complaint Count",
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating line ratio chart: {str(e)}")
        
        # Timeline Analysis
        st.markdown('<div class="sub-header">Timeline Analysis</div>', unsafe_allow_html=True)
        
        try:
            # Group by date
            date_analysis = linked_df.groupby("Production_Date").agg({
                "Internal_Defect_Count": "sum",
                "Customer_Complaint_Count": "sum"
            }).reset_index()
            
            # Calculate ratio
            date_analysis["Ratio"] = date_analysis["Internal_Defect_Count"] / date_analysis["Customer_Complaint_Count"]
            
            # Sort by date
            date_analysis = date_analysis.sort_values("Production_Date")
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add lines for internal defects and customer complaints
            fig.add_trace(
                go.Scatter(
                    x=date_analysis["Production_Date"],
                    y=date_analysis["Internal_Defect_Count"],
                    name="Internal Defects",
                    mode="lines+markers",
                    line=dict(color="royalblue", width=2)
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Scatter(
                    x=date_analysis["Production_Date"],
                    y=date_analysis["Customer_Complaint_Count"],
                    name="Customer Complaints",
                    mode="lines+markers",
                    line=dict(color="firebrick", width=2)
                ),
                secondary_y=False
            )
            
            # Add ratio line
            fig.add_trace(
                go.Scatter(
                    x=date_analysis["Production_Date"],
                    y=date_analysis["Ratio"],
                    name="Defect:Complaint Ratio",
                    mode="lines",
                    line=dict(color="green", width=2, dash="dash")
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title="Internal Defects and Customer Complaints Over Time",
                xaxis_title="Production Date",
                height=400,
                margin=dict(l=40, r=40, t=40, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02)
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Count", secondary_y=False)
            fig.update_yaxes(title_text="Defect:Complaint Ratio", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Calculate correlation
            correlation = date_analysis["Internal_Defect_Count"].corr(date_analysis["Customer_Complaint_Count"])
            
            # Add insight about correlation
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-title">Correlation Analysis</div>
                <div class="insight-content">
                    <p>The correlation between internal defects and customer complaints is <strong>{correlation:.2f}</strong>.</p>
                    <p>{'This positive correlation suggests that increases in internal defects are associated with increases in customer complaints, with a delay of days to weeks.' if correlation > 0 else 'This correlation suggests that internal defects and customer complaints may not be directly related or that there is a significant delay between production issues and customer feedback.'}</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error creating timeline analysis chart: {str(e)}")
        
        # Detection Effectiveness Analysis
        st.markdown('<div class="sub-header">Detection Effectiveness Analysis</div>', unsafe_allow_html=True)
        
        try:
            # Calculate detection effectiveness for each defect type
            effectiveness_df = linked_df.groupby("Defect_Type").agg({
                "Internal_Defect_Count": "sum",
                "Customer_Complaint_Count": "sum"
            }).reset_index()
            
            # Calculate effectiveness percentage
            effectiveness_df["Total_Issues"] = effectiveness_df["Internal_Defect_Count"] + effectiveness_df["Customer_Complaint_Count"]
            effectiveness_df["Detection_Effectiveness"] = (effectiveness_df["Internal_Defect_Count"] / effectiveness_df["Total_Issues"] * 100).round(1)
            
            # Sort by effectiveness
            effectiveness_df = effectiveness_df.sort_values("Detection_Effectiveness")
            
            # Create figure
            fig = go.Figure()
            
            # Add bars for effectiveness
            fig.add_trace(go.Bar(
                y=effectiveness_df["Defect_Type"],
                x=effectiveness_df["Detection_Effectiveness"],
                orientation="h",
                marker_color=effectiveness_df["Detection_Effectiveness"].map(lambda x: "green" if x >= 90 else ("orange" if x >= 75 else "red")),
                text=effectiveness_df["Detection_Effectiveness"].astype(str) + "%",
                textposition="outside"
            ))
            
            # Add reference lines
            fig.add_vline(x=75, line_dash="dash", line_color="orange", annotation_text="75% (Acceptable)")
            fig.add_vline(x=90, line_dash="dash", line_color="green", annotation_text="90% (Excellent)")
            
            # Update layout
            fig.update_layout(
                title="Internal Quality Detection Effectiveness by Defect Type",
                xaxis_title="Detection Effectiveness (%)",
                yaxis_title="Defect Type",
                height=400,
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(range=[0, 100])
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Identify poor detection areas
            poor_detection = effectiveness_df[effectiveness_df["Detection_Effectiveness"] < 75]
            
            if not poor_detection.empty:
                st.markdown(f"""
                <div class="warning-card">
                    <div class="warning-title">Poor Detection Areas</div>
                    <div class="insight-content">
                        <p>The following defect types have detection effectiveness below 75%, indicating significant improvement opportunities:</p>
                        <ul>
                            {''.join([f"<li><strong>{row['Defect_Type']}</strong>: {row['Detection_Effectiveness']}% effective</li>" for _, row in poor_detection.iterrows()])}
                        </ul>
                        <p>Consider implementing targeted improvements in detection methods for these defect types.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error creating detection effectiveness analysis: {str(e)}")
    else:
        st.warning("""
        ⚠️ No linked defect data available. This could be due to:
        
        1. Insufficient historical data to establish connections
        2. Mismatched defect codes between internal and customer data
        3. Data integration issues
        
        Please ensure both AQL and complaint data are available and properly formatted.
        """)

# Footer with dashboard information
st.markdown("""
<div style="text-align: center; padding: 15px; margin-top: 30px; border-top: 1px solid #eee;">
    <p style="color: #555; font-size: 0.9rem;">
        FMCG Quality Analytics Dashboard | Created by Quality Assurance Department
    </p>
</div>
""", unsafe_allow_html=True)

# Auto-refresh mechanism
if auto_refresh:
    time.sleep(300)  # Wait for 5 minutes
    st.experimental_rerun()
