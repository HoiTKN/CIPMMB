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
    page_title="Báo cáo chất lượng CF MMB",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced CSS with improved color scheme and responsive design
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #0d2c54;
        text-align: center;
        margin-bottom: 1rem;
        background: linear-gradient(90deg, #eef2f7, #ffffff, #eef2f7);
        padding: 10px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #0d2c54;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        border-bottom: 2px solid #d0e1ff;
        padding-bottom: 5px;
    }
    .metric-card {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.08);
        border-left: 5px solid #0d2c54;
        transition: transform 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 15px rgba(0, 0, 0, 0.1);
    }
    .metric-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #64748b;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #0d2c54;
        margin: 10px 0;
    }
    .metric-positive {
        color: #10b981;
        font-weight: 600;
        font-size: 1rem;
    }
    .metric-negative {
        color: #ef4444;
        font-weight: 600;
        font-size: 1rem;
    }
    .insight-card {
        background-color: #f0f7ff;
        border-left: 5px solid #3b82f6;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        transition: all 0.2s ease;
    }
    .insight-card:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
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
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .warning-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #be123c;
        margin-bottom: 8px;
    }
    .recommendation-card {
        background-color: #ecfdf5;
        border-left: 5px solid #10b981;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .recommendation-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #047857;
        margin-bottom: 8px;
    }
    .tab-container {
        margin-top: 1rem;
    }
    .data-table {
        border-radius: 10px !important;
        overflow: hidden;
    }
    .data-table table {
        border-collapse: collapse;
        width: 100%;
    }
    .data-table th {
        background-color: #0d2c54 !important;
        color: white !important;
        font-weight: 600;
        padding: 12px !important;
    }
    .data-table td {
        padding: 10px !important;
    }
    .data-table tr:nth-child(even) {
        background-color: #f8f9fa;
    }
    .data-table tr:hover {
        background-color: #eef2f7;
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
        background-color: #0d2c54;
        color: white;
    }
    .critical-issue {
        background-color: #FEE2E2;
        padding: 8px 12px;
        border-radius: 5px;
        color: #991B1B;
        font-weight: bold;
        display: inline-block;
        margin-right: 10px;
        margin-bottom: 10px;
    }
    .badge {
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        display: inline-block;
        margin-right: 5px;
    }
    .badge-good {
        background-color: #d1fae5;
        color: #065f46;
    }
    .badge-warning {
        background-color: #fff7ed;
        color: #c2410c;
    }
    .badge-bad {
        background-color: #fee2e2;
        color: #b91c1c;
    }
    .shift-info {
        font-size: 0.85rem;
        color: #6b7280;
        font-style: italic;
    }
    .line-header {
        display: inline-block;
        padding: 4px 10px;
        background-color: #0d2c54;
        color: white;
        border-radius: 4px;
        margin-right: 10px;
    }
    .trend-indicator {
        font-size: 1.2rem;
        margin-left: 5px;
    }
    .trend-up {
        color: #ef4444;
    }
    .trend-down {
        color: #10b981;
    }
    .trend-stable {
        color: #f59e0b;
    }
    /* Custom toggle styles */
    .toggle-switch {
        position: relative;
        display: inline-block;
        width: 60px;
        height: 34px;
    }
    .toggle-switch input {
        opacity: 0;
        width: 0;
        height: 0;
    }
    .slider {
        position: absolute;
        cursor: pointer;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-color: #ccc;
        transition: .4s;
        border-radius: 34px;
    }
    .slider:before {
        position: absolute;
        content: "";
        height: 26px;
        width: 26px;
        left: 4px;
        bottom: 4px;
        background-color: white;
        transition: .4s;
        border-radius: 50%;
    }
    input:checked + .slider {
        background-color: #0d2c54;
    }
    input:focus + .slider {
        box-shadow: 0 0 1px #0d2c54;
    }
    input:checked + .slider:before {
        transform: translateX(26px);
    }
    .footer {
        text-align: center;
        margin-top: 40px;
        padding: 20px;
        font-size: 0.8rem;
        color: #6b7280;
        border-top: 1px solid #e5e7eb;
    }
    /* Responsive adjustments */
    @media (max-width: 768px) {
        .metric-card {
            margin-bottom: 15px;
        }
        .main-header {
            font-size: 2rem;
        }
        .sub-header {
            font-size: 1.5rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Define the scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authentication function
def authenticate():
    """Authentication using OAuth token"""
    try:
        creds = None
        
        # Check if token.json exists first
        if os.path.exists('token.json'):
            try:
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            except Exception as e:
                st.error(f"Lỗi khi tải token.json: {e}")
        # Otherwise create it from the environment variable or Streamlit secrets
        elif 'GOOGLE_TOKEN_JSON' in os.environ:
            try:
                token_info = os.environ.get('GOOGLE_TOKEN_JSON')
                with open('token.json', 'w') as f:
                    f.write(token_info)
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            except Exception as e:
                st.error(f"Lỗi khi tải từ biến môi trường: {e}")
        elif 'GOOGLE_TOKEN_JSON' in st.secrets:
            try:
                token_info = st.secrets['GOOGLE_TOKEN_JSON']
                with open('token.json', 'w') as f:
                    f.write(token_info)
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            except Exception as e:
                st.error(f"Lỗi khi tải từ Streamlit secrets: {e}")
        else:
            st.error("❌ Không tìm thấy file token.json hoặc GOOGLE_TOKEN_JSON")
            return None
        
        # Refresh token if expired
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
            except Exception as e:
                st.error(f"Lỗi khi làm mới token: {e}")
                
        # Return authorized client
        if creds:
            return gspread.authorize(creds)
        else:
            return None
    
    except Exception as e:
        st.error(f"❌ Lỗi xác thực: {str(e)}")
        return None

# Improved function to extract hour from different formats
def extract_hour(hour_str):
    """Extract numeric hour from different format strings like '2h'"""
    if pd.isna(hour_str):
        return np.nan
    
    # If the hour is already a number, return it
    if isinstance(hour_str, (int, float)):
        return float(hour_str)
    
    # If it's a string, extract the number part
    if isinstance(hour_str, str):
        # Remove 'h' and any whitespace, then try to convert to float
        cleaned_str = hour_str.lower().replace('h', '').strip()
        try:
            return float(cleaned_str)
        except (ValueError, TypeError):
            return np.nan
    
    return np.nan

# Function to load AQL data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aql_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Không thể xác thực với Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (AQL data)
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            
            # Get the ID AQL worksheet
            try:
                worksheet = spreadsheet.worksheet('ID AQL')
            except gspread.exceptions.WorksheetNotFound:
                connection_status.error(f"❌ Không tìm thấy bảng 'ID AQL'")
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
                    connection_status.warning(f"⚠️ Không thể xử lý cột ngày: {e}")
            
            # Make sure numeric columns are properly typed
            if "Số lượng hold ( gói/thùng)" in df.columns:
                df["Số lượng hold ( gói/thùng)"] = pd.to_numeric(df["Số lượng hold ( gói/thùng)"], errors='coerce').fillna(0)
            
            # Ensure Line column is converted to string for consistent filtering
            if "Line" in df.columns:
                df["Line"] = df["Line"].astype(str)
            
            # Process shift information based on hour
            if "Giờ" in df.columns:
                # First, create a cleaned numeric hour column
                df["Giờ_numeric"] = df["Giờ"].apply(extract_hour)
                
                # Also keep the original Giờ column intact for compatibility
                # But ensure it's numeric for functions that expect it that way
                df["Giờ"] = pd.to_numeric(df["Giờ"], errors='coerce')
                
                # Define a function to map hours to shifts
                def map_hour_to_shift(hour):
                    if pd.isna(hour):
                        return "Unknown"
                    hour = float(hour)
                    if 6 <= hour < 14:
                        return "1"
                    elif 14 <= hour < 22:
                        return "2"
                    else:  # 22-24 or 0-6
                        return "3"
                
                # Apply the mapping function using the numeric hour value
                df["Shift"] = df["Giờ_numeric"].apply(map_hour_to_shift)
                
                # Convert Shift to string
                df["Shift"] = df["Shift"].astype(str)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Lỗi truy cập bảng dữ liệu AQL: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Lỗi tải dữ liệu AQL: {str(e)}")
        return pd.DataFrame()

# Function to load production data (Sản lượng)
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_production_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Không thể xác thực với Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (AQL data - same spreadsheet, different worksheet)
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            
            # Get the Sản lượng worksheet
            try:
                worksheet = spreadsheet.worksheet('Sản lượng')
            except gspread.exceptions.WorksheetNotFound:
                connection_status.error(f"❌ Không tìm thấy bảng 'Sản lượng'")
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
                    connection_status.warning(f"⚠️ Không thể xử lý cột ngày: {e}")
            
            # Make sure numeric columns are properly typed
            if "Sản lượng" in df.columns:
                df["Sản lượng"] = pd.to_numeric(df["Sản lượng"], errors='coerce').fillna(0)
            
            # Ensure Line column is converted to string for consistent filtering
            if "Line" in df.columns:
                df["Line"] = df["Line"].astype(str)
            
            # Ensure Ca column is properly formatted
            if "Ca" in df.columns:
                df["Ca"] = df["Ca"].astype(str)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Lỗi truy cập bảng dữ liệu sản lượng: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Lỗi tải dữ liệu sản lượng: {str(e)}")
        return pd.DataFrame()

# Function to load AQL gói data for defect name mapping
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aql_goi_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Không thể xác thực với Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            
            # Get the AQL gói worksheet
            try:
                worksheet = spreadsheet.worksheet('AQL gói')
            except gspread.exceptions.WorksheetNotFound:
                st.error(f"❌ Không tìm thấy bảng 'AQL gói'")
                return pd.DataFrame()
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Return only defect code and name columns if they exist
            defect_code_col = next((col for col in df.columns if "code" in col.lower()), None)
            defect_name_col = next((col for col in df.columns if "name" in col.lower() or "tên" in col.lower()), None)
            
            if defect_code_col and defect_name_col:
                return df[[defect_code_col, defect_name_col]]
            else:
                # If specific columns not found, return the full dataframe
                return df
            
        except Exception as e:
            st.error(f"❌ Lỗi truy cập bảng AQL gói: {str(e)}")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"❌ Lỗi tải dữ liệu AQL gói: {str(e)}")
        return pd.DataFrame()

# Function to load AQL Tô ly data for defect name mapping
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_aql_to_ly_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Không thể xác thực với Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL
        sheet_url = "https://docs.google.com/spreadsheets/d/1MxvsyZTMMO0L5Cf1FzuXoKD634OClCCefeLjv9B49XU/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            
            # Get the AQL Tô ly worksheet
            try:
                worksheet = spreadsheet.worksheet('AQL Tô ly')
            except gspread.exceptions.WorksheetNotFound:
                st.error(f"❌ Không tìm thấy bảng 'AQL Tô ly'")
                return pd.DataFrame()
            
            # Get all records
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Return only defect code and name columns if they exist
            defect_code_col = next((col for col in df.columns if "code" in col.lower()), None)
            defect_name_col = next((col for col in df.columns if "name" in col.lower() or "tên" in col.lower()), None)
            
            if defect_code_col and defect_name_col:
                return df[[defect_code_col, defect_name_col]]
            else:
                # If specific columns not found, return the full dataframe
                return df
            
        except Exception as e:
            st.error(f"❌ Lỗi truy cập bảng AQL Tô ly: {str(e)}")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"❌ Lỗi tải dữ liệu AQL Tô ly: {str(e)}")
        return pd.DataFrame()

# Function to calculate TEM VÀNG
def calculate_tem_vang(aql_df, production_df):
    """Calculate TEM VÀNG by combining AQL hold data with production volume data"""
    try:
        # Check if dataframes are empty
        if aql_df.empty or production_df.empty:
            st.error("❌ Không thể tính TEM VÀNG - thiếu dữ liệu")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Group AQL data by date and line to get total hold quantities
        if "Production_Date" in aql_copy.columns and "Line" in aql_copy.columns and "Số lượng hold ( gói/thùng)" in aql_copy.columns:
            # Make sure we don't count rows with no hold quantity
            aql_copy.loc[aql_copy["Số lượng hold ( gói/thùng)"].isna(), "Số lượng hold ( gói/thùng)"] = 0
            
            aql_grouped = aql_copy.groupby(["Production_Date", "Line"])["Số lượng hold ( gói/thùng)"].sum().reset_index()
            aql_grouped.columns = ["Date", "Line", "Hold_Quantity"]
        else:
            missing_cols = []
            if "Production_Date" not in aql_copy.columns:
                missing_cols.append("Production_Date")
            if "Line" not in aql_copy.columns:
                missing_cols.append("Line")
            if "Số lượng hold ( gói/thùng)" not in aql_copy.columns:
                missing_cols.append("Số lượng hold ( gói/thùng)")
            
            st.warning(f"⚠️ Thiếu cột cần thiết trong dữ liệu AQL để tính TEM VÀNG: {', '.join(missing_cols)}")
            return pd.DataFrame()
        
        # Group production data by date and line to get total production volumes
        if "Production_Date" in prod_copy.columns and "Line" in prod_copy.columns and "Sản lượng" in prod_copy.columns:
            # Make sure we don't count rows with no production volume
            prod_copy.loc[prod_copy["Sản lượng"].isna(), "Sản lượng"] = 0
            
            prod_grouped = prod_copy.groupby(["Production_Date", "Line"])["Sản lượng"].sum().reset_index()
            prod_grouped.columns = ["Date", "Line", "Production_Volume"]
        else:
            missing_cols = []
            if "Production_Date" not in prod_copy.columns:
                missing_cols.append("Production_Date")
            if "Line" not in prod_copy.columns:
                missing_cols.append("Line")
            if "Sản lượng" not in prod_copy.columns:
                missing_cols.append("Sản lượng")
            
            st.warning(f"⚠️ Thiếu cột cần thiết trong dữ liệu sản lượng để tính TEM VÀNG: {', '.join(missing_cols)}")
            return pd.DataFrame()
        
        # Merge the grouped data
        tem_vang_df = pd.merge(aql_grouped, prod_grouped, on=["Date", "Line"], how="inner")
        
        # Calculate TEM VÀNG percentage
        tem_vang_df["TEM_VANG"] = (tem_vang_df["Hold_Quantity"] / tem_vang_df["Production_Volume"]) * 100
        
        # Add month column for filtering
        tem_vang_df["Production_Month"] = tem_vang_df["Date"].dt.strftime("%m/%Y")
        
        return tem_vang_df
        
    except Exception as e:
        st.error(f"❌ Lỗi tính toán TEM VÀNG: {str(e)}")
        return pd.DataFrame()

# Improved function to calculate TEM VÀNG by shift
def calculate_tem_vang_by_shift(aql_df, production_df):
    """Calculate TEM VÀNG by shift using AQL and production data"""
    try:
        # Check if dataframes are empty
        if aql_df.empty or production_df.empty:
            st.warning("⚠️ Không thể tính TEM VÀNG theo ca - thiếu dữ liệu")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Ensure we have all required columns
        required_aql_cols = ["Production_Date", "Line", "Số lượng hold ( gói/thùng)"]
        missing_aql_cols = [col for col in required_aql_cols if col not in aql_copy.columns]
        
        required_prod_cols = ["Production_Date", "Line", "Sản lượng", "Ca"]
        missing_prod_cols = [col for col in required_prod_cols if col not in prod_copy.columns]
        
        if missing_aql_cols:
            st.warning(f"⚠️ Thiếu cột trong dữ liệu AQL để tính TEM VÀNG theo ca: {', '.join(missing_aql_cols)}")
            return pd.DataFrame()
        
        if missing_prod_cols:
            st.warning(f"⚠️ Thiếu cột trong dữ liệu sản lượng để tính TEM VÀNG theo ca: {', '.join(missing_prod_cols)}")
            return pd.DataFrame()
        
        # Ensure we have shift information for AQL data
        if "Shift" not in aql_copy.columns:
            # If we don't have Shift column but have Giờ columns, derive Shift
            if "Giờ_numeric" in aql_copy.columns:
                # Use the already calculated numeric hour column
                hour_col = "Giờ_numeric"
            elif "Giờ" in aql_copy.columns:
                # Process Giờ column if we don't have the numeric version
                aql_copy["Giờ_numeric"] = aql_copy["Giờ"].apply(extract_hour)
                hour_col = "Giờ_numeric"
            else:
                st.warning("⚠️ Không thể xác định ca từ dữ liệu AQL - thiếu cột 'Shift' và 'Giờ'")
                return pd.DataFrame()
                
            # Define shift mapping function
            def hour_to_shift(hour):
                if pd.isna(hour):
                    return "Unknown"
                hour = float(hour)
                if 6 <= hour < 14:
                    return "1"
                elif 14 <= hour < 22:
                    return "2"
                else:  # 22-24 or 0-6
                    return "3"
            
            # Apply mapping function
            aql_copy["Shift"] = aql_copy[hour_col].apply(hour_to_shift)
        
        # Ensure Shift is string type for both dataframes
        aql_copy["Shift"] = aql_copy["Shift"].astype(str)
        prod_copy["Ca"] = prod_copy["Ca"].astype(str)
        
        # Group AQL data by date, line, shift
        try:
            aql_grouped = aql_copy.groupby(["Production_Date", "Line", "Shift"])["Số lượng hold ( gói/thùng)"].sum().reset_index()
            aql_grouped.columns = ["Date", "Line", "Shift", "Hold_Quantity"]
        except Exception as e:
            st.error(f"Lỗi khi nhóm dữ liệu AQL theo ca: {e}")
            return pd.DataFrame()
        
        # Group production data by date, line, shift
        try:
            prod_grouped = prod_copy.groupby(["Production_Date", "Line", "Ca"])["Sản lượng"].sum().reset_index()
            prod_grouped.columns = ["Date", "Line", "Shift", "Production_Volume"]
        except Exception as e:
            st.error(f"Lỗi khi nhóm dữ liệu sản lượng theo ca: {e}")
            return pd.DataFrame()
        
        # Merge the data
        tem_vang_shift_df = pd.merge(
            aql_grouped, 
            prod_grouped, 
            on=["Date", "Line", "Shift"],
            how="inner"
        )
        
        # Calculate TEM VÀNG
        tem_vang_shift_df["TEM_VANG"] = (tem_vang_shift_df["Hold_Quantity"] / tem_vang_shift_df["Production_Volume"]) * 100
        
        # Add month column for filtering
        tem_vang_shift_df["Production_Month"] = tem_vang_shift_df["Date"].dt.strftime("%m/%Y")
        
        return tem_vang_shift_df
        
    except Exception as e:
        st.error(f"❌ Lỗi tính toán TEM VÀNG theo ca: {str(e)}")
        return pd.DataFrame()

# Function to calculate TEM VÀNG by shift leader
def calculate_tem_vang_by_leader(aql_df, production_df):
    """Calculate TEM VÀNG by shift leader using AQL and production data"""
    try:
        # Check if dataframes are empty
        if aql_df.empty or production_df.empty:
            st.warning("⚠️ Không thể tính TEM VÀNG theo trưởng ca - thiếu dữ liệu")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Find the columns for Tên Trưởng ca in AQL data (look for "Tên Trưởng ca" first)
        ten_truong_ca_col = None
        truong_ca_col = None
        
        for col in aql_copy.columns:
            if "tên trưởng ca" in col.lower():
                ten_truong_ca_col = col
                break
                
        # If we couldn't find "Tên Trưởng ca", fall back to "Trưởng ca"
        if not ten_truong_ca_col:
            for col in aql_copy.columns:
                if "trưởng ca" in col.lower() and "tên" not in col.lower():
                    truong_ca_col = col
                    break
        
        # Find the columns for Người phụ trách in production data
        nguoi_phu_trach_col = None
        for col in prod_copy.columns:
            if "người phụ trách" in col.lower() or "phụ trách" in col.lower():
                nguoi_phu_trach_col = col
                break
        
        # Use Tên Trưởng ca if available, otherwise fall back to Trưởng ca
        leader_col = ten_truong_ca_col if ten_truong_ca_col else truong_ca_col
        
        if not leader_col:
            st.warning("⚠️ Không tìm thấy cột 'Tên Trưởng ca' hoặc 'Trưởng ca' trong dữ liệu AQL")
            return pd.DataFrame()
        
        if not nguoi_phu_trach_col:
            st.warning("⚠️ Không tìm thấy cột người phụ trách trong dữ liệu sản lượng")
            return pd.DataFrame()
        
        # Ensure required columns exist
        required_aql_cols = ["Production_Date", "Line", "Số lượng hold ( gói/thùng)"]
        missing_aql_cols = [col for col in required_aql_cols if col not in aql_copy.columns]
        
        required_prod_cols = ["Production_Date", "Line", "Sản lượng"]
        missing_prod_cols = [col for col in required_prod_cols if col not in prod_copy.columns]
        
        if missing_aql_cols:
            st.warning(f"⚠️ Thiếu cột trong dữ liệu AQL để tính TEM VÀNG theo trưởng ca: {', '.join(missing_aql_cols)}")
            return pd.DataFrame()
        
        if missing_prod_cols:
            st.warning(f"⚠️ Thiếu cột trong dữ liệu sản lượng để tính TEM VÀNG theo trưởng ca: {', '.join(missing_prod_cols)}")
            return pd.DataFrame()
        
        # Group AQL data by date, line, leader
        try:
            aql_grouped = aql_copy.groupby(["Production_Date", "Line", leader_col])["Số lượng hold ( gói/thùng)"].sum().reset_index()
            aql_grouped.columns = ["Date", "Line", "Leader", "Hold_Quantity"]
        except Exception as e:
            st.error(f"Lỗi khi nhóm dữ liệu AQL theo trưởng ca: {e}")
            return pd.DataFrame()
        
        # Group production data by date, line, leader
        try:
            prod_grouped = prod_copy.groupby(["Production_Date", "Line", nguoi_phu_trach_col])["Sản lượng"].sum().reset_index()
            prod_grouped.columns = ["Date", "Line", "Leader", "Production_Volume"]
        except Exception as e:
            st.error(f"Lỗi khi nhóm dữ liệu sản lượng theo người phụ trách: {e}")
            return pd.DataFrame()
        
        # Standardize leader names for better matching
        aql_grouped["Leader"] = aql_grouped["Leader"].astype(str).str.strip().str.lower()
        prod_grouped["Leader"] = prod_grouped["Leader"].astype(str).str.strip().str.lower()
        
        # Merge the data
        tem_vang_leader_df = pd.merge(
            aql_grouped, 
            prod_grouped, 
            on=["Date", "Line", "Leader"],
            how="inner"
        )
        
        # Calculate TEM VÀNG
        tem_vang_leader_df["TEM_VANG"] = (tem_vang_leader_df["Hold_Quantity"] / tem_vang_leader_df["Production_Volume"]) * 100
        
        # Add month column for filtering
        tem_vang_leader_df["Production_Month"] = tem_vang_leader_df["Date"].dt.strftime("%m/%Y")
        
        return tem_vang_leader_df
        
    except Exception as e:
        st.error(f"❌ Lỗi tính toán TEM VÀNG theo trưởng ca: {str(e)}")
        return pd.DataFrame()

# Function to calculate TEM VÀNG by hour - improved handling of hour formats
def calculate_tem_vang_by_hour(aql_df, production_df):
    """Calculate TEM VÀNG by hour using AQL and production data"""
    try:
        # Check if dataframes are empty
        if aql_df.empty or production_df.empty:
            st.warning("⚠️ Không thể tính TEM VÀNG theo giờ - thiếu dữ liệu")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Check if we have hour information
        if "Giờ_numeric" in aql_copy.columns:
            hour_col = "Giờ_numeric"
        elif "Giờ" in aql_copy.columns:
            # Process the Giờ column if needed
            aql_copy["Giờ_numeric"] = aql_copy["Giờ"].apply(extract_hour)
            hour_col = "Giờ_numeric"
        else:
            st.warning("⚠️ Thiếu cột 'Giờ' trong dữ liệu AQL để tính TEM VÀNG theo giờ")
            return pd.DataFrame()
        
        # Check if we have shift column in production data
        if "Ca" not in prod_copy.columns:
            st.warning("⚠️ Thiếu cột 'Ca' trong dữ liệu sản lượng để tính TEM VÀNG theo giờ")
            return pd.DataFrame()
        
        # Make sure the hour column has valid numeric values
        aql_copy[hour_col] = pd.to_numeric(aql_copy[hour_col], errors='coerce')
        
        # Map hours to shifts
        hour_to_shift = {
            h: "1" if 6 <= h < 14 else ("2" if 14 <= h < 22 else "3")
            for h in range(24)
        }
        
        # Add shift column based on hour if not already present
        if "Shift" not in aql_copy.columns:
            aql_copy["Shift"] = aql_copy[hour_col].map(
                lambda h: hour_to_shift.get(h, "Unknown") if pd.notna(h) else "Unknown"
            )
        
        # Group AQL data by hour, ignoring date and line to get aggregated values
        aql_hour_grouped = aql_copy.groupby(hour_col)["Số lượng hold ( gói/thùng)"].sum().reset_index()
        aql_hour_grouped.columns = ["Hour", "Hold_Quantity"]
        
        # Add shift column to the grouped data
        aql_hour_grouped["Shift"] = aql_hour_grouped["Hour"].map(
            lambda h: hour_to_shift.get(h, "Unknown") if pd.notna(h) else "Unknown"
        )
        
        # Group production data by shift (Ca)
        prod_copy["Ca"] = prod_copy["Ca"].astype(str)
        shift_production = prod_copy.groupby("Ca")["Sản lượng"].sum().reset_index()
        shift_production.columns = ["Shift", "Production_Volume"]
        
        # Define hours per shift for distribution
        hours_per_shift = {
            "1": 8,  # 6-14 (8 hours)
            "2": 8,  # 14-22 (8 hours)
            "3": 8   # 22-6 (8 hours)
        }
        
        # Merge to get production volume for each hour
        tem_vang_hour_df = pd.merge(
            aql_hour_grouped,
            shift_production,
            on="Shift",
            how="left"
        )
        
        # Calculate hourly production by dividing shift production by hours per shift
        tem_vang_hour_df["Hourly_Production"] = tem_vang_hour_df.apply(
            lambda row: row["Production_Volume"] / hours_per_shift.get(row["Shift"], 8) 
            if pd.notna(row["Shift"]) and pd.notna(row["Production_Volume"]) and row["Production_Volume"] > 0
            else 0,
            axis=1
        )
        
        # Calculate TEM VÀNG percentage
        tem_vang_hour_df["TEM_VANG"] = tem_vang_hour_df.apply(
            lambda row: (row["Hold_Quantity"] / row["Hourly_Production"]) * 100 
            if row["Hourly_Production"] > 0
            else 0,
            axis=1
        )
        
        # Sort by hour
        tem_vang_hour_df = tem_vang_hour_df.sort_values("Hour")
        
        return tem_vang_hour_df
        
    except Exception as e:
        st.error(f"❌ Lỗi tính toán TEM VÀNG theo giờ: {str(e)}")
        return pd.DataFrame()

# Function to map defect codes to defect names
def map_defect_codes_to_names(aql_df, aql_goi_df, aql_to_ly_df):
    """Map defect codes to proper defect names based on line number"""
    try:
        # Check if dataframes are empty
        if aql_df.empty:
            st.warning("⚠️ Không thể ánh xạ mã lỗi - thiếu dữ liệu")
            return pd.DataFrame()
        
        # Create a copy to avoid modifying the original
        df = aql_df.copy()
        
        # Create a Defect_Name column
        df["Defect_Name"] = ""
        
        # Get defect code column from AQL data
        defect_code_col = next((col for col in df.columns if "defect code" in col.lower()), None)
        actual_defect_col = next((col for col in df.columns if "actual defect" in col.lower()), None)
        
        if not defect_code_col:
            st.warning("⚠️ Không tìm thấy cột mã lỗi trong dữ liệu AQL")
            return df
        
        # If we already have actual defect column, use it directly
        if actual_defect_col:
            df["Defect_Name"] = df[actual_defect_col]
            return df
        
        # Get mapping columns from AQL gói and AQL Tô ly data
        if not aql_goi_df.empty:
            goi_code_col = aql_goi_df.columns[0]
            goi_name_col = aql_goi_df.columns[1]
            
            # Create a mapping dictionary for gói
            goi_mapping = dict(zip(aql_goi_df[goi_code_col], aql_goi_df[goi_name_col]))
        else:
            goi_mapping = {}
        
        if not aql_to_ly_df.empty:
            to_ly_code_col = aql_to_ly_df.columns[0]
            to_ly_name_col = aql_to_ly_df.columns[1]
            
            # Create a mapping dictionary for tô ly
            to_ly_mapping = dict(zip(aql_to_ly_df[to_ly_code_col], aql_to_ly_df[to_ly_name_col]))
        else:
            to_ly_mapping = {}
        
        # Function to map defect code to name based on line
        def map_defect_name(row):
            line = row.get("Line", "")
            defect_code = row.get(defect_code_col, "")
            
            # Convert line to string and check range
            try:
                line_str = str(line)
                if line_str in ["1", "2", "3", "4", "5", "6"]:
                    return goi_mapping.get(defect_code, defect_code)
                elif line_str in ["7", "8"]:
                    return to_ly_mapping.get(defect_code, defect_code)
                else:
                    return defect_code
            except:
                return defect_code
        
        # Apply the mapping function
        df["Defect_Name"] = df.apply(map_defect_name, axis=1)
        
        return df
        
    except Exception as e:
        st.error(f"❌ Lỗi ánh xạ mã lỗi: {str(e)}")
        return aql_df.copy()

# Function to analyze defect patterns (enhanced version)
def analyze_defect_patterns(aql_df_with_names):
    """Analyze defect patterns in AQL data using defect names instead of codes"""
    try:
        # Check if dataframe is empty
        if aql_df_with_names.empty:
            return {}
        
        # Create copy to avoid modifying original
        df = aql_df_with_names.copy()
        
        # Group by defect name to get frequency
        if "Defect_Name" in df.columns and df["Defect_Name"].nunique() > 0:
            defect_counts = df.groupby("Defect_Name").size().reset_index(name="Count")
            
            # Add additional metric: total hold quantity by defect
            if "Số lượng hold ( gói/thùng)" in df.columns:
                hold_by_defect = df.groupby("Defect_Name")["Số lượng hold ( gói/thùng)"].sum().reset_index(name="Hold_Quantity")
                defect_counts = pd.merge(defect_counts, hold_by_defect, on="Defect_Name", how="left")
            
            # Sort by count
            defect_counts = defect_counts.sort_values("Count", ascending=False)
            
            # Calculate percentages
            total_defects = defect_counts["Count"].sum()
            defect_counts["Percentage"] = (defect_counts["Count"] / total_defects * 100).round(1)
            defect_counts["Cumulative"] = defect_counts["Percentage"].cumsum()
            
            # Identify top defects (80% by Pareto principle)
            vital_few = defect_counts[defect_counts["Cumulative"] <= 80]
            
            # Group by Line and Defect name to get line-specific patterns
            line_defects = df.groupby(["Line", "Defect_Name"]).size().reset_index(name="Count")
            line_defects_hold = df.groupby(["Line", "Defect_Name"])["Số lượng hold ( gói/thùng)"].sum().reset_index(name="Hold_Quantity")
            line_defects = pd.merge(line_defects, line_defects_hold, on=["Line", "Defect_Name"], how="left")
            
            # Create a pivot table for heatmap visualization
            try:
                # First try with the default top defects
                top_n_defects = defect_counts.head(10)["Defect_Name"].tolist()
                line_defects_filtered = line_defects[line_defects["Defect_Name"].isin(top_n_defects)]
                pivot_line_defects = line_defects_filtered.pivot(index="Line", columns="Defect_Name", values="Count").fillna(0)
            except:
                # If that fails, try with all defects
                try:
                    pivot_line_defects = line_defects.pivot(index="Line", columns="Defect_Name", values="Count").fillna(0)
                except:
                    # If pivot fails, create an empty DataFrame with the right structure
                    pivot_line_defects = pd.DataFrame()
            
            # Group defects by shift to see shift-specific patterns
            if "Shift" in df.columns:
                shift_defects = df.groupby(["Shift", "Defect_Name"]).size().reset_index(name="Count")
                shift_defects_hold = df.groupby(["Shift", "Defect_Name"])["Số lượng hold ( gói/thùng)"].sum().reset_index(name="Hold_Quantity")
                shift_defects = pd.merge(shift_defects, shift_defects_hold, on=["Shift", "Defect_Name"], how="left")
                
                # Create a pivot table for shift defects
                try:
                    shift_defects_filtered = shift_defects[shift_defects["Defect_Name"].isin(top_n_defects)]
                    pivot_shift_defects = shift_defects_filtered.pivot(index="Shift", columns="Defect_Name", values="Count").fillna(0)
                except:
                    pivot_shift_defects = pd.DataFrame()
            else:
                shift_defects = pd.DataFrame()
                pivot_shift_defects = pd.DataFrame()
            
            # Find defect trends over time
            if "Production_Date" in df.columns:
                # Group by date and defect to see trends
                date_defects = df.groupby(["Production_Date", "Defect_Name"]).size().reset_index(name="Count")
                
                # For the top defects, create trends over time
                date_defect_trends = {}
                
                for defect in vital_few["Defect_Name"].tolist():
                    defect_trend = date_defects[date_defects["Defect_Name"] == defect]
                    if not defect_trend.empty:
                        date_defect_trends[defect] = defect_trend
            else:
                date_defect_trends = {}
            
            # Return the enhanced analysis results
            return {
                "defect_counts": defect_counts,
                "vital_few": vital_few,
                "line_defects": line_defects,
                "pivot_line_defects": pivot_line_defects,
                "shift_defects": shift_defects,
                "pivot_shift_defects": pivot_shift_defects,
                "date_defect_trends": date_defect_trends
            }
        else:
            # If we don't have defect names, try using defect codes
            defect_code_col = next((col for col in df.columns if "defect code" in col.lower()), None)
            
            if defect_code_col:
                st.warning(f"⚠️ Sử dụng mã lỗi thay vì tên lỗi cho phân tích Pareto")
                
                defect_counts = df.groupby(defect_code_col).size().reset_index(name="Count")
                
                # Add additional metric: total hold quantity by defect
                if "Số lượng hold ( gói/thùng)" in df.columns:
                    hold_by_defect = df.groupby(defect_code_col)["Số lượng hold ( gói/thùng)"].sum().reset_index(name="Hold_Quantity")
                    defect_counts = pd.merge(defect_counts, hold_by_defect, on=defect_code_col, how="left")
                
                # Sort by count
                defect_counts = defect_counts.sort_values("Count", ascending=False)
                
                # Calculate percentages
                total_defects = defect_counts["Count"].sum()
                defect_counts["Percentage"] = (defect_counts["Count"] / total_defects * 100).round(1)
                defect_counts["Cumulative"] = defect_counts["Percentage"].cumsum()
                
                # Identify top defects (80% by Pareto principle)
                vital_few = defect_counts[defect_counts["Cumulative"] <= 80]
                
                # Group by Line and Defect code to get line-specific patterns
                line_defects = df.groupby(["Line", defect_code_col]).size().reset_index(name="Count")
                line_defects_hold = df.groupby(["Line", defect_code_col])["Số lượng hold ( gói/thùng)"].sum().reset_index(name="Hold_Quantity")
                line_defects = pd.merge(line_defects, line_defects_hold, on=["Line", defect_code_col], how="left")
                
                try:
                    pivot_line_defects = line_defects.pivot(index="Line", columns=defect_code_col, values="Count").fillna(0)
                except:
                    pivot_line_defects = pd.DataFrame()
                
                # Rename columns for compatibility
                defect_counts.rename(columns={defect_code_col: "Defect_Name"}, inplace=True)
                
                # Return the analysis results
                return {
                    "defect_counts": defect_counts,
                    "vital_few": vital_few,
                    "line_defects": line_defects,
                    "pivot_line_defects": pivot_line_defects
                }
            else:
                st.warning("⚠️ Thiếu cột tên lỗi hoặc mã lỗi trong dữ liệu AQL để phân tích mẫu lỗi")
                return {}
            
    except Exception as e:
        st.error(f"❌ Lỗi phân tích mẫu lỗi: {str(e)}")
        return {}

# Function to identify critical issues
def identify_critical_issues(data):
    """Identify critical quality issues for senior management attention"""
    critical_issues = []
    
    try:
        # Check if we have TEM VÀNG data
        if not data["tem_vang_data"].empty:
            # Get most recent date
            recent_date = data["tem_vang_data"]["Date"].max()
            recent_data = data["tem_vang_data"][data["tem_vang_data"]["Date"] >= (recent_date - pd.Timedelta(days=7))]
            
            # Check for high TEM VÀNG values
            for line_group in [["1", "2", "3", "4", "5", "6"], ["7", "8"]]:
                line_target = 0.29 if line_group[0] in ["1", "2", "3", "4", "5", "6"] else 2.18
                
                line_data = recent_data[recent_data["Line"].isin(line_group)]
                if not line_data.empty:
                    avg_tem_vang = line_data["TEM_VANG"].mean()
                    if avg_tem_vang > line_target * 1.5:  # If 50% above target
                        critical_issues.append({
                            "type": "HIGH_TEM_VANG",
                            "description": f"TEM VÀNG cao trên Line {', '.join(line_group)}: {avg_tem_vang:.2f}% (vượt {(avg_tem_vang/line_target - 1)*100:.0f}% mục tiêu)",
                            "value": avg_tem_vang,
                            "target": line_target,
                            "lines": line_group,
                            "priority": "HIGH" if avg_tem_vang > line_target * 2 else "MEDIUM"
                        })

            # Check for rising trends in TEM VÀNG
            if len(data["tem_vang_data"]["Date"].unique()) >= 3:  # Need at least 3 data points
                # Group by date and calculate daily average
                daily_avg = data["tem_vang_data"].groupby("Date")["TEM_VANG"].mean().reset_index()
                daily_avg = daily_avg.sort_values("Date")
                
                if len(daily_avg) >= 3:
                    # Compare last 3 days
                    last_3_days = daily_avg.tail(3)
                    if last_3_days["TEM_VANG"].is_monotonic_increasing:
                        # Calculate increase percentage
                        first_value = last_3_days["TEM_VANG"].iloc[0]
                        last_value = last_3_days["TEM_VANG"].iloc[-1]
                        
                        if first_value > 0 and last_value > first_value * 1.2:  # 20% increase
                            critical_issues.append({
                                "type": "RISING_TEM_VANG",
                                "description": f"Xu hướng TEM VÀNG tăng liên tục: {first_value:.2f}% → {last_value:.2f}% (+{(last_value/first_value - 1)*100:.0f}%)",
                                "from_value": first_value,
                                "to_value": last_value,
                                "priority": "HIGH" if last_value > first_value * 1.5 else "MEDIUM"
                            })
        
        # Check for defect patterns
        if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
            defect_counts = data["defect_patterns"]["defect_counts"]
            
            # If there are dominant defects (one defect constitutes >40% of all defects)
            if not defect_counts.empty and defect_counts["Percentage"].max() > 40:
                top_defect = defect_counts.iloc[0]
                critical_issues.append({
                    "type": "DOMINANT_DEFECT",
                    "description": f"Lỗi '{top_defect['Defect_Name']}' chiếm tỷ lệ cao: {top_defect['Percentage']:.1f}% tổng số lỗi",
                    "defect": top_defect["Defect_Name"],
                    "percentage": top_defect["Percentage"],
                    "priority": "HIGH" if top_defect["Percentage"] > 60 else "MEDIUM"
                })
            
            # Check for defects by line
            if "pivot_line_defects" in data["defect_patterns"] and not data["defect_patterns"]["pivot_line_defects"].empty:
                pivot_df = data["defect_patterns"]["pivot_line_defects"]
                
                # Check each line for dominant defects
                for line in pivot_df.index:
                    line_data = pivot_df.loc[line]
                    if line_data.sum() > 0:
                        max_defect = line_data.idxmax()
                        max_pct = (line_data[max_defect] / line_data.sum()) * 100
                        
                        if max_pct > 50:  # If one defect is >50% for a specific line
                            critical_issues.append({
                                "type": "LINE_SPECIFIC_DEFECT",
                                "description": f"Line {line}: Lỗi '{max_defect}' chiếm {max_pct:.1f}% lỗi trên line này",
                                "line": line,
                                "defect": max_defect,
                                "percentage": max_pct,
                                "priority": "HIGH" if max_pct > 70 else "MEDIUM"
                            })
        
        # Check for shift-specific issues
        if not data["tem_vang_shift_df"].empty:
            shift_data = data["tem_vang_shift_df"]
            
            # Group by shift to see if there's a problematic shift
            shift_avg = shift_data.groupby("Shift")["TEM_VANG"].mean().reset_index()
            
            if not shift_avg.empty:
                max_shift = shift_avg.loc[shift_avg["TEM_VANG"].idxmax()]
                min_shift = shift_avg.loc[shift_avg["TEM_VANG"].idxmin()]
                
                if max_shift["TEM_VANG"] > min_shift["TEM_VANG"] * 2:  # At least 2x difference
                    critical_issues.append({
                        "type": "SHIFT_DISPARITY",
                        "description": f"Ca {max_shift['Shift']} có TEM VÀNG cao ({max_shift['TEM_VANG']:.2f}%), {max_shift['TEM_VANG']/min_shift['TEM_VANG']:.1f}x cao hơn Ca {min_shift['Shift']}",
                        "high_shift": max_shift["Shift"],
                        "low_shift": min_shift["Shift"],
                        "high_value": max_shift["TEM_VANG"],
                        "low_value": min_shift["TEM_VANG"],
                        "priority": "HIGH" if max_shift["TEM_VANG"] > min_shift["TEM_VANG"] * 3 else "MEDIUM"
                    })
    
    except Exception as e:
        st.error(f"Lỗi phân tích vấn đề quan trọng: {str(e)}")
    
    return critical_issues

# Function to generate recommendations
def generate_recommendations(data, critical_issues):
    """Generate actionable recommendations based on data analysis"""
    recommendations = []
    
    try:
        # General recommendations
        if "defect_patterns" in data and "vital_few" in data["defect_patterns"]:
            vital_few = data["defect_patterns"]["vital_few"]
            if not vital_few.empty:
                recommendations.append({
                    "type": "FOCUS_IMPROVEMENT",
                    "title": "Tập trung cải tiến vào các lỗi chính",
                    "description": f"Tập trung nỗ lực cải tiến vào {len(vital_few)} loại lỗi chính: {', '.join(vital_few['Defect_Name'].head(3).tolist())}..., sẽ giải quyết 80% vấn đề chất lượng.",
                    "priority": "HIGH"
                })
        
        # Recommendations based on critical issues
        for issue in critical_issues:
            if issue["type"] == "HIGH_TEM_VANG":
                # Get line-specific recommendation
                is_cup_line = "7" in issue["lines"] or "8" in issue["lines"]
                line_desc = "Tô ly" if is_cup_line else "Gói"
                
                recommendations.append({
                    "type": "REDUCE_TEM_VANG",
                    "title": f"Giảm TEM VÀNG trên Line {', '.join(issue['lines'])}",
                    "description": f"Thực hiện đánh giá quy trình chi tiết trên Line {', '.join(issue['lines'])} ({line_desc}). Kiểm tra thiết bị, tiêu chuẩn vận hành, và kiểm soát chất lượng nguyên liệu đầu vào.",
                    "priority": issue["priority"]
                })
            
            elif issue["type"] == "RISING_TEM_VANG":
                recommendations.append({
                    "type": "INVESTIGATE_TREND",
                    "title": "Điều tra xu hướng tăng TEM VÀNG",
                    "description": "Phân tích lý do TEM VÀNG tăng liên tục trong những ngày gần đây. Kiểm tra thay đổi gần đây về nguyên liệu, thiết bị, hoặc quy trình.",
                    "priority": issue["priority"]
                })
            
            elif issue["type"] == "DOMINANT_DEFECT":
                recommendations.append({
                    "type": "ADDRESS_TOP_DEFECT",
                    "title": f"Khắc phục lỗi '{issue['defect']}'",
                    "description": f"Thực hiện nghiên cứu chuyên sâu về lỗi '{issue['defect']}' (chiếm {issue['percentage']:.1f}%). Phân tích nguyên nhân gốc rễ và thực hiện các hành động khắc phục ưu tiên.",
                    "priority": issue["priority"]
                })
            
            elif issue["type"] == "LINE_SPECIFIC_DEFECT":
                is_cup_line = issue["line"] in ["7", "8"]
                line_desc = "Tô ly" if is_cup_line else "Gói"
                
                recommendations.append({
                    "type": "LINE_SPECIFIC_ACTION",
                    "title": f"Cải thiện Line {issue['line']} - Lỗi '{issue['defect']}'",
                    "description": f"Triển khai đội cải tiến tập trung vào Line {issue['line']} ({line_desc}) để giảm lỗi '{issue['defect']}'. Đánh giá thiết lập thiết bị, quy trình và đào tạo vận hành.",
                    "priority": issue["priority"]
                })
            
            elif issue["type"] == "SHIFT_DISPARITY":
                recommendations.append({
                    "type": "SHIFT_STANDARDIZATION",
                    "title": f"Tiêu chuẩn hóa vận hành giữa các ca",
                    "description": f"Phân tích sự khác biệt giữa Ca {issue['low_shift']} (tốt) và Ca {issue['high_shift']} (kém). Chuyển giao các thực hành tốt và tăng cường đào tạo để đảm bảo tính nhất quán.",
                    "priority": issue["priority"]
                })
        
        # Add general recommendations
        if not data["tem_vang_leader_df"].empty:
            recommendations.append({
                "type": "OPERATOR_TRAINING",
                "title": "Đào tạo vận hành dựa trên hiệu suất của trưởng ca",
                "description": "Tổ chức đào tạo chéo và chia sẻ kinh nghiệm giữa các trưởng ca để đảm bảo áp dụng các phương pháp tốt nhất trên tất cả các ca.",
                "priority": "MEDIUM"
            })
        
        if len(recommendations) == 0:
            # Default recommendation if none generated
            recommendations.append({
                "type": "DEFAULT",
                "title": "Duy trì giám sát chất lượng",
                "description": "Tiếp tục giám sát các chỉ số chất lượng và thực hiện đánh giá định kỳ các quy trình kiểm soát chất lượng trên tất cả các line.",
                "priority": "MEDIUM"
            })
    
    except Exception as e:
        st.error(f"Lỗi tạo khuyến nghị: {str(e)}")
        # Add a failsafe recommendation
        recommendations.append({
            "type": "FALLBACK",
            "title": "Tiếp tục giám sát chất lượng",
            "description": "Duy trì quy trình kiểm soát chất lượng hiện tại và đánh giá dữ liệu khi có thêm thông tin.",
            "priority": "MEDIUM"
        })
    
    return recommendations

# Load all data needed
@st.cache_data(ttl=600)  # Cache the combined data for 10 minutes
def load_all_data():
    """Load and prepare all required data"""
    
    # Initialize an empty result dictionary with all required keys
    result = {
        "aql_data": pd.DataFrame(),
        "aql_data_with_names": pd.DataFrame(),
        "production_data": pd.DataFrame(),
        "tem_vang_data": pd.DataFrame(),
        "tem_vang_shift_df": pd.DataFrame(),
        "tem_vang_leader_df": pd.DataFrame(),
        "tem_vang_hour_data": pd.DataFrame(),
        "defect_patterns": {}
    }
    
    # Load raw data
    aql_df = load_aql_data()
    production_df = load_production_data()
    aql_goi_df = load_aql_goi_data()  
    aql_to_ly_df = load_aql_to_ly_data()
    
    # Update the result dictionary with the loaded data
    result["aql_data"] = aql_df
    result["production_data"] = production_df
    
    # Only proceed with further processing if we have the necessary data
    if not aql_df.empty and not production_df.empty:
        # Map defect codes to names
        aql_df_with_names = map_defect_codes_to_names(aql_df, aql_goi_df, aql_to_ly_df)
        result["aql_data_with_names"] = aql_df_with_names
        
        # Calculate TEM VÀNG metrics directly to result dictionary
        result["tem_vang_data"] = calculate_tem_vang(aql_df, production_df)
        result["tem_vang_shift_df"] = calculate_tem_vang_by_shift(aql_df, production_df)
        result["tem_vang_leader_df"] = calculate_tem_vang_by_leader(aql_df, production_df)
        result["tem_vang_hour_data"] = calculate_tem_vang_by_hour(aql_df, production_df)
        
        # Analyze defect patterns with names
        result["defect_patterns"] = analyze_defect_patterns(aql_df_with_names)
    
    return result

# Title and description
st.markdown('<div class="main-header">Báo cáo chất lượng CF MMB</div>', unsafe_allow_html=True)

# Load all data
data = load_all_data()

# Check if key dataframes are empty
if data["aql_data"].empty or data["production_data"].empty:
    st.warning("⚠️ Thiếu dữ liệu cần thiết. Vui lòng kiểm tra kết nối Google Sheet.")
    # Still continue rendering with available data

# Create a sidebar for filters
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #0d2c54;'>Bộ lọc</h2>", unsafe_allow_html=True)
    
    # Initialize filtered dataframes
    filtered_aql_df = data["aql_data_with_names"].copy()
    filtered_tem_vang_df = data["tem_vang_data"].copy()
    filtered_tem_vang_shift_df = data["tem_vang_shift_df"].copy()
    filtered_tem_vang_leader_df = data["tem_vang_leader_df"].copy()
    filtered_tem_vang_hour_df = data["tem_vang_hour_data"].copy()
    
    # Date filter for production data
    st.subheader("Khoảng thời gian phân tích")
    
    # Get min and max dates from AQL data
    if not data["aql_data"].empty and "Production_Date" in data["aql_data"].columns:
        min_prod_date = data["aql_data"]["Production_Date"].min().date()
        max_prod_date = data["aql_data"]["Production_Date"].max().date()
    else:
        min_prod_date = datetime.now().date() - timedelta(days=365)
        max_prod_date = datetime.now().date()
    
    # Create date range selector for production data
    prod_date_range = st.date_input(
        "Chọn khoảng thời gian sản xuất",
        value=(max_prod_date - timedelta(days=30), max_prod_date),
        min_value=min_prod_date,
        max_value=max_prod_date + timedelta(days=1)
    )
    
    # Apply production date filter if a range is selected
    if len(prod_date_range) == 2:
        start_date, end_date = prod_date_range
        
        # Convert to datetime for filtering
        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        
        # Apply to AQL data
        if not filtered_aql_df.empty and "Production_Date" in filtered_aql_df.columns:
            filtered_aql_df = filtered_aql_df[
                (filtered_aql_df["Production_Date"] >= start_datetime) & 
                (filtered_aql_df["Production_Date"] <= end_datetime)
            ]
        
        # Apply to TEM VÀNG data
        if not filtered_tem_vang_df.empty and "Date" in filtered_tem_vang_df.columns:
            filtered_tem_vang_df = filtered_tem_vang_df[
                (filtered_tem_vang_df["Date"] >= start_datetime) & 
                (filtered_tem_vang_df["Date"] <= end_datetime)
            ]
            
        # Apply to TEM VÀNG by shift data
        if not filtered_tem_vang_shift_df.empty and "Date" in filtered_tem_vang_shift_df.columns:
            filtered_tem_vang_shift_df = filtered_tem_vang_shift_df[
                (filtered_tem_vang_shift_df["Date"] >= start_datetime) & 
                (filtered_tem_vang_shift_df["Date"] <= end_datetime)
            ]
            
        # Apply to TEM VÀNG by leader data
        if not filtered_tem_vang_leader_df.empty and "Date" in filtered_tem_vang_leader_df.columns:
            filtered_tem_vang_leader_df = filtered_tem_vang_leader_df[
                (filtered_tem_vang_leader_df["Date"] >= start_datetime) & 
                (filtered_tem_vang_leader_df["Date"] <= end_datetime)
            ]
    
    # Line filter - Always include all lines from 1 to 8 regardless of data
    all_lines = ["Tất cả"] + [str(i) for i in range(1, 9)]
    selected_line = st.selectbox("🏭 Chọn Line sản xuất", all_lines)
    
    if selected_line != "Tất cả":
        # Apply filter to dataframes if the line exists in them
        if not filtered_tem_vang_df.empty and "Line" in filtered_tem_vang_df.columns:
            filtered_tem_vang_df = filtered_tem_vang_df[filtered_tem_vang_df["Line"] == selected_line]
        
        if not filtered_aql_df.empty and "Line" in filtered_aql_df.columns:
            filtered_aql_df = filtered_aql_df[filtered_aql_df["Line"] == selected_line]
            
        if not filtered_tem_vang_shift_df.empty and "Line" in filtered_tem_vang_shift_df.columns:
            filtered_tem_vang_shift_df = filtered_tem_vang_shift_df[filtered_tem_vang_shift_df["Line"] == selected_line]
            
        if not filtered_tem_vang_leader_df.empty and "Line" in filtered_tem_vang_leader_df.columns:
            filtered_tem_vang_leader_df = filtered_tem_vang_leader_df[filtered_tem_vang_leader_df["Line"] == selected_line]
    
    # Shift filter
    all_shifts = ["Tất cả", "1", "2", "3"]
    selected_shift = st.selectbox("⏱️ Chọn Ca", all_shifts)
    
    if selected_shift != "Tất cả":
        # Apply filter to shift-related dataframes
        if not filtered_tem_vang_shift_df.empty and "Shift" in filtered_tem_vang_shift_df.columns:
            filtered_tem_vang_shift_df = filtered_tem_vang_shift_df[filtered_tem_vang_shift_df["Shift"] == selected_shift]
        
        if not filtered_aql_df.empty and "Shift" in filtered_aql_df.columns:
            filtered_aql_df = filtered_aql_df[filtered_aql_df["Shift"] == selected_shift]
    
    # Identify critical issues
    critical_issues = identify_critical_issues(data)
    
    # Generate recommendations
    recommendations = generate_recommendations(data, critical_issues)
    
    # Refresh button
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    # Show last update time
    st.markdown(f"**Cập nhật cuối:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Add auto-refresh option with improved styling
    st.markdown("""
    <div style="display: flex; align-items: center; margin-top: 10px;">
        <span style="margin-right: 10px;">⏱️ Tự động làm mới (5 phút)</span>
        <label class="toggle-switch">
            <input type="checkbox" id="auto-refresh">
            <span class="slider"></span>
        </label>
    </div>
    """, unsafe_allow_html=True)
    
    auto_refresh = st.checkbox("Auto-refresh", value=False, label_visibility="collapsed")

# Create tabs for better organization
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Tổng quan", 
    "📈 Phân tích TEM VÀNG", 
    "👥 Phân tích theo Ca", 
    "🕒 Phân tích theo Giờ",
    "⚠️ Phân tích Lỗi"
])

with tab1:
    # Overview tab
    st.markdown('<div class="sub-header">Tổng quan chất lượng sản xuất</div>', unsafe_allow_html=True)
    
    # Display critical issues at the top (if any)
    if critical_issues:
        st.markdown("### 🚨 Vấn đề cần chú ý")
        
        issues_container = st.container()
        with issues_container:
            for issue in critical_issues:
                st.markdown(f"""
                <div class="warning-card">
                    <div class="warning-title">{issue['description']}</div>
                </div>
                """, unsafe_allow_html=True)
    
    # Display recommendations
    if recommendations:
        st.markdown("### 📋 Khuyến nghị hành động")
        
        for rec in recommendations:
            priority_color = "#ef4444" if rec["priority"] == "HIGH" else "#f59e0b"
            st.markdown(f"""
            <div class="recommendation-card">
                <div class="recommendation-title">
                    {rec['title']} 
                    <span style="color: {priority_color}; float: right; font-size: 0.8rem;">
                        {rec['priority']}
                    </span>
                </div>
                <div class="insight-content">{rec['description']}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # Key metrics row
    st.markdown("### 📊 Chỉ số chất lượng chính")
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)

    with metrics_col1:
        if not filtered_tem_vang_df.empty:
            avg_tem_vang = filtered_tem_vang_df["TEM_VANG"].mean()
            
            # Target TEM VÀNG now depends on line selection
            if selected_line in ["7", "8"]:
                tem_target = 2.18
            elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                tem_target = 0.29
            else:
                tem_target = 0.41  # Total/all lines target
                
            tem_delta = avg_tem_vang - tem_target
            
            # Add trend information
            tem_trend = ""
            if len(filtered_tem_vang_df["Date"].unique()) >= 7:
                # Check last 7 days trend
                weekly_data = filtered_tem_vang_df.sort_values("Date")
                weekly_avg = weekly_data.groupby("Date")["TEM_VANG"].mean().reset_index()
                
                if len(weekly_avg) >= 3:
                    start_val = weekly_avg["TEM_VANG"].iloc[0]
                    end_val = weekly_avg["TEM_VANG"].iloc[-1]
                    
                    if end_val > start_val * 1.1:  # 10% increase
                        tem_trend = '<span class="trend-indicator trend-up">▲</span>'
                    elif end_val < start_val * 0.9:  # 10% decrease
                        tem_trend = '<span class="trend-indicator trend-down">▼</span>'
                    else:
                        tem_trend = '<span class="trend-indicator trend-stable">◆</span>'
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">TEM VÀNG trung bình</div>
                <div class="metric-value">{avg_tem_vang:.2f}% {tem_trend}</div>
                <div class="{'metric-negative' if tem_delta > 0 else 'metric-positive'}">
                    {f"{tem_delta:.2f}% {'cao hơn' if tem_delta > 0 else 'thấp hơn'} mục tiêu"}
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">TEM VÀNG trung bình</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)

    with metrics_col2:
        if not filtered_tem_vang_df.empty:
            total_hold = filtered_tem_vang_df["Hold_Quantity"].sum()
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Tổng số lượng hold</div>
                <div class="metric-value">{total_hold:,.0f}</div>
                <div style="color: #64748b; font-size: 0.9rem;">Số lượng sản phẩm bị giữ lại</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Tổng số lượng hold</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)

    with metrics_col3:
        if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
            defect_types = len(data["defect_patterns"]["defect_counts"])
            
            # Find the top defect
            top_defect = data["defect_patterns"]["defect_counts"].iloc[0] if not data["defect_patterns"]["defect_counts"].empty else None
            top_defect_info = ""
            if top_defect is not None:
                top_defect_info = f"<div style='color: #64748b; font-size: 0.9rem;'>Lỗi chính: {top_defect['Defect_Name']} ({top_defect['Percentage']:.1f}%)</div>"
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Số loại lỗi</div>
                <div class="metric-value">{defect_types}</div>
                {top_defect_info}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Số loại lỗi</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)
            
    with metrics_col4:
        if not filtered_tem_vang_df.empty:
            total_production = filtered_tem_vang_df["Production_Volume"].sum()
            
            # Calculate quality rate
            quality_rate = 100 - (filtered_tem_vang_df["Hold_Quantity"].sum() / total_production * 100)
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Tỷ lệ chất lượng</div>
                <div class="metric-value">{quality_rate:.2f}%</div>
                <div style="color: #64748b; font-size: 0.9rem;">Tổng SL: {total_production:,.0f} sản phẩm</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Tỷ lệ chất lượng</div>
                <div class="metric-value">N/A</div>
            </div>
            """, unsafe_allow_html=True)
    
    # Main charts for overview
    col1, col2 = st.columns(2)
    
    with col1:
        # TEM VÀNG trend over time with improved styling
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
                    line=dict(color="#0d2c54", width=2),
                    marker=dict(size=6, color="#0d2c54")
                ))
                
                # Set the appropriate target based on line selection
                if selected_line in ["7", "8"]:
                    target_value = 2.18
                    target_label = "Mục tiêu Line 7-8 (2.18%)"
                    target_color = "#ef4444"
                elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                    target_value = 0.29
                    target_label = "Mục tiêu Line 1-6 (0.29%)"
                    target_color = "#10b981"
                else:
                    target_value = 0.41
                    target_label = "Mục tiêu tổng (0.41%)"
                    target_color = "#3b82f6"
                
                # Add target line
                fig.add_hline(
                    y=target_value,
                    line_dash="dash",
                    line_color=target_color,
                    annotation=dict(
                        text=target_label,
                        font=dict(color=target_color),
                        xref="paper",
                        x=0.02,
                        yref="y",
                        y=target_value + (daily_tem_vang["TEM_VANG"].max() - daily_tem_vang["TEM_VANG"].min()) * 0.1
                    )
                )
                
                # Format dates on x-axis
                fig.update_xaxes(
                    tickformat="%d/%m/%Y",
                    tickangle=-45,
                    tickmode="auto",
                    nticks=10
                )
                
                # Update layout
                fig.update_layout(
                    title="Xu hướng TEM VÀNG theo thời gian",
                    xaxis_title="Ngày",
                    yaxis_title="TEM VÀNG (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=60),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333"),
                    hovermode="x unified",
                    hoverlabel=dict(
                        bgcolor="white",
                        font_size=12,
                        font_family="Arial"
                    )
                )
                
                # Add grid lines for better readability
                fig.update_xaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0"
                )
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add insight based on trend analysis
                if len(daily_tem_vang) >= 3:
                    last_value = daily_tem_vang["TEM_VANG"].iloc[-1]
                    avg_value = daily_tem_vang["TEM_VANG"].mean()
                    vs_avg = (last_value / avg_value - 1) * 100
                    
                    trend_direction = "tăng" if vs_avg > 0 else "giảm"
                    trend_color = "#ef4444" if vs_avg > 0 else "#10b981"
                    
                    st.markdown(f"""
                    <div class="insight-card">
                        <div class="insight-title">Phân tích xu hướng</div>
                        <div class="insight-content">
                            TEM VÀNG hiện tại đang <span style="color: {trend_color}; font-weight: bold;">{trend_direction} {abs(vs_avg):.1f}%</span> so với trung bình trong khoảng thời gian đã chọn.
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Lỗi tạo biểu đồ xu hướng TEM VÀNG: {str(e)}")

    with col2:
        # TEM VÀNG by line with improved styling
        if not filtered_tem_vang_df.empty:
            try:
                # Group by line to get average TEM VÀNG per line
                line_tem_vang = filtered_tem_vang_df.groupby("Line")[["TEM_VANG", "Hold_Quantity"]].mean().reset_index()
                
                # Add a target column based on line
                line_tem_vang["Target"] = line_tem_vang["Line"].apply(
                    lambda x: 2.18 if x in ["7", "8"] else 0.29
                )
                
                # Calculate variance from target
                line_tem_vang["Variance"] = line_tem_vang["TEM_VANG"] - line_tem_vang["Target"]
                
                # Sort by Line number
                line_tem_vang = line_tem_vang.sort_values("Line")
                
                # Create color array based on performance
                colors = []
                for _, row in line_tem_vang.iterrows():
                    if row["TEM_VANG"] <= row["Target"]:
                        colors.append("#10b981")  # Good - green
                    elif row["TEM_VANG"] <= row["Target"] * 1.2:
                        colors.append("#f59e0b")  # Warning - amber
                    else:
                        colors.append("#ef4444")  # Bad - red
                
                # Create figure
                fig = go.Figure()
                
                # Add TEM VÀNG bars
                fig.add_trace(go.Bar(
                    x=line_tem_vang["Line"],
                    y=line_tem_vang["TEM_VANG"],
                    name="TEM VÀNG",
                    marker_color=colors,
                    text=line_tem_vang["TEM_VANG"].round(2).astype(str) + "%",
                    textposition="auto",
                    hovertemplate="<b>Line %{x}</b><br>TEM VÀNG: %{y:.2f}%<br>Target: %{customdata:.2f}%<extra></extra>",
                    customdata=line_tem_vang["Target"]
                ))
                
                # Add target markers
                fig.add_trace(go.Scatter(
                    x=line_tem_vang["Line"],
                    y=line_tem_vang["Target"],
                    mode="markers",
                    marker=dict(
                        symbol="diamond",
                        size=10,
                        color="#0d2c54",
                        line=dict(width=2, color="white")
                    ),
                    name="Target",
                    hovertemplate="<b>Line %{x}</b><br>Target: %{y:.2f}%<extra></extra>"
                ))
                
                # Update layout
                fig.update_layout(
                    title="TEM VÀNG theo Line sản xuất",
                    xaxis_title="Line",
                    yaxis_title="TEM VÀNG (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=40),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333"),
                    hovermode="closest",
                    hoverlabel=dict(
                        bgcolor="white",
                        font_size=12,
                        font_family="Arial"
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    xaxis=dict(
                        tickmode='array',
                        tickvals=list(range(1, 9)),
                        ticktext=[str(i) for i in range(1, 9)]
                    )
                )
                
                # Add grid lines for better readability
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add insight about worst performing line
                if not line_tem_vang.empty:
                    worst_line = line_tem_vang.loc[line_tem_vang["Variance"].idxmax()]
                    best_line = line_tem_vang.loc[line_tem_vang["Variance"].idxmin()]
                    
                    if worst_line["Variance"] > 0:
                        st.markdown(f"""
                        <div class="insight-card">
                            <div class="insight-title">Line cần cải thiện</div>
                            <div class="insight-content">
                                Line <span class="line-header">{worst_line["Line"]}</span> có TEM VÀNG cao nhất, vượt 
                                <span style="color: #ef4444; font-weight: bold;">{(worst_line["Variance"] / worst_line["Target"] * 100):.1f}%</span> so với mục tiêu.
                                Nên ưu tiên triển khai cải tiến cho line này.
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Lỗi tạo biểu đồ TEM VÀNG theo line: {str(e)}")
    
    # Add Pareto chart in the overview for top defects
    if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
        try:
            defect_counts = data["defect_patterns"]["defect_counts"]
            
            # Take only top 10 defects for cleaner visualization
            top_defects = defect_counts.head(10)
            
            # Create Pareto chart
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add bars for defect counts
            fig.add_trace(
                go.Bar(
                    x=top_defects["Defect_Name"],
                    y=top_defects["Count"],
                    name="Số lần xuất hiện",
                    marker_color="#0d2c54",
                    text=top_defects["Count"].astype(int),
                    textposition="auto"
                ),
                secondary_y=False
            )
            
            # Add line for cumulative percentage
            fig.add_trace(
                go.Scatter(
                    x=top_defects["Defect_Name"],
                    y=top_defects["Cumulative"],
                    name="Tích lũy %",
                    mode="lines+markers",
                    marker=dict(color="#ef4444", size=8),
                    line=dict(color="#ef4444", width=3)
                ),
                secondary_y=True
            )
            
            # Add 80% reference line
            fig.add_hline(
                y=80,
                line_dash="dash",
                line_color="#10b981",
                annotation=dict(
                    text="80% ngưỡng",
                    font=dict(color="#10b981"),
                    xref="paper",
                    x=1,
                    yref="y2",
                    y=80
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title="Top 10 lỗi chất lượng (Biểu đồ Pareto)",
                xaxis_title="Loại lỗi",
                height=400,
                margin=dict(l=40, r=40, t=60, b=100),
                xaxis_tickangle=-45,
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="#333333"),
                hovermode="x unified",
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=12,
                    font_family="Arial"
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Số lần xuất hiện", secondary_y=False)
            fig.update_yaxes(title_text="Tích lũy %", secondary_y=True)
            
            # Add grid lines for better readability
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0",
                secondary_y=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ Pareto: {str(e)}")

with tab2:
    # TEM VÀNG Analysis tab
    st.markdown('<div class="sub-header">Phân tích chi tiết TEM VÀNG</div>', unsafe_allow_html=True)
    
    # Monthly trend analysis
    st.markdown("### 📅 Phân tích xu hướng theo tháng")
    
    if not filtered_tem_vang_df.empty:
        try:
            # Create monthly aggregation
            filtered_tem_vang_df["Month"] = filtered_tem_vang_df["Date"].dt.strftime("%m/%Y")
            monthly_data = filtered_tem_vang_df.groupby("Month").agg({
                "TEM_VANG": "mean",
                "Hold_Quantity": "sum",
                "Production_Volume": "sum",
                "Date": "min"  # Get first date for sorting
            }).reset_index()
            
            # Sort by actual date
            monthly_data = monthly_data.sort_values("Date")
            
            # Create figure
            fig = go.Figure()
            
            # Add TEM VÀNG line
            fig.add_trace(go.Scatter(
                x=monthly_data["Month"],
                y=monthly_data["TEM_VANG"],
                mode="lines+markers",
                name="TEM VÀNG",
                line=dict(color="#0d2c54", width=3),
                marker=dict(size=10, color="#0d2c54")
            ))
            
            # Add target line
            if selected_line in ["7", "8"]:
                target_value = 2.18
            elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                target_value = 0.29
            else:
                target_value = 0.41
                
            fig.add_hline(
                y=target_value,
                line_dash="dash",
                line_color="#ef4444",
                line_width=2,
                annotation=dict(
                    text=f"Mục tiêu: {target_value}%",
                    font=dict(color="#ef4444"),
                    xref="paper",
                    x=0,
                    yref="y",
                    y=target_value
                )
            )
            
            # Update layout
            fig.update_layout(
                title="Xu hướng TEM VÀNG theo tháng",
                xaxis_title="Tháng",
                yaxis_title="TEM VÀNG (%)",
                height=400,
                margin=dict(l=40, r=40, t=60, b=40),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="#333333"),
                hovermode="x unified",
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=12,
                    font_family="Arial"
                )
            )
            
            # Add grid lines for better readability
            fig.update_xaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0"
            )
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add comparison to previous period
            if len(monthly_data) >= 2:
                current_month = monthly_data.iloc[-1]
                previous_month = monthly_data.iloc[-2]
                
                pct_change = ((current_month["TEM_VANG"] / previous_month["TEM_VANG"]) - 1) * 100
                direction = "tăng" if pct_change > 0 else "giảm"
                color = "#ef4444" if pct_change > 0 else "#10b981"
                
                st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">So sánh kỳ trước</div>
                    <div class="insight-content">
                        TEM VÀNG tháng {current_month["Month"]} đã <span style="color: {color}; font-weight: bold;">{direction} {abs(pct_change):.1f}%</span> 
                        so với tháng {previous_month["Month"]} (từ {previous_month["TEM_VANG"]:.2f}% sang {current_month["TEM_VANG"]:.2f}%).
                    </div>
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Lỗi phân tích xu hướng tháng: {str(e)}")
    else:
        st.warning("⚠️ Không có đủ dữ liệu để phân tích xu hướng tháng")
    
    # Line-specific analysis
    st.markdown("### 🏭 Phân tích theo Line")
    
    if not filtered_tem_vang_df.empty:
        try:
            # Create two columns for side-by-side charts
            line_col1, line_col2 = st.columns(2)
            
            with line_col1:
                # Line vs Target
                line_performance = filtered_tem_vang_df.groupby("Line").agg({
                    "TEM_VANG": "mean",
                    "Hold_Quantity": "sum",
                    "Production_Volume": "sum"
                }).reset_index()
                
                # Add target column
                line_performance["Target"] = line_performance["Line"].apply(
                    lambda x: 2.18 if x in ["7", "8"] else 0.29
                )
                
                # Add performance metric
                line_performance["Performance"] = (line_performance["Target"] / line_performance["TEM_VANG"]) * 100
                line_performance["TargetDiff"] = line_performance["TEM_VANG"] - line_performance["Target"]
                
                # Sort by performance
                line_performance = line_performance.sort_values("Performance", ascending=False)
                
                # Create color array based on performance
                colors = []
                for perf in line_performance["Performance"]:
                    if perf >= 100:
                        colors.append("#10b981")  # Good - green
                    elif perf >= 80:
                        colors.append("#f59e0b")  # Warning - amber
                    else:
                        colors.append("#ef4444")  # Bad - red
                
                # Create figure for performance vs target
                fig = go.Figure()
                
                # Add performance bars
                fig.add_trace(go.Bar(
                    x=line_performance["Line"],
                    y=line_performance["Performance"],
                    marker_color=colors,
                    text=line_performance["Performance"].round(1).astype(str) + "%",
                    textposition="auto",
                    hovertemplate="<b>Line %{x}</b><br>Hiệu suất: %{y:.1f}%<br>TEM VÀNG: %{customdata:.2f}%<extra></extra>",
                    customdata=line_performance["TEM_VANG"]
                ))
                
                # Add 100% line
                fig.add_hline(
                    y=100,
                    line_dash="dash",
                    line_color="#0d2c54",
                    line_width=2,
                    annotation=dict(
                        text="100% (Đạt mục tiêu)",
                        font=dict(color="#0d2c54"),
                        xref="paper",
                        x=1,
                        yref="y",
                        y=100
                    )
                )
                
                # Update layout
                fig.update_layout(
                    title="Hiệu suất Line so với mục tiêu",
                    xaxis_title="Line",
                    yaxis_title="Hiệu suất (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=40),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333")
                )
                
                # Add grid lines for better readability
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0"
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with line_col2:
                # Hold quantity by line
                line_hold = filtered_tem_vang_df.groupby("Line").agg({
                    "Hold_Quantity": "sum",
                    "Production_Volume": "sum"
                }).reset_index()
                
                # Calculate percentage held
                line_hold["Hold_Percentage"] = (line_hold["Hold_Quantity"] / line_hold["Production_Volume"]) * 100
                
                # Sort by hold quantity
                line_hold = line_hold.sort_values("Hold_Quantity", ascending=False)
                
                # Create figure for hold quantity
                fig = go.Figure()
                
                # Add hold quantity bars
                fig.add_trace(go.Bar(
                    x=line_hold["Line"],
                    y=line_hold["Hold_Quantity"],
                    marker_color="#3b82f6",
                    name="Số lượng hold",
                    text=line_hold["Hold_Quantity"].astype(int),
                    textposition="auto",
                    hovertemplate="<b>Line %{x}</b><br>Hold: %{y:,.0f}<br>Tỷ lệ: %{customdata:.2f}%<extra></extra>",
                    customdata=line_hold["Hold_Percentage"]
                ))
                
                # Add hold percentage line
                fig.add_trace(go.Scatter(
                    x=line_hold["Line"],
                    y=line_hold["Hold_Percentage"],
                    mode="lines+markers",
                    marker=dict(color="#ef4444", size=8),
                    line=dict(color="#ef4444", width=2),
                    name="% bị hold",
                    yaxis="y2",
                    hovertemplate="<b>Line %{x}</b><br>Tỷ lệ hold: %{y:.2f}%<extra></extra>"
                ))
                
                # Update layout
                fig.update_layout(
                    title="Số lượng bị hold theo Line",
                    xaxis_title="Line",
                    yaxis_title="Số lượng bị hold",
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=40),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333"),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    yaxis2=dict(
                        title="Tỷ lệ hold (%)",
                        overlaying="y",
                        side="right",
                        showgrid=False
                    )
                )
                
                # Add grid lines for better readability
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0",
                    secondary_y=False
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Line trend over time
            st.markdown("### 📈 Xu hướng TEM VÀNG theo Line")
            
            # Create line trend visualization
            line_trend_df = filtered_tem_vang_df.copy()
            line_trend_df["Week"] = line_trend_df["Date"].dt.strftime("%Y-%U")
            
            # Group by week and line
            weekly_line_data = line_trend_df.groupby(["Week", "Line"])["TEM_VANG"].mean().reset_index()
            
            # Create figure for line trend
            fig = px.line(
                weekly_line_data,
                x="Week",
                y="TEM_VANG",
                color="Line",
                markers=True,
                labels={"TEM_VANG": "TEM VÀNG (%)", "Week": "Tuần"},
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            
            # Update layout
            fig.update_layout(
                title="Xu hướng TEM VÀNG theo Line qua các tuần",
                height=450,
                margin=dict(l=40, r=40, t=60, b=60),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="#333333"),
                hovermode="closest",
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=12,
                    font_family="Arial"
                ),
                legend_title="Line"
            )
            
            # Add grid lines for better readability
            fig.update_xaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0",
                tickangle=-45
            )
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add insight about line trend
            highest_line = weekly_line_data.groupby("Line")["TEM_VANG"].mean().idxmax()
            most_improved = weekly_line_data.pivot(index="Week", columns="Line", values="TEM_VANG").fillna(method="ffill")
            
            if len(most_improved) >= 2:
                first_week = most_improved.iloc[0]
                last_week = most_improved.iloc[-1]
                improvements = ((last_week - first_week) / first_week) * -100  # Negative means improvement
                
                best_improved_line = improvements.idxmax()
                worst_trend_line = improvements.idxmin()
                
                if not pd.isna(best_improved_line) and not pd.isna(worst_trend_line):
                    st.markdown(f"""
                    <div class="insight-card">
                        <div class="insight-title">Phân tích xu hướng theo Line</div>
                        <div class="insight-content">
                            <p>Line <span class="badge badge-good">{best_improved_line}</span> có cải thiện tốt nhất với 
                            TEM VÀNG giảm <span style="color: #10b981; font-weight: bold;">{improvements[best_improved_line]:.1f}%</span></p>
                            
                            <p>Line <span class="badge badge-bad">{worst_trend_line}</span> có xu hướng kém nhất với 
                            TEM VÀNG {improvements[worst_trend_line] < 0 ? "tăng" : "giảm"} 
                            <span style="color: {improvements[worst_trend_line] < 0 ? "#ef4444" : "#10b981"}; font-weight: bold;">
                            {abs(improvements[worst_trend_line]):.1f}%</span></p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"Lỗi phân tích theo Line: {str(e)}")
    else:
        st.warning("⚠️ Không có đủ dữ liệu để phân tích theo Line")

with tab3:
    # Shift Analysis tab
    st.markdown('<div class="sub-header">Phân tích theo Ca sản xuất</div>', unsafe_allow_html=True)
    
    # Shift performance comparison
    st.markdown("### 👥 So sánh hiệu suất theo Ca")
    
    if not filtered_tem_vang_shift_df.empty:
        try:
            # Create shift performance summary
            shift_summary = filtered_tem_vang_shift_df.groupby("Shift").agg({
                "TEM_VANG": ["mean", "std", "min", "max", "count"],
                "Hold_Quantity": "sum",
                "Production_Volume": "sum"
            }).reset_index()
            
            # Flatten the column names
            shift_summary.columns = [
                "Shift" if col == "Shift" else 
                f"{col[0]}_{col[1]}" for col in shift_summary.columns
            ]
            
            # Calculate hold percentage
            shift_summary["Hold_Percentage"] = (shift_summary["Hold_Quantity_sum"] / shift_summary["Production_Volume_sum"]) * 100
            
            # Create two columns for side-by-side shift analysis
            shift_col1, shift_col2 = st.columns(2)
            
            with shift_col1:
                # Create TEM VÀNG by shift chart
                fig = go.Figure()
                
                # Determine colors based on TEM VÀNG value
                shift_colors = []
                for tem_value in shift_summary["TEM_VANG_mean"]:
                    if selected_line in ["7", "8"]:
                        target = 2.18
                    elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                        target = 0.29
                    else:
                        target = 0.41
                        
                    if tem_value <= target:
                        shift_colors.append("#10b981")  # Good - green
                    elif tem_value <= target * 1.2:
                        shift_colors.append("#f59e0b")  # Warning - amber
                    else:
                        shift_colors.append("#ef4444")  # Bad - red
                
                # Add TEM VÀNG bars
                fig.add_trace(go.Bar(
                    x=shift_summary["Shift"],
                    y=shift_summary["TEM_VANG_mean"],
                    marker_color=shift_colors,
                    text=shift_summary["TEM_VANG_mean"].round(2).astype(str) + "%",
                    textposition="auto",
                    hovertemplate="<b>Ca %{x}</b><br>TEM VÀNG: %{y:.2f}%<br>Số lượng mẫu: %{customdata}<extra></extra>",
                    customdata=shift_summary["TEM_VANG_count"]
                ))
                
                # Add error bars
                fig.add_trace(go.Scatter(
                    x=shift_summary["Shift"],
                    y=shift_summary["TEM_VANG_mean"],
                    error_y=dict(
                        type="data",
                        array=shift_summary["TEM_VANG_std"],
                        visible=True,
                        color="#0d2c54"
                    ),
                    mode="markers",
                    marker=dict(
                        color="rgba(0,0,0,0)",
                        line=dict(color="rgba(0,0,0,0)", width=0)
                    ),
                    showlegend=False,
                    hoverinfo="skip"
                ))
                
                # Add target line
                if selected_line in ["7", "8"]:
                    target_value = 2.18
                elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                    target_value = 0.29
                else:
                    target_value = 0.41
                    
                fig.add_hline(
                    y=target_value,
                    line_dash="dash",
                    line_color="#0d2c54",
                    line_width=2,
                    annotation=dict(
                        text=f"Mục tiêu: {target_value}%",
                        font=dict(color="#0d2c54"),
                        xref="paper",
                        x=0,
                        yref="y",
                        y=target_value
                    )
                )
                
                # Update layout
                fig.update_layout(
                    title="TEM VÀNG theo Ca sản xuất",
                    xaxis_title="Ca",
                    yaxis_title="TEM VÀNG (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=40),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333"),
                    hovermode="closest"
                )
                
                # Add grid lines for better readability
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add shift time info
                st.markdown("""
                <div class="shift-info">
                    <p>Ca 1: 6:00 - 14:00 | Ca 2: 14:00 - 22:00 | Ca 3: 22:00 - 6:00</p>
                </div>
                """, unsafe_allow_html=True)
            
            with shift_col2:
                # Create hold quantity by shift chart
                fig = go.Figure()
                
                # Add hold quantity bars
                fig.add_trace(go.Bar(
                    x=shift_summary["Shift"],
                    y=shift_summary["Hold_Quantity_sum"],
                    marker_color="#3b82f6",
                    name="Số lượng hold",
                    text=shift_summary["Hold_Quantity_sum"].astype(int),
                    textposition="auto",
                    hovertemplate="<b>Ca %{x}</b><br>Hold: %{y:,.0f}<br>Tỷ lệ: %{customdata:.2f}%<extra></extra>",
                    customdata=shift_summary["Hold_Percentage"]
                ))
                
                # Add production volume line on secondary y-axis
                fig.add_trace(go.Scatter(
                    x=shift_summary["Shift"],
                    y=shift_summary["Production_Volume_sum"],
                    mode="lines+markers",
                    marker=dict(color="#10b981", size=10),
                    line=dict(color="#10b981", width=3),
                    name="Sản lượng",
                    yaxis="y2",
                    hovertemplate="<b>Ca %{x}</b><br>Sản lượng: %{y:,.0f}<extra></extra>"
                ))
                
                # Update layout
                fig.update_layout(
                    title="Số lượng sản xuất và hold theo Ca",
                    xaxis_title="Ca",
                    yaxis_title="Số lượng hold",
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=40),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333"),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    yaxis2=dict(
                        title="Sản lượng",
                        overlaying="y",
                        side="right",
                        showgrid=False
                    )
                )
                
                # Add grid lines for better readability
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0",
                    secondary_y=False
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Add insights about shift performance
            best_shift = shift_summary.loc[shift_summary["TEM_VANG_mean"].idxmin()]
            worst_shift = shift_summary.loc[shift_summary["TEM_VANG_mean"].idxmax()]
            
            shift_diff_pct = ((worst_shift["TEM_VANG_mean"] / best_shift["TEM_VANG_mean"]) - 1) * 100
            
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-title">Phân tích hiệu suất Ca</div>
                <div class="insight-content">
                    <p>
                        Ca <span class="badge badge-good">{best_shift['Shift']}</span> có hiệu suất tốt nhất với 
                        TEM VÀNG trung bình <strong>{best_shift['TEM_VANG_mean']:.2f}%</strong>, trong khi
                        Ca <span class="badge badge-bad">{worst_shift['Shift']}</span> có TEM VÀNG trung bình 
                        <strong>{worst_shift['TEM_VANG_mean']:.2f}%</strong> 
                        (<span style="color: #ef4444">cao hơn {shift_diff_pct:.1f}%</span>).
                    </p>
                    <p>
                        Nguyên nhân có thể do: khác biệt về đội ngũ vận hành, trình độ kỹ thuật, mệt mỏi (đặc biệt Ca 3), 
                        hoặc quy trình kiểm soát chất lượng không nhất quán giữa các ca.
                    </p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Shift Leader Analysis
            st.markdown("### 👨‍💼 Phân tích theo Trưởng ca")
            
            if not filtered_tem_vang_leader_df.empty and len(filtered_tem_vang_leader_df["Leader"].unique()) > 1:
                # Create leader performance summary
                leader_summary = filtered_tem_vang_leader_df.groupby("Leader").agg({
                    "TEM_VANG": ["mean", "std", "min", "max", "count"],
                    "Hold_Quantity": "sum",
                    "Production_Volume": "sum"
                }).reset_index()
                
                # Flatten the column names
                leader_summary.columns = [
                    "Leader" if col == "Leader" else 
                    f"{col[0]}_{col[1]}" for col in leader_summary.columns
                ]
                
                # Calculate hold percentage
                leader_summary["Hold_Percentage"] = (leader_summary["Hold_Quantity_sum"] / leader_summary["Production_Volume_sum"]) * 100
                
                # Sort by TEM VÀNG performance
                leader_summary = leader_summary.sort_values("TEM_VANG_mean")
                
                # Create TEM VÀNG by leader chart
                fig = go.Figure()
                
                # Add TEM VÀNG bars
                fig.add_trace(go.Bar(
                    x=leader_summary["Leader"],
                    y=leader_summary["TEM_VANG_mean"],
                    marker=dict(
                        color=leader_summary["TEM_VANG_mean"],
                        colorscale="RdYlGn_r",
                        showscale=True,
                        colorbar=dict(
                            title="TEM VÀNG",
                            titleside="right"
                        )
                    ),
                    text=leader_summary["TEM_VANG_mean"].round(2).astype(str) + "%",
                    textposition="auto",
                    hovertemplate="<b>%{x}</b><br>TEM VÀNG: %{y:.2f}%<br>Số mẫu: %{customdata}<extra></extra>",
                    customdata=leader_summary["TEM_VANG_count"]
                ))
                
                # Update layout
                fig.update_layout(
                    title="TEM VÀNG theo Trưởng ca",
                    xaxis_title="Trưởng ca",
                    yaxis_title="TEM VÀNG (%)",
                    height=400,
                    margin=dict(l=40, r=40, t=60, b=100),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="#333333"),
                    hovermode="closest",
                    xaxis=dict(
                        tickangle=-45,
                        tickmode="array",
                        tickvals=list(range(len(leader_summary["Leader"]))),
                        ticktext=leader_summary["Leader"]
                    )
                )
                
                # Add grid lines for better readability
                fig.update_yaxes(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor="#f0f0f0"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add insights about leader performance
                if len(leader_summary) >= 2:
                    best_leader = leader_summary.iloc[0]
                    worst_leader = leader_summary.iloc[-1]
                    
                    leader_diff_pct = ((worst_leader["TEM_VANG_mean"] / best_leader["TEM_VANG_mean"]) - 1) * 100
                    
                    st.markdown(f"""
                    <div class="insight-card">
                        <div class="insight-title">Phân tích hiệu suất Trưởng ca</div>
                        <div class="insight-content">
                            <p>
                                Trưởng ca <span class="badge badge-good">{best_leader['Leader']}</span> đạt hiệu suất cao nhất với 
                                TEM VÀNG trung bình <strong>{best_leader['TEM_VANG_mean']:.2f}%</strong>, trong khi
                                <span class="badge badge-bad">{worst_leader['Leader']}</span> có TEM VÀNG trung bình 
                                <strong>{worst_leader['TEM_VANG_mean']:.2f}%</strong>.
                            </p>
                            <p>
                                <strong>Khuyến nghị:</strong> Tổ chức đào tạo chéo, chia sẻ phương pháp làm việc giữa các trưởng ca, 
                                đặc biệt là từ {best_leader['Leader']} để cải thiện hiệu suất chung.
                            </p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Line-Shift interaction analysis
                    if "Line" in filtered_tem_vang_shift_df.columns and "Shift" in filtered_tem_vang_shift_df.columns:
                        # Group by line and shift
                        line_shift_data = filtered_tem_vang_shift_df.groupby(["Line", "Shift"])["TEM_VANG"].mean().reset_index()
                        
                        # Create pivot table for heatmap
                        line_shift_pivot = line_shift_data.pivot(index="Line", columns="Shift", values="TEM_VANG")
                        
                        # Create heatmap
                        fig = px.imshow(
                            line_shift_pivot,
                            labels=dict(x="Ca", y="Line", color="TEM VÀNG (%)"),
                            x=line_shift_pivot.columns,
                            y=line_shift_pivot.index,
                            color_continuous_scale="RdYlGn_r",
                            aspect="auto",
                            text_auto=".2f"
                        )
                        
                        # Update layout
                        fig.update_layout(
                            title="Phân tích TEM VÀNG theo Line và Ca",
                            height=400,
                            margin=dict(l=40, r=40, t=60, b=40),
                            plot_bgcolor="white",
                            paper_bgcolor="white",
                            font=dict(color="#333333")
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("⚠️ Không có đủ dữ liệu về trưởng ca để phân tích")
        except Exception as e:
            st.error(f"Lỗi phân tích theo ca: {str(e)}")
    else:
        st.warning("⚠️ Không có đủ dữ liệu để phân tích theo ca")

with tab4:
    # Hour Analysis tab
    st.markdown('<div class="sub-header">Phân tích theo Giờ sản xuất</div>', unsafe_allow_html=True)
    
    if not filtered_tem_vang_hour_df.empty:
        try:
            # Sort by hour for visualization
            hour_tem_vang = filtered_tem_vang_hour_df.sort_values("Hour")
            
            # Create hour performance chart
            fig = go.Figure()
            
            # Add TEM VÀNG line
            fig.add_trace(go.Scatter(
                x=hour_tem_vang["Hour"],
                y=hour_tem_vang["TEM_VANG"],
                mode="lines+markers",
                name="TEM VÀNG",
                line=dict(color="#0d2c54", width=3),
                marker=dict(size=8, color="#0d2c54")
            ))
            
            # Map hours to shift labels for display
            hour_labels = {
                h: f"{h:02d}:00" for h in range(24)
            }
            
            # Add target line
            if selected_line in ["7", "8"]:
                target_value = 2.18
            elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                target_value = 0.29
            else:
                target_value = 0.41
                
            fig.add_hline(
                y=target_value,
                line_dash="dash",
                line_color="#ef4444",
                line_width=2,
                annotation=dict(
                    text=f"Mục tiêu: {target_value}%",
                    font=dict(color="#ef4444"),
                    xref="paper",
                    x=0,
                    yref="y",
                    y=target_value
                )
            )
            
            # Add shift background colors with improved transparency and labels
            fig.add_vrect(
                x0=6, x1=14,
                fillcolor="rgba(135, 206, 250, 0.2)",
                layer="below",
                line_width=0,
                annotation=dict(
                    text="Ca 1 (6:00-14:00)",
                    font=dict(size=12, color="#1e3a8a"),
                    x=10,
                    y=0.98,
                    yref="paper",
                    showarrow=False
                )
            )
            
            fig.add_vrect(
                x0=14, x1=22,
                fillcolor="rgba(255, 228, 181, 0.2)",
                layer="below",
                line_width=0,
                annotation=dict(
                    text="Ca 2 (14:00-22:00)",
                    font=dict(size=12, color="#1e3a8a"),
                    x=18,
                    y=0.98,
                    yref="paper",
                    showarrow=False
                )
            )
            
            fig.add_vrect(
                x0=0, x1=6,
                fillcolor="rgba(211, 211, 211, 0.3)",
                layer="below",
                line_width=0,
                annotation=dict(
                    text="Ca 3 (22:00-6:00)",
                    font=dict(size=12, color="#1e3a8a"),
                    x=3,
                    y=0.98,
                    yref="paper",
                    showarrow=False
                )
            )
            
            fig.add_vrect(
                x0=22, x1=24,
                fillcolor="rgba(211, 211, 211, 0.3)",
                layer="below",
                line_width=0
            )
            
            # Update layout
            fig.update_layout(
                title="Phân tích TEM VÀNG theo Giờ sản xuất",
                xaxis_title="Giờ",
                yaxis_title="TEM VÀNG (%)",
                height=500,
                margin=dict(l=40, r=40, t=80, b=60),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="#333333"),
                hovermode="closest",
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=12,
                    font_family="Arial"
                ),
                xaxis=dict(
                    tickmode='array',
                    tickvals=list(range(0, 24, 2)),
                    ticktext=[f"{i:02d}:00" for i in range(0, 24, 2)]
                )
            )
            
            # Add grid lines for better readability
            fig.update_xaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0"
            )
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Hour analysis with Hold Quantity
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add bars for hold quantity
            fig.add_trace(
                go.Bar(
                    x=hour_tem_vang["Hour"],
                    y=hour_tem_vang["Hold_Quantity"],
                    name="Số lượng hold",
                    marker_color="#3b82f6",
                    text=hour_tem_vang["Hold_Quantity"].astype(int),
                    textposition="auto"
                ),
                secondary_y=False
            )
            
            # Add line for hourly production
            fig.add_trace(
                go.Scatter(
                    x=hour_tem_vang["Hour"],
                    y=hour_tem_vang["Hourly_Production"],
                    name="Sản lượng / giờ",
                    mode="lines+markers",
                    marker=dict(color="#10b981", size=8),
                    line=dict(color="#10b981", width=2)
                ),
                secondary_y=True
            )
            
            # Add shift background colors
            fig.add_vrect(
                x0=6, x1=14,
                fillcolor="rgba(135, 206, 250, 0.2)",
                layer="below",
                line_width=0
            )
            
            fig.add_vrect(
                x0=14, x1=22,
                fillcolor="rgba(255, 228, 181, 0.2)",
                layer="below",
                line_width=0
            )
            
            fig.add_vrect(
                x0=0, x1=6,
                fillcolor="rgba(211, 211, 211, 0.3)",
                layer="below",
                line_width=0
            )
            
            fig.add_vrect(
                x0=22, x1=24,
                fillcolor="rgba(211, 211, 211, 0.3)",
                layer="below",
                line_width=0
            )
            
            # Update layout
            fig.update_layout(
                title="Số lượng hold và Sản lượng theo Giờ",
                xaxis_title="Giờ",
                height=400,
                margin=dict(l=40, r=40, t=60, b=60),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="#333333"),
                hovermode="x unified",
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=12,
                    font_family="Arial"
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                xaxis=dict(
                    tickmode='array',
                    tickvals=list(range(0, 24, 2)),
                    ticktext=[f"{i:02d}:00" for i in range(0, 24, 2)]
                )
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Số lượng hold", secondary_y=False)
            fig.update_yaxes(title_text="Sản lượng / giờ", secondary_y=True)
            
            # Add grid lines for better readability
            fig.update_xaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0"
            )
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0",
                secondary_y=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Peak hour analysis
            peak_hour = hour_tem_vang.loc[hour_tem_vang["TEM_VANG"].idxmax()]
            best_hour = hour_tem_vang.loc[hour_tem_vang["TEM_VANG"].idxmin()]
            
            # Determine shift for each hour
            peak_shift = "Ca 1" if 6 <= peak_hour["Hour"] < 14 else ("Ca 2" if 14 <= peak_hour["Hour"] < 22 else "Ca 3")
            best_shift = "Ca 1" if 6 <= best_hour["Hour"] < 14 else ("Ca 2" if 14 <= best_hour["Hour"] < 22 else "Ca 3")
            
            # Format hours for display
            peak_hour_fmt = f"{int(peak_hour['Hour']):02d}:00"
            best_hour_fmt = f"{int(best_hour['Hour']):02d}:00"
            
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-title">Phân tích theo giờ</div>
                <div class="insight-content">
                    <p>
                        <strong>Giờ có TEM VÀNG cao nhất:</strong> {peak_hour_fmt} ({peak_shift}) với 
                        <span style="color: #ef4444; font-weight: bold;">{peak_hour['TEM_VANG']:.2f}%</span>
                    </p>
                    <p>
                        <strong>Giờ có TEM VÀNG thấp nhất:</strong> {best_hour_fmt} ({best_shift}) với 
                        <span style="color: #10b981; font-weight: bold;">{best_hour['TEM_VANG']:.2f}%</span>
                    </p>
                    <p>
                        <strong>Nguyên nhân có thể:</strong> Mệt mỏi trong ca, thay đổi đội vận hành, tình trạng thiết bị, 
                        thời điểm bảo trì/vệ sinh, hoặc khác biệt về giám sát chất lượng.
                    </p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Lỗi phân tích theo giờ: {str(e)}")
    else:
        st.warning("⚠️ Không có đủ dữ liệu để phân tích theo giờ")

with tab5:
    # Defect Analysis tab
    st.markdown('<div class="sub-header">Phân tích chi tiết lỗi</div>', unsafe_allow_html=True)
    
    if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
        defect_counts = data["defect_patterns"]["defect_counts"]
        
        if not defect_counts.empty:
            # Create two columns for Pareto and defect details
            defect_col1, defect_col2 = st.columns([3, 2])
            
            with defect_col1:
                # Enhanced Pareto chart
                try:
                    # Create enhanced Pareto chart
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    # Use only top 10 defects for better visualization
                    top_defects = defect_counts.head(10)
                    
                    # Add bars for defect counts and hold quantity
                    fig.add_trace(
                        go.Bar(
                            x=top_defects["Defect_Name"],
                            y=top_defects["Count"],
                            name="Số lần xuất hiện",
                            marker_color="#0d2c54",
                            text=top_defects["Count"].astype(int),
                            textposition="auto"
                        ),
                        secondary_y=False
                    )
                    
                    if "Hold_Quantity" in top_defects.columns:
                        fig.add_trace(
                            go.Bar(
                                x=top_defects["Defect_Name"],
                                y=top_defects["Hold_Quantity"],
                                name="Số lượng hold",
                                marker_color="#ef4444",
                                opacity=0.7,
                                text=top_defects["Hold_Quantity"].astype(int),
                                textposition="auto"
                            ),
                            secondary_y=False
                        )
                    
                    # Add line for cumulative percentage
                    fig.add_trace(
                        go.Scatter(
                            x=top_defects["Defect_Name"],
                            y=top_defects["Cumulative"],
                            name="Tích lũy %",
                            mode="lines+markers",
                            marker=dict(color="#f59e0b", size=8),
                            line=dict(color="#f59e0b", width=3)
                        ),
                        secondary_y=True
                    )
                    
                    # Add 80% reference line
                    fig.add_hline(
                        y=80,
                        line_dash="dash",
                        line_color="#10b981",
                        line_width=2,
                        annotation=dict(
                            text="80% ngưỡng Pareto",
                            font=dict(color="#10b981"),
                            xref="paper",
                            x=1,
                            yref="y2",
                            y=80
                        ),
                        secondary_y=True
                    )
                    
                    # Update layout
                    fig.update_layout(
                        title="Phân tích Pareto các loại lỗi",
                        xaxis_title="Loại lỗi",
                        height=500,
                        margin=dict(l=40, r=40, t=60, b=100),
                        xaxis_tickangle=-45,
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(color="#333333"),
                        hovermode="x unified",
                        hoverlabel=dict(
                            bgcolor="white",
                            font_size=12,
                            font_family="Arial"
                        ),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        )
                    )
                    
                    # Set y-axes titles
                    fig.update_yaxes(title_text="Số lỗi / Số lượng hold", secondary_y=False)
                    fig.update_yaxes(title_text="Tích lũy %", secondary_y=True, range=[0, 100])
                    
                    # Add grid lines for better readability
                    fig.update_yaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor="#f0f0f0",
                        secondary_y=False
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ Pareto: {str(e)}")
            
            with defect_col2:
                # Top defects table with enhanced styling
                st.markdown("### Top 5 lỗi chính")
                
                top5_defects = defect_counts.head(5).copy()
                top5_defects["Percentage"] = top5_defects["Percentage"].round(1).astype(str) + "%"
                
                if "Hold_Quantity" in top5_defects.columns:
                    top5_defects["Hold_Quantity"] = top5_defects["Hold_Quantity"].astype(int)
                
                styled_top5 = pd.DataFrame({
                    "Loại lỗi": top5_defects["Defect_Name"],
                    "Số lần": top5_defects["Count"],
                    "Tỷ lệ": top5_defects["Percentage"],
                    "SL hold": top5_defects["Hold_Quantity"] if "Hold_Quantity" in top5_defects.columns else "N/A"
                })
                
                st.markdown(
                    f"""
                    <div class="data-table">
                        {styled_top5.to_html(index=False, classes='table table-striped')}
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                
                # Pareto analysis insight
                if "vital_few" in data["defect_patterns"]:
                    vital_few = data["defect_patterns"]["vital_few"]
                    
                    st.markdown(f"""
                    <div class="insight-card">
                        <div class="insight-title">Phân tích Pareto</div>
                        <div class="insight-content">
                            <p><strong>{len(vital_few)}</strong> loại lỗi ({len(vital_few)/len(defect_counts)*100:.0f}% tổng số loại) chiếm 80% tổng số lỗi.</p>
                            <p><strong>Tập trung cải tiến:</strong> {', '.join(vital_few['Defect_Name'].head(3).tolist())} và {len(vital_few)-3} loại khác</p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Defects by line heatmap
            st.markdown("### 🔍 Phân bố lỗi theo Line sản xuất")
            
            if "pivot_line_defects" in data["defect_patterns"]:
                try:
                    pivot_df = data["defect_patterns"]["pivot_line_defects"]
                    
                    if not pivot_df.empty:
                        # Use only top defects for cleaner visualization
                        top_defect_names = defect_counts.head(8)["Defect_Name"].tolist()
                        
                        # Filter pivot table to include only top defects
                        cols_to_include = [col for col in pivot_df.columns if col in top_defect_names]
                        
                        if cols_to_include:
                            filtered_pivot = pivot_df[cols_to_include]
                            
                            # Create heatmap
                            fig = px.imshow(
                                filtered_pivot,
                                labels=dict(x="Loại lỗi", y="Line", color="Số lỗi"),
                                x=filtered_pivot.columns,
                                y=filtered_pivot.index,
                                color_continuous_scale="YlOrRd",
                                aspect="auto",
                                text_auto=True
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title="Phân bố lỗi theo Line",
                                height=450,
                                margin=dict(l=40, r=40, t=60, b=80),
                                xaxis_tickangle=-45,
                                plot_bgcolor="white",
                                paper_bgcolor="white",
                                font=dict(color="#333333"),
                                hoverlabel=dict(
                                    bgcolor="white",
                                    font_size=12,
                                    font_family="Arial"
                                )
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Analysis of line-specific defects
                            line_specific_issues = []
                            
                            for line in filtered_pivot.index:
                                line_data = filtered_pivot.loc[line]
                                max_defect = line_data.idxmax()
                                max_count = line_data[max_defect]
                                total_line_defects = line_data.sum()
                                
                                if total_line_defects > 0:
                                    max_pct = (max_count / total_line_defects) * 100
                                    
                                    if max_pct > 40:  # Significant concentration on one defect
                                        line_specific_issues.append({
                                            "line": line,
                                            "defect": max_defect,
                                            "count": max_count,
                                            "percentage": max_pct
                                        })
                            
                            if line_specific_issues:
                                st.markdown("### 📊 Vấn đề đặc trưng theo Line")
                                
                                for issue in line_specific_issues:
                                    st.markdown(f"""
                                    <div class="warning-card">
                                        <div class="warning-title">Line {issue['line']}: Tập trung lỗi '{issue['defect']}'</div>
                                        <div class="insight-content">
                                            <p>Lỗi '{issue['defect']}' chiếm <strong>{issue['percentage']:.1f}%</strong> tổng số lỗi trên Line {issue['line']}</p>
                                            <p><strong>Khuyến nghị:</strong> Kiểm tra thiết bị, quy trình vận hành và đào tạo nhân viên trên Line này</p>
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                        else:
                            st.warning("Không có dữ liệu về top lỗi để hiển thị")
                    else:
                        st.warning("⚠️ Không có dữ liệu lỗi để hiển thị biểu đồ nhiệt")
                except Exception as e:
                    st.error(f"Lỗi tạo bản đồ nhiệt lỗi: {str(e)}")
            
            # Shift-specific defect analysis
            if "pivot_shift_defects" in data["defect_patterns"]:
                st.markdown("### ⏱️ Phân tích lỗi theo Ca sản xuất")
                
                try:
                    pivot_shift_df = data["defect_patterns"]["pivot_shift_defects"]
                    
                    if not pivot_shift_df.empty:
                        # Use only top defects for cleaner visualization
                        top_defect_names = defect_counts.head(8)["Defect_Name"].tolist()
                        
                        # Filter pivot table to include only top defects
                        cols_to_include = [col for col in pivot_shift_df.columns if col in top_defect_names]
                        
                        if cols_to_include:
                            filtered_pivot = pivot_shift_df[cols_to_include]
                            
                            # Create heatmap
                            fig = px.imshow(
                                filtered_pivot,
                                labels=dict(x="Loại lỗi", y="Ca", color="Số lỗi"),
                                x=filtered_pivot.columns,
                                y=filtered_pivot.index,
                                color_continuous_scale="YlOrRd",
                                aspect="auto",
                                text_auto=True
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title="Phân bố lỗi theo Ca",
                                height=350,
                                margin=dict(l=40, r=40, t=60, b=80),
                                xaxis_tickangle=-45,
                                plot_bgcolor="white",
                                paper_bgcolor="white",
                                font=dict(color="#333333")
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("Không có dữ liệu về top lỗi để hiển thị theo ca")
                except Exception as e:
                    st.error(f"Lỗi tạo bản đồ nhiệt lỗi theo ca: {str(e)}")
            
            # Recommendations based on defect analysis
            st.markdown("### 📋 Khuyến nghị dựa trên phân tích lỗi")
            
            if "vital_few" in data["defect_patterns"] and not data["defect_patterns"]["vital_few"].empty:
                vital_few = data["defect_patterns"]["vital_few"]
                
                st.markdown(f"""
                <div class="recommendation-card">
                    <div class="recommendation-title">Kế hoạch hành động chất lượng</div>
                    <div class="insight-content">
                        <ol>
                            <li>
                                <strong>Tập trung cải tiến:</strong> Ưu tiên giải quyết {len(vital_few.head(3))} lỗi chính: 
                                {', '.join(vital_few['Defect_Name'].head(3).tolist())}
                            </li>
                            <li>
                                <strong>Thành lập nhóm chất lượng:</strong> Tạo nhóm cải tiến chất lượng chuyên biệt cho từng loại lỗi chính
                            </li>
                            <li>
                                <strong>Phân tích nguyên nhân gốc rễ:</strong> Áp dụng phương pháp 5 Why và biểu đồ Ishikawa
                            </li>
                            <li>
                                <strong>Tiêu chuẩn hóa quy trình:</strong> Cập nhật SOP và đào tạo nhân viên về các cải tiến
                            </li>
                            <li>
                                <strong>Giám sát và đánh giá:</strong> Theo dõi hiệu quả của các biện pháp cải tiến bằng chỉ số KPI chất lượng
                            </li>
                        </ol>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.warning("⚠️ Không có dữ liệu lỗi để phân tích")
    else:
        st.warning("⚠️ Không có dữ liệu lỗi để phân tích")

# Footer with document info
st.markdown("""
<div class="footer">
    <p>Báo cáo chất lượng CF MMB | Cập nhật cuối: {}</p>
    <p>Báo cáo này được tạo tự động từ dữ liệu chất lượng. Vui lòng liên hệ Phòng QA để biết thêm chi tiết.</p>
</div>
""".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")), unsafe_allow_html=True)

# Implement auto-refresh if enabled
if auto_refresh:
    time.sleep(300)  # Wait 5 minutes to allow user to view the dashboard
    st.rerun()
