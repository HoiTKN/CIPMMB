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
                # Convert to numeric if not already
                df["Giờ"] = pd.to_numeric(df["Giờ"], errors='coerce')
                
                # Define shifts based on hour ranges
                df["Shift"] = pd.cut(
                    df["Giờ"],
                    bins=[-0.1, 6, 14, 22, 24],
                    labels=["3", "1", "2", "3"],
                    include_lowest=True
                )
                
                # Handle any NaN values in Shift
                df["Shift"] = df["Shift"].fillna("Unknown")
                
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
            aql_grouped = aql_copy.groupby(["Production_Date", "Line"])["Số lượng hold ( gói/thùng)"].sum().reset_index()
            aql_grouped.columns = ["Date", "Line", "Hold_Quantity"]
        else:
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu AQL để tính TEM VÀNG")
            return pd.DataFrame()
        
        # Group production data by date and line to get total production volumes
        if "Production_Date" in prod_copy.columns and "Line" in prod_copy.columns and "Sản lượng" in prod_copy.columns:
            prod_grouped = prod_copy.groupby(["Production_Date", "Line"])["Sản lượng"].sum().reset_index()
            prod_grouped.columns = ["Date", "Line", "Production_Volume"]
        else:
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu sản lượng để tính TEM VÀNG")
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

# Function to calculate TEM VÀNG by shift
def calculate_tem_vang_by_shift(aql_df, production_df):
    """Calculate TEM VÀNG by shift using AQL and production data"""
    try:
        # Check if dataframes are empty or missing required columns
        if aql_df.empty or production_df.empty:
            st.error("❌ Không thể tính TEM VÀNG theo ca - thiếu dữ liệu")
            return pd.DataFrame()
        
        if not all(col in aql_df.columns for col in ["Production_Date", "Line", "Số lượng hold ( gói/thùng)", "Shift"]):
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu AQL để tính TEM VÀNG theo ca")
            return pd.DataFrame()
        
        if not all(col in production_df.columns for col in ["Production_Date", "Line", "Sản lượng", "Ca"]):
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu sản lượng để tính TEM VÀNG theo ca")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Group AQL data by date, line, and shift to get total hold quantities
        aql_grouped = aql_copy.groupby(["Production_Date", "Line", "Shift"])["Số lượng hold ( gói/thùng)"].sum().reset_index()
        aql_grouped.columns = ["Date", "Line", "Shift", "Hold_Quantity"]
        
        # Rename Ca to Shift in production data for consistent naming
        prod_copy.rename(columns={"Ca": "Shift"}, inplace=True)
        
        # Group production data by date, line, and shift to get total production volumes
        prod_grouped = prod_copy.groupby(["Production_Date", "Line", "Shift"])["Sản lượng"].sum().reset_index()
        prod_grouped.columns = ["Date", "Line", "Shift", "Production_Volume"]
        
        # Merge the grouped data
        tem_vang_shift_df = pd.merge(
            aql_grouped, 
            prod_grouped, 
            on=["Date", "Line", "Shift"], 
            how="inner"
        )
        
        # Calculate TEM VÀNG percentage
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
        # Check if dataframes are empty or missing required columns
        if aql_df.empty or production_df.empty:
            st.error("❌ Không thể tính TEM VÀNG theo trưởng ca - thiếu dữ liệu")
            return pd.DataFrame()
        
        if not all(col in aql_df.columns for col in ["Production_Date", "Line", "Số lượng hold ( gói/thùng)", "Tên Trưởng ca"]):
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu AQL để tính TEM VÀNG theo trưởng ca")
            return pd.DataFrame()
        
        if not all(col in production_df.columns for col in ["Production_Date", "Line", "Sản lượng", "Người phụ trách"]):
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu sản lượng để tính TEM VÀNG theo trưởng ca")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        prod_copy = production_df.copy()
        
        # Group AQL data by date, line, and shift leader to get total hold quantities
        aql_grouped = aql_copy.groupby(["Production_Date", "Line", "Tên Trưởng ca"])["Số lượng hold ( gói/thùng)"].sum().reset_index()
        aql_grouped.columns = ["Date", "Line", "Leader", "Hold_Quantity"]
        
        # Rename column in production data for consistent naming
        prod_copy.rename(columns={"Người phụ trách": "Leader"}, inplace=True)
        
        # Group production data by date, line, and leader to get total production volumes
        prod_grouped = prod_copy.groupby(["Production_Date", "Line", "Leader"])["Sản lượng"].sum().reset_index()
        prod_grouped.columns = ["Date", "Line", "Leader", "Production_Volume"]
        
        # Merge the grouped data
        tem_vang_leader_df = pd.merge(
            aql_grouped, 
            prod_grouped, 
            on=["Date", "Line", "Leader"], 
            how="inner"
        )
        
        # Calculate TEM VÀNG percentage
        tem_vang_leader_df["TEM_VANG"] = (tem_vang_leader_df["Hold_Quantity"] / tem_vang_leader_df["Production_Volume"]) * 100
        
        # Add month column for filtering
        tem_vang_leader_df["Production_Month"] = tem_vang_leader_df["Date"].dt.strftime("%m/%Y")
        
        return tem_vang_leader_df
        
    except Exception as e:
        st.error(f"❌ Lỗi tính toán TEM VÀNG theo trưởng ca: {str(e)}")
        return pd.DataFrame()

# Function to calculate TEM VÀNG by hour
def calculate_tem_vang_by_hour(aql_df, production_df):
    """Calculate TEM VÀNG by hour using AQL and production data"""
    try:
        # Check if dataframes are empty or missing required columns
        if aql_df.empty or production_df.empty:
            st.error("❌ Không thể tính TEM VÀNG theo giờ - thiếu dữ liệu")
            return pd.DataFrame()
        
        if not all(col in aql_df.columns for col in ["Production_Date", "Line", "Số lượng hold ( gói/thùng)", "Giờ"]):
            st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu AQL để tính TEM VÀNG theo giờ")
            return pd.DataFrame()
        
        # Create copies to avoid modifying originals
        aql_copy = aql_df.copy()
        
        # Ensure Giờ is numeric
        aql_copy["Giờ"] = pd.to_numeric(aql_copy["Giờ"], errors='coerce')
        
        # Group AQL data by hour to get total hold quantities
        aql_grouped = aql_copy.groupby("Giờ")["Số lượng hold ( gói/thùng)"].sum().reset_index()
        aql_grouped.columns = ["Hour", "Hold_Quantity"]
        
        # Map hours to shifts to get production volumes
        hour_to_shift = {
            h: "1" if 6 <= h < 14 else ("2" if 14 <= h < 22 else "3")
            for h in range(24)
        }
        
        aql_grouped["Shift"] = aql_grouped["Hour"].map(hour_to_shift)
        
        # Calculate total production by shift
        prod_copy = production_df.copy()
        prod_copy.rename(columns={"Ca": "Shift"}, inplace=True)
        
        shift_production = prod_copy.groupby("Shift")["Sản lượng"].sum().reset_index()
        
        # Merge to get production volume for each hour
        # We'll distribute the shift production evenly across hours in that shift
        hours_per_shift = {
            "1": 8,  # 6-14 (8 hours)
            "2": 8,  # 14-22 (8 hours)
            "3": 8   # 22-6 (8 hours)
        }
        
        # Join shift production to hours
        tem_vang_hour_df = pd.merge(
            aql_grouped,
            shift_production,
            on="Shift",
            how="left"
        )
        
        # Distribute production evenly by hours in shift
        tem_vang_hour_df["Hourly_Production"] = tem_vang_hour_df.apply(
            lambda row: row["Sản lượng"] / hours_per_shift[row["Shift"]] 
            if row["Shift"] in hours_per_shift and row["Sản lượng"] > 0
            else 0,
            axis=1
        )
        
        # Calculate TEM VÀNG percentage
        tem_vang_hour_df["TEM_VANG"] = (tem_vang_hour_df["Hold_Quantity"] / tem_vang_hour_df["Hourly_Production"]) * 100
        
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
            st.error("❌ Không thể ánh xạ mã lỗi - thiếu dữ liệu")
            return pd.DataFrame()
        
        # Create a copy to avoid modifying the original
        df = aql_df.copy()
        
        # Create a Defect_Name column
        df["Defect_Name"] = ""
        
        # Get defect code column from AQL data
        defect_code_col = "Defect code" if "Defect code" in df.columns else None
        
        if not defect_code_col:
            st.warning("⚠️ Không tìm thấy cột mã lỗi trong dữ liệu AQL")
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
            line = row["Line"] if "Line" in row else ""
            defect_code = row[defect_code_col] if defect_code_col in row else ""
            
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

# Function to analyze defect patterns (revised to use defect names)
def analyze_defect_patterns(aql_df_with_names):
    """Analyze defect patterns in AQL data using defect names instead of codes"""
    try:
        # Check if dataframe is empty
        if aql_df_with_names.empty:
            return {}
        
        # Create copy to avoid modifying original
        df = aql_df_with_names.copy()
        
        # Group by defect name to get frequency
        if "Defect_Name" in df.columns:
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
            pivot_line_defects = line_defects.pivot(index="Line", columns="Defect_Name", values="Count").fillna(0)
            
            # Return the analysis results
            return {
                "defect_counts": defect_counts,
                "vital_few": vital_few,
                "line_defects": line_defects,
                "pivot_line_defects": pivot_line_defects
            }
        else:
            st.warning("⚠️ Thiếu cột 'Defect_Name' trong dữ liệu AQL để phân tích mẫu lỗi")
            return {}
            
    except Exception as e:
        st.error(f"❌ Lỗi phân tích mẫu lỗi: {str(e)}")
        return {}

# Load all data needed
@st.cache_data(ttl=600)  # Cache the combined data for 10 minutes
def load_all_data():
    """Load and prepare all required data"""
    
    # Load raw data
    aql_df = load_aql_data()
    production_df = load_production_data()
    aql_goi_df = load_aql_goi_data()  
    aql_to_ly_df = load_aql_to_ly_data()
    
    # Map defect codes to names
    aql_df_with_names = map_defect_codes_to_names(aql_df, aql_goi_df, aql_to_ly_df)
    
    # Calculate TEM VÀNG metrics
    tem_vang_df = calculate_tem_vang(aql_df, production_df)
    tem_vang_shift_df = calculate_tem_vang_by_shift(aql_df, production_df)
    tem_vang_leader_df = calculate_tem_vang_by_leader(aql_df, production_df)
    tem_vang_hour_df = calculate_tem_vang_by_hour(aql_df, production_df)
    
    # Analyze defect patterns with names
    defect_patterns = analyze_defect_patterns(aql_df_with_names)
    
    return {
        "aql_data": aql_df,
        "aql_data_with_names": aql_df_with_names,
        "production_data": production_df,
        "tem_vang_data": tem_vang_df,
        "tem_vang_shift_data": tem_vang_shift_df,
        "tem_vang_leader_data": tem_vang_leader_df,
        "tem_vang_hour_data": tem_vang_hour_df,
        "defect_patterns": defect_patterns
    }

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
    st.markdown("<h2 style='text-align: center; color: #1E3A8A;'>Bộ lọc</h2>", unsafe_allow_html=True)
    
    # Initialize filtered dataframes
    filtered_aql_df = data["aql_data_with_names"].copy()
    filtered_tem_vang_df = data["tem_vang_data"].copy()
    filtered_tem_vang_shift_df = data["tem_vang_shift_data"].copy()
    filtered_tem_vang_leader_df = data["tem_vang_leader_data"].copy()
    filtered_tem_vang_hour_df = data["tem_vang_hour_data"].copy()
    
    # Date filter for production data
    st.subheader("Khoảng thời gian sản xuất")
    
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
        value=(min_prod_date, max_prod_date),
        min_value=min_prod_date - timedelta(days=365),
        max_value=max_prod_date + timedelta(days=30)
    )
    
    # Apply production date filter if a range is selected
    if len(prod_date_range) == 2:
        start_date, end_date = prod_date_range
        
        # Convert to datetime for filtering
        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        
        # Apply to AQL data
        if "Production_Date" in filtered_aql_df.columns:
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
        
        if "Line" in filtered_aql_df.columns:
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
        
        if "Shift" in filtered_aql_df.columns:
            filtered_aql_df = filtered_aql_df[filtered_aql_df["Shift"] == selected_shift]
    
    # Refresh button
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Show last update time
    st.markdown(f"**Cập nhật cuối:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Add auto-refresh checkbox
    auto_refresh = st.checkbox("⏱️ Tự động làm mới (5p)", value=False)

# Production Quality Analysis (just the first tab)
st.markdown('<div class="sub-header">Tổng quan chất lượng sản xuất</div>', unsafe_allow_html=True)

# Key metrics row
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
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">TEM VÀNG trung bình</div>
            <div class="metric-value">{avg_tem_vang:.2f}%</div>
            <div style="color: {'red' if tem_delta > 0 else 'green'};">
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
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Số loại lỗi</div>
            <div class="metric-value">{defect_types}</div>
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
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Tổng sản lượng</div>
            <div class="metric-value">{total_production:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Tổng sản lượng</div>
            <div class="metric-value">N/A</div>
        </div>
        """, unsafe_allow_html=True)

# TEM VÀNG Analysis
st.markdown('<div class="sub-header">Phân tích TEM VÀNG</div>', unsafe_allow_html=True)

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
            
            # Set the appropriate target based on line selection
            if selected_line in ["7", "8"]:
                target_value = 2.18
                target_label = "Mục tiêu Line 7-8 (2.18%)"
            elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                target_value = 0.29
                target_label = "Mục tiêu Line 1-6 (0.29%)"
            else:
                target_value = 0.41
                target_label = "Mục tiêu tổng (0.41%)"
            
            # Add target line
            fig.add_hline(
                y=target_value,
                line_dash="dash",
                line_color="red",
                annotation_text=target_label
            )
            
            # Update layout
            fig.update_layout(
                title="Xu hướng TEM VÀNG theo thời gian",
                xaxis_title="Ngày",
                yaxis_title="TEM VÀNG (%)",
                height=350,
                margin=dict(l=40, r=40, t=40, b=40)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ xu hướng TEM VÀNG: {str(e)}")

with tem_col2:
    # TEM VÀNG by line
    if not filtered_tem_vang_df.empty:
        try:
            # Group by line to get average TEM VÀNG per line
            line_tem_vang = filtered_tem_vang_df.groupby("Line")[["TEM_VANG", "Hold_Quantity"]].mean().reset_index()
            
            # Sort by Line number
            line_tem_vang = line_tem_vang.sort_values("Line")
            
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
            
            # Add target lines for different line groups
            fig.add_shape(
                type="line",
                x0=-0.5, x1=5.5,  # Lines 1-6
                y0=0.29, y1=0.29,
                line=dict(color="green", width=2, dash="dash"),
                name="Target Lines 1-6"
            )
            
            fig.add_shape(
                type="line",
                x0=5.5, x1=7.5,  # Lines 7-8
                y0=2.18, y1=2.18,
                line=dict(color="red", width=2, dash="dash"),
                name="Target Lines 7-8"
            )
            
            # Add annotations for targets
            fig.add_annotation(
                x=2.5, y=0.29,
                text="Target Lines 1-6: 0.29%",
                showarrow=False,
                yshift=10,
                font=dict(size=10, color="green")
            )
            
            fig.add_annotation(
                x=6.5, y=2.18,
                text="Target Lines 7-8: 2.18%",
                showarrow=False,
                yshift=10,
                font=dict(size=10, color="red")
            )
            
            # Update layout
            fig.update_layout(
                title="TEM VÀNG theo Line sản xuất",
                xaxis_title="Line",
                yaxis_title="TEM VÀNG (%)",
                height=350,
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(
                    tickmode='array',
                    tickvals=list(range(1, 9)),
                    ticktext=[str(i) for i in range(1, 9)]
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ TEM VÀNG theo line: {str(e)}")

# TEM VÀNG by Shift Analysis
st.markdown('<div class="sub-header">Phân tích TEM VÀNG theo ca</div>', unsafe_allow_html=True)

shift_col1, shift_col2 = st.columns(2)

with shift_col1:
    # TEM VÀNG by shift
    if not filtered_tem_vang_shift_df.empty:
        try:
            # Group by shift to get average TEM VÀNG per shift
            shift_tem_vang = filtered_tem_vang_shift_df.groupby("Shift")[["TEM_VANG", "Hold_Quantity"]].mean().reset_index()
            
            # Sort by shift number
            shift_tem_vang = shift_tem_vang.sort_values("Shift")
            
            # Create figure
            fig = go.Figure()
            
            # Add TEM VÀNG bars
            fig.add_trace(go.Bar(
                x=shift_tem_vang["Shift"],
                y=shift_tem_vang["TEM_VANG"],
                name="TEM VÀNG",
                marker_color="royalblue",
                text=shift_tem_vang["TEM_VANG"].round(2).astype(str) + "%",
                textposition="auto"
            ))
            
            # Set the appropriate target based on line selection
            if selected_line in ["7", "8"]:
                target_value = 2.18
                target_label = "Mục tiêu Line 7-8 (2.18%)"
            elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                target_value = 0.29
                target_label = "Mục tiêu Line 1-6 (0.29%)"
            else:
                target_value = 0.41
                target_label = "Mục tiêu tổng (0.41%)"
            
            # Add target line
            fig.add_hline(
                y=target_value,
                line_dash="dash",
                line_color="red",
                annotation_text=target_label
            )
            
            # Update layout
            fig.update_layout(
                title="TEM VÀNG theo ca",
                xaxis_title="Ca",
                yaxis_title="TEM VÀNG (%)",
                height=350,
                margin=dict(l=40, r=40, t=40, b=40)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ TEM VÀNG theo ca: {str(e)}")
    else:
        st.warning("⚠️ Không có dữ liệu TEM VÀNG theo ca")

with shift_col2:
    # TEM VÀNG by shift leader
    if not filtered_tem_vang_leader_df.empty:
        try:
            # Group by leader to get average TEM VÀNG per leader
            leader_tem_vang = filtered_tem_vang_leader_df.groupby("Leader")[["TEM_VÀNG", "Hold_Quantity"]].mean().reset_index()
            
            # Sort by TEM VÀNG value
            leader_tem_vang = leader_tem_vang.sort_values("TEM_VÀNG", ascending=False)
            
            # Create figure
            fig = go.Figure()
            
            # Add TEM VÀNG bars
            fig.add_trace(go.Bar(
                x=leader_tem_vang["Leader"],
                y=leader_tem_vang["TEM_VANG"],
                name="TEM VÀNG",
                marker_color="royalblue",
                text=leader_tem_vang["TEM_VANG"].round(2).astype(str) + "%",
                textposition="auto"
            ))
            
            # Set the appropriate target based on line selection
            if selected_line in ["7", "8"]:
                target_value = 2.18
                target_label = "Mục tiêu Line 7-8 (2.18%)"
            elif selected_line in ["1", "2", "3", "4", "5", "6"]:
                target_value = 0.29
                target_label = "Mục tiêu Line 1-6 (0.29%)"
            else:
                target_value = 0.41
                target_label = "Mục tiêu tổng (0.41%)"
            
            # Add target line
            fig.add_hline(
                y=target_value,
                line_dash="dash",
                line_color="red",
                annotation_text=target_label
            )
            
            # Update layout
            fig.update_layout(
                title="TEM VÀNG theo trưởng ca",
                xaxis_title="Trưởng ca",
                yaxis_title="TEM VÀNG (%)",
                height=350,
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis_tickangle=-45
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ TEM VÀNG theo trưởng ca: {str(e)}")
    else:
        st.warning("⚠️ Không có dữ liệu TEM VÀNG theo trưởng ca")

# TEM VÀNG by Hour Analysis
st.markdown('<div class="sub-header">Phân tích TEM VÀNG theo giờ</div>', unsafe_allow_html=True)

if not filtered_tem_vang_hour_df.empty:
    try:
        # Sort by hour
        hour_tem_vang = filtered_tem_vang_hour_df.sort_values("Hour")
        
        # Create figure
        fig = go.Figure()
        
        # Add TEM VÀNG line
        fig.add_trace(go.Scatter(
            x=hour_tem_vang["Hour"],
            y=hour_tem_vang["TEM_VANG"],
            mode="lines+markers",
            name="TEM VÀNG",
            line=dict(color="royalblue", width=2),
            marker=dict(size=6)
        ))
        
        # Set the appropriate target based on line selection
        if selected_line in ["7", "8"]:
            target_value = 2.18
            target_label = "Mục tiêu Line 7-8 (2.18%)"
        elif selected_line in ["1", "2", "3", "4", "5", "6"]:
            target_value = 0.29
            target_label = "Mục tiêu Line 1-6 (0.29%)"
        else:
            target_value = 0.41
            target_label = "Mục tiêu tổng (0.41%)"
        
        # Add target line
        fig.add_hline(
            y=target_value,
            line_dash="dash",
            line_color="red",
            annotation_text=target_label
        )
        
        # Add shift background colors
        fig.add_vrect(
            x0=6, x1=14,
            fillcolor="rgba(135, 206, 250, 0.2)",
            layer="below",
            line_width=0,
            annotation_text="Ca 1 (6-14)",
            annotation_position="top left"
        )
        
        fig.add_vrect(
            x0=14, x1=22,
            fillcolor="rgba(255, 228, 181, 0.2)",
            layer="below",
            line_width=0,
            annotation_text="Ca 2 (14-22)",
            annotation_position="top left"
        )
        
        fig.add_vrect(
            x0=0, x1=6,
            fillcolor="rgba(211, 211, 211, 0.2)",
            layer="below",
            line_width=0,
            annotation_text="Ca 3 (22-6)",
            annotation_position="top left"
        )
        
        fig.add_vrect(
            x0=22, x1=24,
            fillcolor="rgba(211, 211, 211, 0.2)",
            layer="below",
            line_width=0
        )
        
        # Update layout
        fig.update_layout(
            title="Phân tích TEM VÀNG theo giờ",
            xaxis_title="Giờ",
            yaxis_title="TEM VÀNG (%)",
            height=400,
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis=dict(
                tickmode='array',
                tickvals=list(range(0, 24)),
                ticktext=[f"{i:02d}:00" for i in range(0, 24)]
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi tạo biểu đồ TEM VÀNG theo giờ: {str(e)}")
else:
    st.warning("⚠️ Không có dữ liệu TEM VÀNG theo giờ")

# Defect Analysis
st.markdown('<div class="sub-header">Phân tích lỗi theo Line</div>', unsafe_allow_html=True)

defect_col1, defect_col2 = st.columns(2)

with defect_col1:
    # Pareto chart of defects by name
    if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
        try:
            defect_counts = data["defect_patterns"]["defect_counts"]
            
            # Create Pareto chart
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add bars for defect counts and hold quantity
            fig.add_trace(
                go.Bar(
                    x=defect_counts["Defect_Name"],
                    y=defect_counts["Count"],
                    name="Số lần xuất hiện",
                    marker_color="steelblue"
                ),
                secondary_y=False
            )
            
            if "Hold_Quantity" in defect_counts.columns:
                fig.add_trace(
                    go.Bar(
                        x=defect_counts["Defect_Name"],
                        y=defect_counts["Hold_Quantity"],
                        name="Số lượng hold",
                        marker_color="darkred",
                        opacity=0.7
                    ),
                    secondary_y=False
                )
            
            # Add line for cumulative percentage
            fig.add_trace(
                go.Scatter(
                    x=defect_counts["Defect_Name"],
                    y=defect_counts["Cumulative"],
                    name="Tích lũy %",
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
                annotation_text="80% lỗi",
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title="Phân tích Pareto các loại lỗi",
                xaxis_title="Tên lỗi",
                height=400,
                margin=dict(l=40, r=40, t=40, b=80),
                xaxis_tickangle=-45,
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
            fig.update_yaxes(title_text="Tích lũy %", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add Pareto analysis insight
            if "vital_few" in data["defect_patterns"]:
                vital_few = data["defect_patterns"]["vital_few"]
                
                st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">Phân tích Pareto</div>
                    <div class="insight-content">
                        <p>{len(vital_few)} loại lỗi ({len(vital_few)/len(defect_counts)*100:.0f}% tổng số loại) chiếm 80% số lỗi.</p>
                        <p>Tập trung cải tiến chất lượng vào: {', '.join(vital_few['Defect_Name'].tolist())}</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ Pareto: {str(e)}")

with defect_col2:
    # Defects by line heatmap
    if "defect_patterns" in data and "pivot_line_defects" in data["defect_patterns"]:
        try:
            pivot_df = data["defect_patterns"]["pivot_line_defects"]
            
            if not pivot_df.empty:
                # Create heatmap
                fig = px.imshow(
                    pivot_df,
                    labels=dict(x="Tên lỗi", y="Line", color="Số lỗi"),
                    x=pivot_df.columns,
                    y=pivot_df.index,
                    color_continuous_scale="YlOrRd",
                    aspect="auto"
                )
                
                # Update layout
                fig.update_layout(
                    title="Phân bố lỗi theo Line",
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=80),
                    xaxis_tickangle=-45
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("⚠️ Không có dữ liệu lỗi để hiển thị biểu đồ nhiệt")
        except Exception as e:
            st.error(f"Lỗi tạo bản đồ nhiệt lỗi: {str(e)}")

# Implement auto-refresh if enabled
if auto_refresh:
    time.sleep(5)  # Wait 5 seconds to allow user to view the dashboard
    st.experimental_rerun()  # Then refresh
