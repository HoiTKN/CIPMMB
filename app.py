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

# Function to load complaint data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_complaint_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Không thể xác thực với Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL (complaint data)
        sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit"
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet and get the worksheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            
            # Try to get the "Integrated_Data" worksheet
            try:
                worksheet = spreadsheet.worksheet('Integrated_Data')
            except gspread.exceptions.WorksheetNotFound:
                # Fall back to first worksheet if Integrated_Data doesn't exist
                worksheet = spreadsheet.get_worksheet(0)
            
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
            st.error(f"❌ Lỗi truy cập bảng dữ liệu khiếu nại: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Lỗi tải dữ liệu khiếu nại: {str(e)}")
        return pd.DataFrame()

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
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Lỗi truy cập bảng dữ liệu sản lượng: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Lỗi tải dữ liệu sản lượng: {str(e)}")
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

# Function to analyze defect patterns
def analyze_defect_patterns(aql_df):
    """Analyze defect patterns in AQL data"""
    try:
        # Check if dataframe is empty
        if aql_df.empty:
            return {}
        
        # Create copy to avoid modifying original
        df = aql_df.copy()
        
        # Group by defect code to get frequency
        if "Defect code" in df.columns:
            defect_counts = df.groupby("Defect code").size().reset_index(name="Count")
            defect_counts = defect_counts.sort_values("Count", ascending=False)
            
            # Calculate percentages
            total_defects = defect_counts["Count"].sum()
            defect_counts["Percentage"] = (defect_counts["Count"] / total_defects * 100).round(1)
            defect_counts["Cumulative"] = defect_counts["Percentage"].cumsum()
            
            # Identify top defects (80% by Pareto principle)
            vital_few = defect_counts[defect_counts["Cumulative"] <= 80]
            
            # Group by Line and Defect code to get line-specific patterns
            line_defects = df.groupby(["Line", "Defect code"]).size().reset_index(name="Count")
            pivot_line_defects = line_defects.pivot(index="Line", columns="Defect code", values="Count").fillna(0)
            
            # Calculate defect rates by MDG (assuming MDG is stored in "Máy" column)
            if "Máy" in df.columns:
                mdg_defects = df.groupby(["Line", "Máy", "Defect code"]).size().reset_index(name="Count")
            else:
                mdg_defects = pd.DataFrame()
            
            # Return the analysis results
            return {
                "defect_counts": defect_counts,
                "vital_few": vital_few,
                "line_defects": line_defects,
                "pivot_line_defects": pivot_line_defects,
                "mdg_defects": mdg_defects
            }
        else:
            st.warning("⚠️ Thiếu cột 'Defect code' trong dữ liệu AQL để phân tích mẫu lỗi")
            return {}
            
    except Exception as e:
        st.error(f"❌ Lỗi phân tích mẫu lỗi: {str(e)}")
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
                st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu AQL để liên kết")
                return pd.DataFrame()
            
            # Group complaints by date, line, and defect type
            if "Tên lỗi" in complaint_copy.columns and "Line" in complaint_copy.columns:
                # Count unique ticket IDs for each group
                if "Mã ticket" in complaint_copy.columns:
                    complaint_grouped = complaint_copy.groupby(["Production_Date", "Line", "Tên lỗi"])["Mã ticket"].nunique().reset_index(name="Complaint_Count")
                else:
                    complaint_grouped = complaint_copy.groupby(["Production_Date", "Line", "Tên lỗi"]).size().reset_index(name="Complaint_Count")
            else:
                st.warning("⚠️ Thiếu cột cần thiết trong dữ liệu khiếu nại để liên kết")
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
            st.warning("⚠️ Thiếu cột ngày để liên kết lỗi với khiếu nại")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"❌ Lỗi liên kết lỗi với khiếu nại: {str(e)}")
        return pd.DataFrame()

# Load the data
@st.cache_data(ttl=600)  # Cache the combined data for 10 minutes
def load_all_data():
    """Load and prepare all required data"""
    
    # Load raw data
    complaint_df = load_complaint_data()
    aql_df = load_aql_data()
    production_df = load_production_data()
    
    # Calculate TEM VÀNG
    tem_vang_df = calculate_tem_vang(aql_df, production_df)
    
    # Analyze defect patterns
    defect_patterns = analyze_defect_patterns(aql_df)
    
    # Link defects with complaints
    linked_defects_df = link_defects_with_complaints(aql_df, complaint_df)
    
    return {
        "complaint_data": complaint_df,
        "aql_data": aql_df,
        "production_data": production_df,
        "tem_vang_data": tem_vang_df,
        "defect_patterns": defect_patterns,
        "linked_defects": linked_defects_df
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
    filtered_aql_df = data["aql_data"].copy()
    filtered_complaint_df = data["complaint_data"].copy()
    filtered_tem_vang_df = data["tem_vang_data"].copy()
    
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
    
    # Date filter for complaint data
    st.subheader("Khoảng thời gian khiếu nại")
    
    # Get min and max dates from complaint data
    if not data["complaint_data"].empty and "Production_Date" in data["complaint_data"].columns:
        min_complaint_date = data["complaint_data"]["Production_Date"].min().date()
        max_complaint_date = data["complaint_data"]["Production_Date"].max().date()
    else:
        min_complaint_date = datetime.now().date() - timedelta(days=365)
        max_complaint_date = datetime.now().date()
    
    # Create date range selector for complaint data
    complaint_date_range = st.date_input(
        "Chọn khoảng thời gian khiếu nại",
        value=(min_complaint_date, max_complaint_date),
        min_value=min_complaint_date - timedelta(days=365),
        max_value=max_complaint_date + timedelta(days=30)
    )
    
    # Apply complaint date filter if a range is selected
    if len(complaint_date_range) == 2:
        start_date, end_date = complaint_date_range
        
        # Convert to datetime for filtering
        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        
        # Apply to complaint data
        if "Production_Date" in filtered_complaint_df.columns:
            filtered_complaint_df = filtered_complaint_df[
                (filtered_complaint_df["Production_Date"] >= start_datetime) & 
                (filtered_complaint_df["Production_Date"] <= end_datetime)
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
        
        if "Line" in filtered_complaint_df.columns:
            filtered_complaint_df = filtered_complaint_df[filtered_complaint_df["Line"] == selected_line]
    
    # Product filter
    if not data["complaint_data"].empty and "Tên sản phẩm" in data["complaint_data"].columns:
        try:
            products = ["Tất cả"] + sorted(data["complaint_data"]["Tên sản phẩm"].unique().tolist())
            selected_product = st.selectbox("🍜 Chọn sản phẩm", products)
            
            if selected_product != "Tất cả":
                filtered_complaint_df = filtered_complaint_df[filtered_complaint_df["Tên sản phẩm"] == selected_product]
                
                # Filter AQL data by item if possible
                if "Tên sản phẩm" in filtered_aql_df.columns:
                    filtered_aql_df = filtered_aql_df[filtered_aql_df["Tên sản phẩm"] == selected_product]
        except Exception as e:
            st.warning(f"Lỗi ở bộ lọc sản phẩm: {e}")
    
    # Refresh button
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Show last update time
    st.markdown(f"**Cập nhật cuối:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Add auto-refresh checkbox
    auto_refresh = st.checkbox("⏱️ Tự động làm mới (5p)", value=False)

# Main dashboard layout with tabs for the 3 pages
tab1, tab2, tab3 = st.tabs([
    "📈 Phân tích chất lượng sản xuất", 
    "🔍 Phân tích khiếu nại khách hàng",
    "🔄 Liên kết chất lượng trong và ngoài"
])

# Page 1: Production Quality Analysis (TEM VÀNG and defects by line/MDG)
with tab1:
    st.markdown('<div class="sub-header">Tổng quan chất lượng sản xuất</div>', unsafe_allow_html=True)
    
    # Key metrics row
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        if not filtered_tem_vang_df.empty:
            avg_tem_vang = filtered_tem_vang_df["TEM_VANG"].mean()
            tem_target = 2.18  # TEM VÀNG target
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
        if not filtered_aql_df.empty and "Defect code" in filtered_aql_df.columns:
            defect_types = filtered_aql_df["Defect code"].nunique()
            
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
                
                # Add target line
                fig.add_hline(
                    y=2.18,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Mục tiêu (2.18%)"
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
                    annotation_text="Mục tiêu (2.18%)"
                )
                
                # Update layout
                fig.update_layout(
                    title="TEM VÀNG theo Line sản xuất",
                    xaxis_title="Line",
                    yaxis_title="TEM VÀNG (%)",
                    height=350,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Lỗi tạo biểu đồ TEM VÀNG theo line: {str(e)}")
    
    # Defect Analysis by Line and MDG
    st.markdown('<div class="sub-header">Phân tích lỗi theo Line và MDG</div>', unsafe_allow_html=True)
    
    defect_col1, defect_col2 = st.columns(2)
    
    with defect_col1:
        # Pareto chart of defects
        if "defect_patterns" in data and "defect_counts" in data["defect_patterns"]:
            try:
                defect_counts = data["defect_patterns"]["defect_counts"]
                
                # Create Pareto chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add bars for defect counts
                fig.add_trace(
                    go.Bar(
                        x=defect_counts["Defect code"],
                        y=defect_counts["Count"],
                        name="Số lỗi",
                        marker_color="steelblue"
                    ),
                    secondary_y=False
                )
                
                # Add line for cumulative percentage
                fig.add_trace(
                    go.Scatter(
                        x=defect_counts["Defect code"],
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
                    xaxis_title="Mã lỗi",
                    height=350,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                # Set y-axes titles
                fig.update_yaxes(title_text="Số lỗi", secondary_y=False)
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
                            <p>Tập trung cải tiến chất lượng vào: {', '.join(vital_few['Defect code'].tolist())}</p>
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
                        labels=dict(x="Mã lỗi", y="Line", color="Số lỗi"),
                        x=pivot_df.columns,
                        y=pivot_df.index,
                        color_continuous_scale="YlOrRd",
                        aspect="auto"
                    )
                    
                    # Update layout
                    fig.update_layout(
                        title="Phân bố lỗi theo Line",
                        height=350,
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("⚠️ Không có dữ liệu lỗi để hiển thị biểu đồ nhiệt")
            except Exception as e:
                st.error(f"Lỗi tạo bản đồ nhiệt lỗi: {str(e)}")
    
    # MDG Analysis
    st.markdown('<div class="sub-header">Phân tích theo MDG (Máy)</div>', unsafe_allow_html=True)
    
    if "defect_patterns" in data and "mdg_defects" in data["defect_patterns"] and not data["defect_patterns"]["mdg_defects"].empty:
        try:
            mdg_defects = data["defect_patterns"]["mdg_defects"].copy()
            
            # Group by Line and MDG to get total defects
            line_mdg_summary = mdg_defects.groupby(["Line", "Máy"])["Count"].sum().reset_index()
            
            # Create bar chart
            fig = px.bar(
                line_mdg_summary,
                x="Máy",
                y="Count",
                color="Line",
                title="Phân tích lỗi theo MDG và Line",
                labels={"Máy": "MDG (Máy)", "Count": "Số lỗi"},
                barmode="group"
            )
            
            # Update layout
            fig.update_layout(
                height=400,
                margin=dict(l=40, r=40, t=40, b=40)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display top MDG-defect combinations
            st.markdown("#### Những tổ hợp MDG-Loại lỗi phổ biến nhất")
            
            # Group by Line, MDG, and Defect code
            top_mdg_defects = mdg_defects.sort_values("Count", ascending=False).head(10)
            
            # Create a styled dataframe
            st.dataframe(top_mdg_defects, use_container_width=True, height=250)
            
        except Exception as e:
            st.error(f"Lỗi trong phân tích MDG: {str(e)}")
    else:
        st.warning("⚠️ Dữ liệu phân tích MDG không có sẵn")

# Page 2: Customer Complaint Analysis
with tab2:
    st.markdown('<div class="sub-header">Tổng quan khiếu nại khách hàng</div>', unsafe_allow_html=True)
    
    # Check if complaint dataframe is empty
    if filtered_complaint_df.empty:
        st.warning("⚠️ Không có dữ liệu khiếu nại để phân tích")
    else:
        # Key metrics row
        comp_col1, comp_col2, comp_col3, comp_col4 = st.columns(4)
        
        with comp_col1:
            if "Mã ticket" in filtered_complaint_df.columns:
                total_complaints = filtered_complaint_df["Mã ticket"].nunique()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Tổng số khiếu nại</div>
                    <div class="metric-value">{total_complaints}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Thiếu cột 'Mã ticket'")
        
        with comp_col2:
            if "SL pack/ cây lỗi" in filtered_complaint_df.columns:
                total_defective_packs = filtered_complaint_df["SL pack/ cây lỗi"].sum()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Số lượng gói lỗi</div>
                    <div class="metric-value">{total_defective_packs:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Thiếu cột 'SL pack/ cây lỗi'")
        
        with comp_col3:
            if "Tỉnh" in filtered_complaint_df.columns:
                total_provinces = filtered_complaint_df["Tỉnh"].nunique()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Số tỉnh/thành bị ảnh hưởng</div>
                    <div class="metric-value">{total_provinces}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Thiếu cột 'Tỉnh'")
        
        with comp_col4:
            if "Tên lỗi" in filtered_complaint_df.columns:
                total_defect_types = filtered_complaint_df["Tên lỗi"].nunique()
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Số loại lỗi được báo cáo</div>
                    <div class="metric-value">{total_defect_types}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("Thiếu cột 'Tên lỗi'")
        
        # Complaint Analysis
        st.markdown('<div class="sub-header">Phân tích khiếu nại</div>', unsafe_allow_html=True)
        
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
                    
                    # Create horizontal bar chart with improved styling
                    fig = go.Figure()
                    
                    # Add bars for complaints
                    fig.add_trace(go.Bar(
                        y=product_complaints["Tên sản phẩm"],
                        x=product_complaints["Mã ticket"],
                        name="Số khiếu nại",
                        orientation='h',
                        marker=dict(
                            color=product_complaints["Mã ticket"],
                            colorscale='Reds',
                            line=dict(width=1, color='black')
                        ),
                        text=product_complaints["Mã ticket"],
                        textposition="outside",
                        textfont=dict(size=12)
                    ))
                    
                    # Update layout with better styling
                    fig.update_layout(
                        title={
                            'text': "Top 10 sản phẩm có nhiều khiếu nại nhất",
                            'y':0.9,
                            'x':0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'
                        },
                        xaxis_title="Số lượng khiếu nại",
                        yaxis_title="Sản phẩm",
                        height=400,
                        margin=dict(l=20, r=20, t=60, b=40),
                        plot_bgcolor='rgba(240,240,240,0.5)',
                        xaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200,200,200,0.5)'
                        ),
                        yaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200,200,200,0.5)'
                        )
                    )
                    
                    # Add value labels on the bars
                    for i in range(len(product_complaints)):
                        fig.add_annotation(
                            x=product_complaints["Mã ticket"].iloc[i] + 1,
                            y=i,
                            text=str(product_complaints["Mã ticket"].iloc[i]),
                            showarrow=False,
                            font=dict(color="black", size=12)
                        )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ khiếu nại theo sản phẩm: {str(e)}")
            else:
                st.warning("Thiếu cột cần thiết cho biểu đồ sản phẩm")
        
        with comp_col2:
            if "Tên lỗi" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by defect type
                    defect_complaints = filtered_complaint_df.groupby("Tên lỗi").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Calculate percentages
                    defect_complaints["Complaint %"] = (defect_complaints["Mã ticket"] / defect_complaints["Mã ticket"].sum() * 100).round(1)
                    
                    # Create improved pie chart
                    fig = go.Figure()
                    
                    # Add pie chart with improved styling
                    fig.add_trace(go.Pie(
                        labels=defect_complaints["Tên lỗi"],
                        values=defect_complaints["Mã ticket"],
                        hole=0.4,
                        textinfo="percent",
                        hoverinfo="label+value+percent",
                        textfont=dict(size=12),
                        marker=dict(
                            colors=px.colors.qualitative.Set3,
                            line=dict(color='white', width=2)
                        ),
                        pull=[0.05 if i == defect_complaints["Mã ticket"].idxmax() else 0 for i in range(len(defect_complaints))]
                    ))
                    
                    # Add a custom annotation in the center
                    fig.add_annotation(
                        text=f"Tổng số<br>{defect_complaints['Mã ticket'].sum():,.0f}",
                        font=dict(size=14, color="#1E3A8A", family="Arial", weight="bold"),
                        showarrow=False,
                        x=0.5,
                        y=0.5
                    )
                    
                    # Update layout with better styling
                    fig.update_layout(
                        title={
                            'text': "Phân tích khiếu nại theo loại lỗi",
                            'y':0.9,
                            'x':0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'
                        },
                        height=400,
                        margin=dict(l=20, r=20, t=60, b=40),
                        legend=dict(
                            orientation="v",
                            yanchor="middle",
                            y=0.5,
                            xanchor="left",
                            x=1.05,
                            font=dict(size=10)
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ khiếu nại theo loại lỗi: {str(e)}")
            else:
                st.warning("Thiếu cột cần thiết cho biểu đồ loại lỗi")
        
        # Complaint Timeline and Production Analysis
        st.markdown('<div class="sub-header">Phân tích xu hướng khiếu nại theo thời gian</div>', unsafe_allow_html=True)
        
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
                    
                    # Create figure - Changed to column chart instead of line chart
                    fig = go.Figure()
                    
                    # Add bars for complaints
                    fig.add_trace(go.Bar(
                        x=date_complaints["Production_Date"],
                        y=date_complaints["Mã ticket"],
                        name="Số khiếu nại",
                        marker_color='rgba(70, 130, 180, 0.8)',
                        text=date_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout with better styling
                    fig.update_layout(
                        title={
                            'text': "Xu hướng khiếu nại theo thời gian",
                            'y':0.9,
                            'x':0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'
                        },
                        xaxis_title="Ngày sản xuất",
                        yaxis_title="Số lượng khiếu nại",
                        height=400,
                        margin=dict(l=20, r=20, t=60, b=40),
                        plot_bgcolor='rgba(240,240,240,0.5)',
                        xaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200,200,200,0.5)'
                        ),
                        yaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200,200,200,0.5)'
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ xu hướng khiếu nại: {str(e)}")
            else:
                st.warning("Thiếu cột ngày cho biểu đồ xu hướng")
        
        with time_col2:
            if "Line" in filtered_complaint_df.columns and "Mã ticket" in filtered_complaint_df.columns:
                try:
                    # Group by line
                    line_complaints = filtered_complaint_df.groupby("Line").agg({
                        "Mã ticket": "nunique",
                        "SL pack/ cây lỗi": "sum"
                    }).reset_index()
                    
                    # Sort by line number
                    line_complaints = line_complaints.sort_values("Line")
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Add bars for complaints with adjusted scale for 8 lines
                    fig.add_trace(go.Bar(
                        x=line_complaints["Line"],
                        y=line_complaints["Mã ticket"],
                        name="Số khiếu nại",
                        marker_color='rgba(128, 0, 0, 0.8)',
                        text=line_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout with better styling and fixed scale for 8 lines
                    fig.update_layout(
                        title={
                            'text': "Khiếu nại theo Line sản xuất",
                            'y':0.9,
                            'x':0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'
                        },
                        xaxis_title="Line sản xuất",
                        yaxis_title="Số lượng khiếu nại",
                        height=400,
                        margin=dict(l=20, r=20, t=60, b=40),
                        plot_bgcolor='rgba(240,240,240,0.5)',
                        xaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200,200,200,0.5)',
                            categoryorder='array',
                            categoryarray=['1', '2', '3', '4', '5', '6', '7', '8']
                        ),
                        yaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200,200,200,0.5)',
                            range=[0, 8]  # Fixed scale from 0 to 8
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ khiếu nại theo line: {str(e)}")
            else:
                st.warning("Thiếu cột Line cho biểu đồ line")
        
        # Geographic Distribution of Complaints
        st.markdown('<div class="sub-header">Phân bố địa lý của khiếu nại</div>', unsafe_allow_html=True)
        
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
                    title="Top các tỉnh/thành theo số lượng khiếu nại",
                    labels={"Tỉnh": "Tỉnh/Thành", "Mã ticket": "Số lượng khiếu nại", "SL pack/ cây lỗi": "Số gói lỗi"},
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
                    <div class="insight-title">Phân tích địa lý</div>
                    <div class="insight-content">
                        <p>Top 5 tỉnh/thành chiếm {top_provinces['Percentage'].sum():.1f}% tổng số khiếu nại.</p>
                        <p>Tỉnh/thành cao nhất ({top_provinces.iloc[0]['Tỉnh']}) chiếm {top_provinces.iloc[0]['Percentage']:.1f}% tổng số khiếu nại.</p>
                        <p>Cân nhắc chương trình cải tiến chất lượng tại các khu vực này.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Lỗi tạo biểu đồ phân bố địa lý: {str(e)}")
        else:
            st.warning("Thiếu cột tỉnh/thành để phân tích địa lý")
        
        # Personnel Analysis
        st.markdown('<div class="sub-header">Phân tích nhân sự sản xuất</div>', unsafe_allow_html=True)
        
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
                        name="Số khiếu nại",
                        marker_color="purple",
                        text=qa_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Khiếu nại theo nhân viên QA",
                        xaxis_title="Nhân viên QA",
                        yaxis_title="Số lượng khiếu nại",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=80),
                        xaxis_tickangle=-45
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ khiếu nại theo QA: {str(e)}")
            else:
                st.warning("Thiếu cột QA cho phân tích nhân sự")
        
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
                        name="Số khiếu nại",
                        marker_color="darkred",
                        text=leader_complaints["Mã ticket"],
                        textposition="outside"
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Khiếu nại theo Trưởng ca",
                        xaxis_title="Trưởng ca",
                        yaxis_title="Số lượng khiếu nại",
                        height=400,
                        margin=dict(l=40, r=40, t=40, b=80),
                        xaxis_tickangle=-45
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi tạo biểu đồ khiếu nại theo trưởng ca: {str(e)}")
            else:
                st.warning("Thiếu cột trưởng ca cho phân tích nhân sự")
        
        # Complaint Details Table
        st.markdown('<div class="sub-header">Chi tiết khiếu nại</div>', unsafe_allow_html=True)
        
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
                st.warning("Không có dữ liệu khiếu nại để hiển thị")
        except Exception as e:
            st.error(f"Lỗi hiển thị chi tiết khiếu nại: {str(e)}")

# Page 3: Linking Internal and External Quality
with tab3:
    st.markdown('<div class="sub-header">Phân tích liên kết chất lượng trong và ngoài</div>', unsafe_allow_html=True)
    
    # Check if linked defects data is available
    if "linked_defects" in data and not data["linked_defects"].empty:
        # Key metrics row
        link_col1, link_col2, link_col3, link_col4 = st.columns(4)
        
        linked_df = data["linked_defects"].copy()
        
        with link_col1:
            total_linkages = len(linked_df)
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Tổng số liên kết</div>
                <div class="metric-value">{total_linkages}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with link_col2:
            avg_ratio = linked_df["Defect_to_Complaint_Ratio"].replace([float('inf'), -float('inf')], np.nan).mean()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Tỷ lệ trung bình Lỗi:Khiếu nại</div>
                <div class="metric-value">{avg_ratio:.1f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with link_col3:
            unique_defect_types = linked_df["Defect_Type"].nunique()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Số loại lỗi liên kết</div>
                <div class="metric-value">{unique_defect_types}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with link_col4:
            total_lines = linked_df["Line"].nunique()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Số Line bị ảnh hưởng</div>
                <div class="metric-value">{total_lines}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Defect to Complaint Ratio Analysis
        st.markdown('<div class="sub-header">Phân tích tỷ lệ Lỗi-Khiếu nại</div>', unsafe_allow_html=True)
        
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
                    title="Tỷ lệ Lỗi-Khiếu nại theo loại lỗi",
                    xaxis_title="Tỷ lệ (Lỗi nội bộ : Khiếu nại khách hàng)",
                    yaxis_title="Loại lỗi",
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add interpretation
                st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">Diễn giải tỷ lệ</div>
                    <div class="insight-content">
                        <p>Tỷ lệ cao hơn cho thấy nhiều lỗi nội bộ được phát hiện cho mỗi khiếu nại khách hàng.</p>
                        <p>Tỷ lệ thấp hơn cho thấy lỗi không được phát hiện hiệu quả trong quá trình sản xuất.</p>
                        <p><strong>{defect_type_ratios.iloc[-1]['Defect_Type']}</strong> có tỷ lệ cao nhất ({defect_type_ratios.iloc[-1]['Ratio']:.1f}), cho thấy phát hiện nội bộ hiệu quả.</p>
                        <p><strong>{defect_type_ratios.iloc[0]['Defect_Type']}</strong> có tỷ lệ thấp nhất ({defect_type_ratios.iloc[0]['Ratio']:.1f}), cần cải thiện phát hiện lỗi.</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Lỗi tạo biểu đồ phân tích tỷ lệ: {str(e)}")
        
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
                    title="Lỗi nội bộ và khiếu nại khách hàng theo Line"
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
                    name="Tỷ lệ 1:1"
                ))
                
                # Update layout
                fig.update_layout(
                    xaxis_title="Số lỗi nội bộ",
                    yaxis_title="Số khiếu nại khách hàng",
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Lỗi tạo biểu đồ tỷ lệ theo line: {str(e)}")
        
        # Timeline Analysis
        st.markdown('<div class="sub-header">Phân tích theo thời gian</div>', unsafe_allow_html=True)
        
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
                    name="Lỗi nội bộ",
                    mode="lines+markers",
                    line=dict(color="royalblue", width=2)
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Scatter(
                    x=date_analysis["Production_Date"],
                    y=date_analysis["Customer_Complaint_Count"],
                    name="Khiếu nại khách hàng",
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
                    name="Tỷ lệ Lỗi:Khiếu nại",
                    mode="lines",
                    line=dict(color="green", width=2, dash="dash")
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title="Lỗi nội bộ và khiếu nại khách hàng theo thời gian",
                xaxis_title="Ngày sản xuất",
                height=400,
                margin=dict(l=40, r=40, t=40, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02)
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Số lượng", secondary_y=False)
            fig.update_yaxes(title_text="Tỷ lệ Lỗi:Khiếu nại", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Calculate correlation
            correlation = date_analysis["Internal_Defect_Count"].corr(date_analysis["Customer_Complaint_Count"])
            
            # Add insight about correlation
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-title">Phân tích tương quan</div>
                <div class="insight-content">
                    <p>Tương quan giữa lỗi nội bộ và khiếu nại khách hàng là <strong>{correlation:.2f}</strong>.</p>
                    <p>{'Tương quan dương này cho thấy sự gia tăng lỗi nội bộ có liên quan đến sự gia tăng khiếu nại khách hàng, với độ trễ từ vài ngày đến vài tuần.' if correlation > 0 else 'Tương quan này cho thấy lỗi nội bộ và khiếu nại khách hàng có thể không liên quan trực tiếp hoặc có độ trễ đáng kể giữa vấn đề sản xuất và phản hồi của khách hàng.'}</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Lỗi tạo biểu đồ phân tích thời gian: {str(e)}")
        
        # Detection Effectiveness Analysis
        st.markdown('<div class="sub-header">Phân tích hiệu quả phát hiện lỗi</div>', unsafe_allow_html=True)
        
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
            fig.add_vline(x=75, line_dash="dash", line_color="orange", annotation_text="75% (Chấp nhận được)")
            fig.add_vline(x=90, line_dash="dash", line_color="green", annotation_text="90% (Xuất sắc)")
            
            # Update layout
            fig.update_layout(
                title="Hiệu quả phát hiện lỗi nội bộ theo loại lỗi",
                xaxis_title="Hiệu quả phát hiện (%)",
                yaxis_title="Loại lỗi",
                height=400,
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(range=[0, 100])
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Identify poor detection areas
            poor_detection = effectiveness_df[effectiveness_df["Detection_Effectiveness"] < 75]
            
            if not poor_detection.empty:
    # Build a list of <li> elements for each defect type with low detection
    low_items = ''.join([
        f"<li><strong>{row['Defect_Type']}</strong>: {row['Detection_Effectiveness']}% hiệu quả</li>"
        for _, row in poor_detection.iterrows()
    ])

    st.markdown(f"""
    <div class="warning-card">
        <div class="warning-title">Khu vực phát hiện lỗi kém</div>
        <div class="insight-content">
            <p>Các loại lỗi sau đây có hiệu quả phát hiện dưới 75%, cho thấy cơ hội cải tiến đáng kể:</p>
            <ul>
                {low_items}
            </ul>
            <p>Cân nhắc triển khai các cải tiến nhắm mục tiêu trong phương pháp phát hiện cho các loại lỗi này.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Lỗi tạo phân tích hiệu quả phát hiện: {str(e)}")
    else:
        st.warning("""
        ⚠️ Không có dữ liệu lỗi liên kết. Điều này có thể do:
        
        1. Không đủ dữ liệu lịch sử để thiết lập kết nối
        2. Không khớp mã lỗi giữa dữ liệu nội bộ và dữ liệu khách hàng
        3. Vấn đề tích hợp dữ liệu
        
        Vui lòng đảm bảo cả dữ liệu AQL và khiếu nại đều có sẵn và được định dạng đúng.
        """)

# Footer with dashboard information
st.markdown("""
<div style="text-align: center; padding: 15px; margin-top: 30px; border-top: 1px solid #eee;">
    <p style="color: #555; font-size: 0.9rem;">
        Báo cáo chất lượng CF MMB | Được tạo bởi Phòng Đảm bảo Chất lượng
    </p>
</div>
""", unsafe_allow_html=True)

# Auto-refresh mechanism
if auto_refresh:
    time.sleep(300)  # Wait for 5 minutes
    st.experimental_rerun()
