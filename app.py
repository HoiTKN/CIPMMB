import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import gspread
import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Set page configuration with improved styling
st.set_page_config(
    page_title="Customer Complaint Dashboard",
    page_icon="⚠️",
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
</style>
""", unsafe_allow_html=True)

# Title and description
st.markdown('<div class="main-header">Customer Complaint Dashboard</div>', unsafe_allow_html=True)
st.markdown("Real-time dashboard for monitoring customer complaints in FMCG production")

# Define the scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authentication function - using the same approach as sheets_integration.py
def authenticate():
    """Authentication using OAuth token"""
    try:
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

# Function to load and process data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    try:
        # Authenticate and connect to Google Sheets
        gc = authenticate()
        
        if gc is None:
            st.error("❌ Failed to authenticate with Google Sheets")
            return pd.DataFrame()
        
        # Open the Google Sheet by URL
        sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit"
        # Extract sheet key from URL
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        
        # Open the spreadsheet and get the worksheet
        try:
            spreadsheet = gc.open_by_key(sheet_key)
            connection_status = st.empty()
            connection_status.success(f"✅ Successfully opened spreadsheet: {spreadsheet.title}")
            
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
                    df["Production_Month"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
                    df["Production_Month"] = df["Production_Month"].dt.strftime("%m/%Y")
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
            st.error(f"❌ Error accessing spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        return pd.DataFrame()

# Load the data
df = load_data()

# Check if dataframe is empty
if df.empty:
    st.warning("⚠️ No data available. Please check your Google Sheet connection.")
    
    # Show additional debug information if dataframe is empty
    st.markdown("### Additional Debug Information")
    st.markdown("Your dataframe is empty. Here are possible reasons:")
    
    st.markdown("1. Authentication failure - Check that your token.json or GOOGLE_TOKEN_JSON is properly configured")
    st.markdown("2. Worksheet not found - Check that 'Integrated_Data' exists in your spreadsheet")
    st.markdown("3. Sheet permissions - Make sure your Google Sheet is shared with your Google account")
    st.markdown("4. Data format - Ensure the data in your spreadsheet is properly formatted")
    
    # Add a button to attempt raw data fetch
    if st.button("Attempt Raw Data Fetch"):
        try:
            gc = authenticate()
            if gc:
                sheet_key = "1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4"
                spreadsheet = gc.open_by_key(sheet_key)
                st.write(f"Found spreadsheet: {spreadsheet.title}")
                
                # List all worksheets
                worksheets = spreadsheet.worksheets()
                st.write(f"Available worksheets in the spreadsheet:")
                for ws in worksheets:
                    st.write(f"- {ws.title} (rows: {ws.row_count}, cols: {ws.col_count})")
                
                # Try to get the first few rows from the first worksheet
                first_ws = spreadsheet.get_worksheet(0)
                values = first_ws.get_all_values()
                st.write(f"First worksheet '{first_ws.title}' has {len(values)} rows")
                st.write("First few rows:")
                for i, row in enumerate(values[:5]):
                    st.write(f"Row {i}: {row}")
            else:
                st.error("Could not authenticate")
        except Exception as e:
            st.error(f"Error in raw data fetch: {e}")
    
    st.stop()

# Create a sidebar for filters
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #1E3A8A;'>Filters</h2>", unsafe_allow_html=True)
    
    # Initialize filtered_df
    filtered_df = df.copy()
    
    # Date filter - if you have a date range
    if "Production_Month" in df.columns and not df["Production_Month"].isna().all():
        try:
            production_months = ["All"] + sorted(df["Production_Month"].dropna().unique().tolist())
            selected_month = st.selectbox("📅 Select Production Month", production_months)
            
            if selected_month != "All":
                filtered_df = filtered_df[filtered_df["Production_Month"] == selected_month]
        except Exception as e:
            st.warning(f"Error in month filter: {e}")
    
    # Product filter
    if "Tên sản phẩm" in df.columns and not df["Tên sản phẩm"].isna().all():
        try:
            products = ["All"] + sorted(df["Tên sản phẩm"].dropna().unique().tolist())
            selected_product = st.selectbox("🍜 Select Product", products)
            
            if selected_product != "All":
                filtered_df = filtered_df[filtered_df["Tên sản phẩm"] == selected_product]
        except Exception as e:
            st.warning(f"Error in product filter: {e}")
    
    # Line filter
    if "Line" in df.columns and not df["Line"].isna().all():
        try:
            # Ensure Line is treated as string for consistent comparison
            lines = ["All"] + sorted(filtered_df["Line"].astype(str).dropna().unique().tolist())
            selected_line = st.selectbox("🏭 Select Production Line", lines)
            
            if selected_line != "All":
                filtered_df = filtered_df[filtered_df["Line"].astype(str) == selected_line]
        except Exception as e:
            st.warning(f"Error in line filter: {e}")
    
    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Show last update time
    st.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Add auto-refresh checkbox
    auto_refresh = st.checkbox("⏱️ Enable Auto-Refresh (30s)", value=False)

# Clear any residual connection messages
st.markdown("")

# Main dashboard layout
# First row - KPIs
st.markdown('<div class="sub-header">Complaint Overview</div>', unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

# KPIs
with col1:
    if "Mã ticket" in filtered_df.columns:
        total_complaints = filtered_df["Mã ticket"].nunique()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Complaints</div>
            <div class="metric-value">{total_complaints}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("Missing 'Mã ticket' column")

with col2:
    if "SL pack/ cây lỗi" in filtered_df.columns:
        total_defective_packs = filtered_df["SL pack/ cây lỗi"].sum()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Defective Packs</div>
            <div class="metric-value">{total_defective_packs:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("Missing 'SL pack/ cây lỗi' column")

with col3:
    if "Tỉnh" in filtered_df.columns:
        total_provinces = filtered_df["Tỉnh"].nunique()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Affected Provinces</div>
            <div class="metric-value">{total_provinces}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("Missing 'Tỉnh' column")

# Second row - Top charts
st.markdown('<div class="sub-header">Product & Defect Analysis</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

# Complaints by Product - improved visualization
with col1:
    if "Tên sản phẩm" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            product_counts = filtered_df.groupby("Tên sản phẩm")["Mã ticket"].nunique().reset_index()
            product_counts.columns = ["Product", "Complaints"]
            product_counts = product_counts.sort_values("Complaints", ascending=False).head(10)
            
            # Improved horizontal bar chart for better label readability
            fig = px.bar(
                product_counts, 
                y="Product", 
                x="Complaints",
                color="Complaints",
                color_continuous_scale="Reds",
                orientation='h',
                title="Top 10 Products by Complaints",
                text="Complaints"
            )
            
            # Improve layout
            fig.update_layout(
                height=400,
                xaxis_title="Number of Complaints",
                yaxis_title="",
                font=dict(size=12),
                margin=dict(l=20, r=20, t=40, b=20),
                yaxis=dict(tickfont=dict(size=10))
            )
            
            # Add data labels
            fig.update_traces(textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in product chart: {e}")
    else:
        st.warning("Missing columns required for product chart")

# Complaints by Defect Type
with col2:
    if "Tên lỗi" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            defect_counts = filtered_df.groupby("Tên lỗi")["Mã ticket"].nunique().reset_index()
            defect_counts.columns = ["Defect Type", "Complaints"]
            defect_counts = defect_counts.sort_values("Complaints", ascending=False)
            
            # Calculate percentages for better context
            total = defect_counts['Complaints'].sum()
            defect_counts['Percentage'] = (defect_counts['Complaints'] / total * 100).round(1)
            defect_counts['Label'] = defect_counts['Defect Type'] + ' (' + defect_counts['Percentage'].astype(str) + '%)'
            
            # Improved pie chart with percentage labels
            fig = px.pie(
                defect_counts, 
                names="Label", 
                values="Complaints",
                title="Complaints by Defect Type",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            
            # Improve layout
            fig.update_layout(
                height=400,
                font=dict(size=12),
                legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                margin=dict(l=20, r=20, t=40, b=80)
            )
            
            # Improve trace display
            fig.update_traces(textposition='inside', textinfo='percent+label')
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in defect chart: {e}")
    else:
        st.warning("Missing columns required for defect chart")

# Third row - Production metrics
st.markdown('<div class="sub-header">Production Analysis</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

# Complaints by Line
with col1:
    if "Line" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            line_counts = filtered_df.groupby("Line")["Mã ticket"].nunique().reset_index()
            line_counts.columns = ["Production Line", "Complaints"]
            line_counts = line_counts.sort_values("Complaints", ascending=False)
            
            # Improved bar chart
            fig = px.bar(
                line_counts, 
                x="Production Line", 
                y="Complaints",
                color="Complaints",
                color_continuous_scale="Blues",
                title="Complaints by Production Line",
                text="Complaints"
            )
            
            # Improve layout
            fig.update_layout(
                height=400,
                xaxis_title="Production Line",
                yaxis_title="Number of Complaints",
                font=dict(size=12),
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis=dict(tickangle=-45, tickfont=dict(size=10))
            )
            
            # Add data labels
            fig.update_traces(textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in line chart: {e}")
    else:
        st.warning("Missing columns required for line chart")

# Complaints by Production Month
with col2:
    if "Production_Month" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            month_counts = filtered_df.groupby("Production_Month")["Mã ticket"].nunique().reset_index()
            month_counts.columns = ["Production Month", "Complaints"]
            
            # Sort by date
            try:
                month_counts["Sort_Date"] = pd.to_datetime(month_counts["Production Month"], format="%m/%Y")
                month_counts = month_counts.sort_values("Sort_Date")
            except:
                # If date sorting fails, use the original order
                pass
            
            # Improved line chart
            fig = px.line(
                month_counts, 
                x="Production Month", 
                y="Complaints",
                markers=True,
                title="Complaints Trend by Production Month",
                text="Complaints"
            )
            
            # Improve layout
            fig.update_layout(
                height=400,
                xaxis_title="Production Month",
                yaxis_title="Number of Complaints",
                font=dict(size=12),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            # Customize line
            fig.update_traces(
                line=dict(width=3, color='royalblue'),
                marker=dict(size=10, color='royalblue'),
                textposition='top center'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in month chart: {e}")
    else:
        st.warning("Missing Production_Month column required for month chart")

# Fourth row - Machine and Personnel
st.markdown('<div class="sub-header">Machine & Personnel Analysis</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

# Complaints by Machine (MDG)
with col1:
    if "Máy" in filtered_df.columns and "Line" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            # Create a combined column for line-machine
            filtered_df["Line_Machine"] = filtered_df["Line"].astype(str) + " - " + filtered_df["Máy"].astype(str)
            
            machine_counts = filtered_df.groupby("Line_Machine")["Mã ticket"].nunique().reset_index()
            machine_counts.columns = ["Line-Machine", "Complaints"]
            machine_counts = machine_counts.sort_values("Complaints", ascending=False).head(10)  # Top 10
            
            # Improved horizontal bar chart
            fig = px.bar(
                machine_counts, 
                y="Line-Machine", 
                x="Complaints",
                color="Complaints",
                color_continuous_scale="Greens",
                orientation='h',
                title="Top 10 Machine-Line Combinations by Complaints",
                text="Complaints"
            )
            
            # Improve layout
            fig.update_layout(
                height=400,
                xaxis_title="Number of Complaints",
                yaxis_title="",
                font=dict(size=12),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            # Add data labels
            fig.update_traces(textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in machine chart: {e}")
    else:
        st.warning("Missing columns required for machine chart")

# Complaints by QA and Shift Leader
with col2:
    try:
        if "QA" in filtered_df.columns and "Tên Trưởng ca" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
            # Create a combined personnel analysis
            # QA Personnel Analysis
            qa_counts = filtered_df.groupby("QA")["Mã ticket"].nunique().reset_index()
            qa_counts.columns = ["Personnel", "Complaints"]
            qa_counts["Role"] = "QA"
            
            # Shift Leader Analysis
            leader_counts = filtered_df.groupby("Tên Trưởng ca")["Mã ticket"].nunique().reset_index()
            leader_counts.columns = ["Personnel", "Complaints"]
            leader_counts["Role"] = "Shift Leader"
            
            # Combine both dataframes
            personnel_counts = pd.concat([qa_counts, leader_counts])
            personnel_counts = personnel_counts.sort_values(["Role", "Complaints"], ascending=[True, False])
            
            # Improved grouped bar chart
            fig = px.bar(
                personnel_counts, 
                x="Personnel", 
                y="Complaints",
                color="Role",
                barmode="group",
                title="Complaints by Personnel",
                text="Complaints",
                color_discrete_map={"QA": "purple", "Shift Leader": "darkred"}
            )
            
            # Improve layout
            fig.update_layout(
                height=400,
                xaxis_title="Personnel",
                yaxis_title="Number of Complaints",
                font=dict(size=12),
                margin=dict(l=20, r=20, t=40, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            # Add data labels
            fig.update_traces(textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Missing columns required for personnel charts")
    except Exception as e:
        st.error(f"Error in personnel charts: {e}")

# Detailed complaint data table with improved styling
st.markdown('<div class="sub-header">Detailed Complaint Data</div>', unsafe_allow_html=True)

# Format the dataframe for better display
if not filtered_df.empty:
    try:
        display_df = filtered_df.copy()
        
        # Format date columns if they exist
        date_columns = ["Ngày SX", "Ngày tiếp nhận"]
        for col in date_columns:
            if col in display_df.columns:
                try:
                    display_df[col] = pd.to_datetime(display_df[col], errors='coerce').dt.strftime('%d/%m/%Y')
                except:
                    pass
        
        # Show the dataframe with pagination
        st.dataframe(display_df.style.set_properties(**{'text-align': 'left'}), use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error displaying data table: {e}")
else:
    st.warning("No data available to display")

# Add an auto-refresh mechanism
if auto_refresh:
    time.sleep(30)  # Wait for 30 seconds
    st.experimental_rerun()
