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
from plotly.subplots import make_subplots

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
    if "Tên sản phẩm" in filtered_df.columns and "Mã ticket" in filtered_df.columns and "SL pack/ cây lỗi" in filtered_df.columns:
        try:
            # Group by product and aggregate both metrics
            product_counts = filtered_df.groupby("Tên sản phẩm").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            product_counts.columns = ["Product", "Complaints", "Defective Packs"]
            product_counts = product_counts.sort_values("Complaints", ascending=False).head(10)
            
            # Create figure with two traces
            fig = go.Figure()
            
            # Add bars for complaints
            fig.add_trace(go.Bar(
                y=product_counts["Product"],
                x=product_counts["Complaints"],
                name="Complaints",
                orientation='h',
                marker_color='firebrick',
                text=product_counts["Complaints"],
                textposition="outside"
            ))
            
            # Add scatter markers for defective packs
            fig.add_trace(go.Scatter(
                y=product_counts["Product"],
                x=product_counts["Defective Packs"],
                name="Defective Packs",
                mode="markers",
                marker=dict(
                    size=12, 
                    color='royalblue',
                    symbol='diamond'
                ),
                text=product_counts["Defective Packs"].round(0).astype(int),
                textposition="middle right"
            ))
            
            # Update layout
            fig.update_layout(
                title="Top 10 Products by Complaints",
                height=400,
                xaxis_title="Count",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in product chart: {e}")
    else:
        st.warning("Missing columns required for product chart")

# Complaints by Defect Type
with col2:
    if "Tên lỗi" in filtered_df.columns and "Mã ticket" in filtered_df.columns and "SL pack/ cây lỗi" in filtered_df.columns:
        try:
            # Prepare data with both metrics
            defect_counts = filtered_df.groupby("Tên lỗi").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            defect_counts.columns = ["Defect Type", "Complaints", "Defective Packs"]
            defect_counts = defect_counts.sort_values("Complaints", ascending=False)
            
            # Calculate percentages
            defect_counts["Complaint %"] = (defect_counts["Complaints"] / defect_counts["Complaints"].sum() * 100).round(1)
            defect_counts["Label"] = defect_counts["Defect Type"] + " (" + defect_counts["Complaint %"].astype(str) + "%)"
            
            # Create figure
            fig = go.Figure()
            
            # Add pie chart
            fig.add_trace(go.Pie(
                labels=defect_counts["Label"],
                values=defect_counts["Complaints"],
                hole=0.4,
                textinfo="percent+label",
                insidetextorientation="radial",
                marker_colors=px.colors.qualitative.Bold
            ))
            
            # Add a custom annotation in the center showing defective packs
            fig.add_annotation(
                text=f"Total Defective Packs:<br>{defect_counts['Defective Packs'].sum():,.0f}",
                font=dict(size=12, color="darkblue", family="Arial"),
                showarrow=False,
                x=0.5,
                y=0.5
            )
            
            # Improve layout
            fig.update_layout(
                title="Complaints by Defect Type",
                height=400,
                font=dict(size=12),
                legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                margin=dict(l=20, r=20, t=40, b=80)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in defect chart: {e}")
    else:
        st.warning("Missing columns required for defect chart")

# Third row - Production metrics
st.markdown('<div class="sub-header">Production Analysis</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

# Complaints by Line with FIXED SCALING for the 8 production lines
with col1:
    if "Line" in filtered_df.columns and "Mã ticket" in filtered_df.columns and "SL pack/ cây lỗi" in filtered_df.columns:
        try:
            # Prepare data with both metrics
            line_counts = filtered_df.groupby("Line").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            line_counts.columns = ["Production Line", "Complaints", "Defective Packs"]
            line_counts = line_counts.sort_values("Complaints", ascending=False)
            
            # Create figure with two y-axes
            fig = go.Figure()
            
            # Add bars for complaints
            fig.add_trace(go.Bar(
                x=line_counts["Production Line"],
                y=line_counts["Complaints"],
                name="Complaints",
                marker_color="navy",
                text=line_counts["Complaints"],
                textposition="outside"
            ))
            
            # Add markers for defective packs
            fig.add_trace(go.Scatter(
                x=line_counts["Production Line"],
                y=line_counts["Defective Packs"],
                name="Defective Packs",
                mode="markers",
                marker=dict(
                    size=15,
                    color="orange",
                    symbol="star"
                ),
                text=line_counts["Defective Packs"].round(0).astype(int),
                hovertemplate="Line: %{x}<br>Defective Packs: %{y}<br>%{text}"
            ))
            
            # Update layout
            fig.update_layout(
                title="Complaints and Defective Packs by Production Line",
                height=400,
                xaxis=dict(
                    title="Production Line",
                    type='category',  # Fixed scale for discrete production lines
                    categoryorder='array',
                    categoryarray=line_counts["Production Line"]
                ),
                yaxis=dict(
                    title="Count"
                ),
                legend=dict(
                    orientation="h", 
                    yanchor="bottom", 
                    y=1.02, 
                    xanchor="right", 
                    x=1
                ),
                margin=dict(l=20, r=20, t=40, b=20),
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in line chart: {e}")
    else:
        st.warning("Missing columns required for line chart")

# Complaints by Production Month
with col2:
    if "Production_Month" in filtered_df.columns and "Mã ticket" in filtered_df.columns and "SL pack/ cây lỗi" in filtered_df.columns:
        try:
            # Prepare data with both metrics
            month_counts = filtered_df.groupby("Production_Month").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            month_counts.columns = ["Production Month", "Complaints", "Defective Packs"]
            
            # Sort by date
            try:
                month_counts["Sort_Date"] = pd.to_datetime(month_counts["Production Month"], format="%m/%Y")
                month_counts = month_counts.sort_values("Sort_Date")
                month_counts = month_counts.drop(columns=["Sort_Date"])
            except:
                pass
            
            # Create figure
            fig = go.Figure()
            
            # Add line for complaints
            fig.add_trace(go.Scatter(
                x=month_counts["Production Month"],
                y=month_counts["Complaints"],
                name="Complaints",
                mode="lines+markers",
                line=dict(color="royalblue", width=3),
                marker=dict(size=10, color="royalblue"),
                text=month_counts["Complaints"],
                textposition="top center"
            ))
            
            # Add bars for defective packs
            fig.add_trace(go.Bar(
                x=month_counts["Production Month"],
                y=month_counts["Defective Packs"],
                name="Defective Packs",
                marker_color="rgba(178, 34, 34, 0.7)",
                text=month_counts["Defective Packs"].round(0).astype(int),
                textposition="outside"
            ))
            
            # Update layout
            fig.update_layout(
                title="Complaints and Defective Packs by Production Month",
                height=400,
                xaxis=dict(
                    title="Production Month",
                    tickangle=0
                ),
                yaxis=dict(
                    title="Count"
                ),
                legend=dict(
                    orientation="h", 
                    yanchor="bottom", 
                    y=1.02, 
                    xanchor="right", 
                    x=1
                ),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in month chart: {e}")
    else:
        st.warning("Missing Production_Month column required for month chart")

# Fourth row - Machine and Personnel
st.markdown('<div class="sub-header">Machine & Personnel Analysis</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

# Complaints by Machine (MDG) - FIXED to prevent secondary_y error
with col1:
    if "Máy" in filtered_df.columns and "Line" in filtered_df.columns and "Mã ticket" in filtered_df.columns and "SL pack/ cây lỗi" in filtered_df.columns:
        try:
            # Create a combined column for line-machine
            filtered_df["Line_Machine"] = filtered_df["Line"].astype(str) + " - " + filtered_df["Máy"].astype(str)
            
            # Prepare data with both metrics
            machine_counts = filtered_df.groupby("Line_Machine").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            machine_counts.columns = ["Line-Machine", "Complaints", "Defective Packs"]
            machine_counts = machine_counts.sort_values("Complaints", ascending=False).head(10)  # Top 10
            
            # Create figure
            fig = go.Figure()
            
            # Add bars for complaints
            fig.add_trace(go.Bar(
                y=machine_counts["Line-Machine"],
                x=machine_counts["Complaints"],
                name="Complaints",
                orientation='h',
                marker_color="darkgreen",
                text=machine_counts["Complaints"],
                textposition="outside"
            ))
            
            # Add markers for defective packs
            fig.add_trace(go.Scatter(
                y=machine_counts["Line-Machine"],
                x=machine_counts["Defective Packs"],
                name="Defective Packs",
                mode="markers",
                marker=dict(
                    size=12,
                    color="lightgreen",
                    symbol="circle"
                ),
                text=machine_counts["Defective Packs"].round(0).astype(int)
            ))
            
            # Update layout
            fig.update_layout(
                title="Top 10 Machine-Line Combinations",
                height=400,
                xaxis_title="Count",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in machine chart: {e}")
    else:
        st.warning("Missing columns required for machine chart")

# Complaints by QA and Shift Leader
with col2:
    try:
        if "QA" in filtered_df.columns and "Tên Trưởng ca" in filtered_df.columns and "Mã ticket" in filtered_df.columns and "SL pack/ cây lỗi" in filtered_df.columns:
            # QA Personnel Analysis
            qa_counts = filtered_df.groupby("QA").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            qa_counts.columns = ["Personnel", "Complaints", "Defective Packs"]
            qa_counts["Role"] = "QA"
            
            # Shift Leader Analysis
            leader_counts = filtered_df.groupby("Tên Trưởng ca").agg({
                "Mã ticket": "nunique",
                "SL pack/ cây lỗi": "sum"
            }).reset_index()
            leader_counts.columns = ["Personnel", "Complaints", "Defective Packs"]
            leader_counts["Role"] = "Shift Leader"
            
            # Combine both dataframes
            personnel_counts = pd.concat([qa_counts, leader_counts])
            personnel_counts = personnel_counts.sort_values(["Role", "Complaints"], ascending=[True, False])
            
            # Create the figure
            fig = go.Figure()
            
            # Add bars for complaints
            fig.add_trace(go.Bar(
                x=personnel_counts["Personnel"],
                y=personnel_counts["Complaints"],
                name="Complaints",
                marker_color=personnel_counts["Role"].map({"QA": "purple", "Shift Leader": "darkred"}),
                text=personnel_counts["Complaints"],
                textposition="outside"
            ))
            
            # Add markers for defective packs
            fig.add_trace(go.Scatter(
                x=personnel_counts["Personnel"],
                y=personnel_counts["Defective Packs"],
                name="Defective Packs",
                mode="markers",
                marker=dict(
                    size=12,
                    color="gold",
                    symbol="diamond"
                ),
                text=personnel_counts["Defective Packs"].round(0).astype(int)
            ))
            
            # Update layout
            fig.update_layout(
                title="Complaints and Defective Packs by Personnel",
                height=400,
                xaxis_title="Personnel",
                yaxis_title="Count",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            
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
