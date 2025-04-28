import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import gspread
import os
import json
import sys
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Set page configuration
st.set_page_config(
    page_title="Customer Complaint Dashboard",
    page_icon="⚠️",
    layout="wide"
)

# Title and description
st.title("Customer Complaint Dashboard")
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
        debug_expander = st.expander("Authentication Debugging")
        
        with debug_expander:
            st.markdown("#### OAuth Authentication Status")
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
            st.success(f"✅ Successfully opened spreadsheet: {spreadsheet.title}")
            
            # Try to get the "Integrated_Data" worksheet
            try:
                worksheet = spreadsheet.worksheet('Integrated_Data')
                st.success(f"✅ Found worksheet: Integrated_Data")
            except gspread.exceptions.WorksheetNotFound:
                # Fall back to first worksheet if Integrated_Data doesn't exist
                worksheet = spreadsheet.get_worksheet(0)
                st.warning(f"⚠️ 'Integrated_Data' worksheet not found. Using '{worksheet.title}' instead.")
            
            # Get all records
            data = worksheet.get_all_records()
            st.success(f"✅ Retrieved {len(data)} records from worksheet")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Display available columns for debugging
            if not df.empty:
                st.write("Available columns:", df.columns.tolist())
            
            # Basic data cleaning
            # Convert date columns to datetime if needed
            if "Ngày SX" in df.columns:
                try:
                    df["Production_Month"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
                    df["Production_Month"] = df["Production_Month"].dt.strftime("%m/%Y")
                    st.success("✅ Successfully created Production_Month column")
                except Exception as e:
                    st.warning(f"⚠️ Could not process date column: {e}")
            
            # Make sure numeric columns are properly typed
            if "SL pack/ cây lỗi" in df.columns:
                df["SL pack/ cây lỗi"] = pd.to_numeric(df["SL pack/ cây lỗi"], errors='coerce').fillna(0)
            
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
st.sidebar.header("Filters")

# Initialize filtered_df
filtered_df = df.copy()

# Date filter - if you have a date range
if "Production_Month" in df.columns and not df["Production_Month"].isna().all():
    try:
        production_months = ["All"] + sorted(df["Production_Month"].dropna().unique().tolist())
        selected_month = st.sidebar.selectbox("Select Production Month", production_months)
        
        if selected_month != "All":
            filtered_df = filtered_df[filtered_df["Production_Month"] == selected_month]
    except Exception as e:
        st.sidebar.warning(f"Error in month filter: {e}")

# Product filter
if "Tên sản phẩm" in df.columns and not df["Tên sản phẩm"].isna().all():
    try:
        products = ["All"] + sorted(df["Tên sản phẩm"].dropna().unique().tolist())
        selected_product = st.sidebar.selectbox("Select Product", products)
        
        if selected_product != "All":
            filtered_df = filtered_df[filtered_df["Tên sản phẩm"] == selected_product]
    except Exception as e:
        st.sidebar.warning(f"Error in product filter: {e}")

# Line filter
if "Line" in df.columns and not df["Line"].isna().all():
    try:
        lines = ["All"] + sorted(filtered_df["Line"].dropna().unique().tolist())
        selected_line = st.sidebar.selectbox("Select Production Line", lines)
        
        if selected_line != "All":
            filtered_df = filtered_df[filtered_df["Line"] == selected_line]
    except Exception as e:
        st.sidebar.warning(f"Error in line filter: {e}")

# Refresh button
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

# Show last update time
st.sidebar.markdown(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Remove debug information when data is successfully loaded
st.markdown("---")

# Main dashboard layout
col1, col2 = st.columns(2)

# KPIs in the first row
with col1:
    st.subheader("Complaint Summary")
    
    # Count unique tickets
    if "Mã ticket" in filtered_df.columns:
        total_complaints = filtered_df["Mã ticket"].nunique()
        st.metric("Total Complaints", f"{total_complaints}")
    else:
        st.warning("No 'Mã ticket' column found")
    
    # Sum of defective packs
    if "SL pack/ cây lỗi" in filtered_df.columns:
        total_defective_packs = filtered_df["SL pack/ cây lỗi"].sum()
        st.metric("Total Defective Packs", f"{total_defective_packs:,.0f}")
    else:
        st.warning("No 'SL pack/ cây lỗi' column found")

# Complaints by Product
with col2:
    st.subheader("Complaints by Product")
    
    if "Tên sản phẩm" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            product_counts = filtered_df.groupby("Tên sản phẩm")["Mã ticket"].nunique().reset_index()
            product_counts.columns = ["Product", "Complaints"]
            product_counts = product_counts.sort_values("Complaints", ascending=False)
            
            fig = px.bar(
                product_counts, 
                x="Product", 
                y="Complaints",
                color="Complaints",
                color_continuous_scale="Reds",
            )
            fig.update_layout(xaxis_title="Product", yaxis_title="Number of Complaints")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in product chart: {e}")
    else:
        st.warning("Missing columns required for product chart")

# Second row
col3, col4 = st.columns(2)

# Complaints by Defect Type
with col3:
    st.subheader("Complaints by Defect Type")
    
    if "Tên lỗi" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            defect_counts = filtered_df.groupby("Tên lỗi")["Mã ticket"].nunique().reset_index()
            defect_counts.columns = ["Defect Type", "Complaints"]
            defect_counts = defect_counts.sort_values("Complaints", ascending=False)
            
            fig = px.pie(
                defect_counts, 
                names="Defect Type", 
                values="Complaints",
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in defect chart: {e}")
    else:
        st.warning("Missing columns required for defect chart")

# Complaints by Line
with col4:
    st.subheader("Complaints by Production Line")
    
    if "Line" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            line_counts = filtered_df.groupby("Line")["Mã ticket"].nunique().reset_index()
            line_counts.columns = ["Production Line", "Complaints"]
            line_counts = line_counts.sort_values("Complaints", ascending=False)
            
            fig = px.bar(
                line_counts, 
                x="Production Line", 
                y="Complaints",
                color="Complaints",
                color_continuous_scale="Blues",
            )
            fig.update_layout(xaxis_title="Production Line", yaxis_title="Number of Complaints")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in line chart: {e}")
    else:
        st.warning("Missing columns required for line chart")

# Third row
col5, col6 = st.columns(2)

# Complaints by Machine (MDG)
with col5:
    st.subheader("Complaints by Machine (MDG)")
    
    if "Máy" in filtered_df.columns and "Line" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            # Create a combined column for line-machine
            filtered_df["Line_Machine"] = filtered_df["Line"].astype(str) + " - " + filtered_df["Máy"].astype(str)
            
            machine_counts = filtered_df.groupby("Line_Machine")["Mã ticket"].nunique().reset_index()
            machine_counts.columns = ["Line-Machine", "Complaints"]
            machine_counts = machine_counts.sort_values("Complaints", ascending=False).head(10)  # Top 10
            
            fig = px.bar(
                machine_counts, 
                x="Line-Machine", 
                y="Complaints",
                color="Complaints",
                color_continuous_scale="Greens",
            )
            fig.update_layout(xaxis_title="Line-Machine", yaxis_title="Number of Complaints")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in machine chart: {e}")
    else:
        st.warning("Missing columns required for machine chart")

# Complaints by Production Month
with col6:
    st.subheader("Complaints by Production Month")
    
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
            
            fig = px.line(
                month_counts, 
                x="Production Month", 
                y="Complaints",
                markers=True,
            )
            fig.update_layout(xaxis_title="Production Month", yaxis_title="Number of Complaints")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in month chart: {e}")
    else:
        st.warning("Missing Production_Month column required for month chart")

# Fourth row
col7, col8 = st.columns(2)

# Complaints by QA
with col7:
    st.subheader("Complaints by QA Personnel")
    
    if "QA" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            qa_counts = filtered_df.groupby("QA")["Mã ticket"].nunique().reset_index()
            qa_counts.columns = ["QA Personnel", "Complaints"]
            qa_counts = qa_counts.sort_values("Complaints", ascending=False)
            
            fig = px.bar(
                qa_counts, 
                x="QA Personnel", 
                y="Complaints",
                color="Complaints",
                color_continuous_scale="Purples",
            )
            fig.update_layout(xaxis_title="QA Personnel", yaxis_title="Number of Complaints")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in QA chart: {e}")
    else:
        st.warning("Missing columns required for QA chart")

# Complaints by Shift Leader
with col8:
    st.subheader("Complaints by Shift Leader")
    
    if "Tên Trưởng ca" in filtered_df.columns and "Mã ticket" in filtered_df.columns:
        try:
            leader_counts = filtered_df.groupby("Tên Trưởng ca")["Mã ticket"].nunique().reset_index()
            leader_counts.columns = ["Shift Leader", "Complaints"]
            leader_counts = leader_counts.sort_values("Complaints", ascending=False)
            
            fig = px.bar(
                leader_counts, 
                x="Shift Leader", 
                y="Complaints",
                color="Complaints",
                color_continuous_scale="Oranges",
            )
            fig.update_layout(xaxis_title="Shift Leader", yaxis_title="Number of Complaints")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error in Shift Leader chart: {e}")
    else:
        st.warning("Missing columns required for Shift Leader chart")

# Detailed complaint data table
st.subheader("Detailed Complaint Data")
st.dataframe(filtered_df, use_container_width=True)

# Add an auto-refresh mechanism
if st.sidebar.checkbox("Enable Auto-Refresh (30s)", value=False):
    st.empty()
    time.sleep(30)  # Wait for 30 seconds
    st.experimental_rerun()
