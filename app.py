import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import gspread
import os
import json
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

# Function to authenticate with OAuth
def authenticate():
    try:
        st.markdown("### 🔍 Authentication Status")
        auth_expander = st.expander("Authentication Details")
        
        with auth_expander:
            creds = None
            # Check if token.json exists in local environment
            if os.path.exists('token.json'):
                st.success("✅ Found token.json file")
                creds = Credentials.from_authorized_user_file('token.json', [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ])
            # Check if token is in Streamlit secrets
            elif 'GOOGLE_TOKEN_JSON' in st.secrets:
                st.success("✅ Found GOOGLE_TOKEN_JSON in Streamlit secrets")
                token_info = json.loads(st.secrets["GOOGLE_TOKEN_JSON"])
                creds = Credentials.from_authorized_user_info(token_info, [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ])
            else:
                st.error("❌ No authentication credentials found")
                st.info("Please add GOOGLE_TOKEN_JSON to Streamlit secrets or token.json to your repository")
                return None
            
            # Refresh token if expired
            if creds and creds.expired and creds.refresh_token:
                st.info("🔄 Token expired, refreshing...")
                creds.refresh(Request())
                # In local environment, save the refreshed token
                if os.path.exists('token.json'):
                    with open('token.json', 'w') as token:
                        token.write(creds.to_json())
                        st.success("✅ Token refreshed and saved")
            
            if creds:
                st.success("✅ Authentication successful")
                return gspread.authorize(creds)
            else:
                st.error("❌ Authentication failed")
                return None
                
    except Exception as e:
        st.error(f"❌ Authentication error: {str(e)}")
        return None

# Load and process data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    try:
        # Authenticate
        client = authenticate()
        if client is None:
            return pd.DataFrame()
        
        # Open the Google Sheet
        sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit"
        sheet_id = sheet_url.split('/d/')[1].split('/')[0]
        
        try:
            spreadsheet = client.open_by_key(sheet_id)
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
            
            # Basic data cleaning
            # Convert date columns to datetime if needed
            if "Ngày SX" in df.columns:
                df["Production_Month"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
                df["Production_Month"] = df["Production_Month"].dt.strftime("%m/%Y")
            
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
    
    st.markdown("1. Authentication failure - Check that your GOOGLE_TOKEN_JSON is properly configured in Streamlit secrets")
    st.markdown("2. Worksheet not found - Check that 'Integrated_Data' exists in your spreadsheet")
    st.markdown("3. Sheet permissions - Make sure your Google Sheet is shared with your personal Google account")
    st.markdown("4. Data format - Ensure the data in your spreadsheet is properly formatted")
    
    # Add a button to attempt raw data fetch
    if st.button("Attempt Raw Data Fetch"):
        try:
            client = authenticate()
            if client:
                sheet_id = "1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4"
                spreadsheet = client.open_by_key(sheet_id)
                st.write(f"Found spreadsheet: {spreadsheet.title}")
                
                # List all worksheets
                worksheets = spreadsheet.worksheets()
                st.write(f"Found {len(worksheets)} worksheets:")
                for ws in worksheets:
                    st.write(f"- {ws.title} (rows: {ws.row_count}, cols: {ws.col_count})")
                
                # Try to get the first few rows from the first worksheet
                first_worksheet = spreadsheet.get_worksheet(0)
                values = first_worksheet.get_all_values()
                st.write(f"First worksheet '{first_worksheet.title}' has {len(values)} rows")
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

# Date filter - if you have a date range
if "Production_Month" in df.columns:
    production_months = ["All"] + sorted(df["Production_Month"].unique().tolist())
    selected_month = st.sidebar.selectbox("Select Production Month", production_months)
    
    if selected_month != "All":
        filtered_df = df[df["Production_Month"] == selected_month]
    else:
        filtered_df = df
else:
    filtered_df = df

# Product filter
if "Tên sản phẩm" in df.columns:
    products = ["All"] + sorted(df["Tên sản phẩm"].unique().tolist())
    selected_product = st.sidebar.selectbox("Select Product", products)
    
    if selected_product != "All":
        filtered_df = filtered_df[filtered_df["Tên sản phẩm"] == selected_product]

# Line filter
if "Line" in df.columns:
    lines = ["All"] + sorted(filtered_df["Line"].unique().tolist())
    selected_line = st.sidebar.selectbox("Select Production Line", lines)
    
    if selected_line != "All":
        filtered_df = filtered_df[filtered_df["Line"] == selected_line]

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
    total_complaints = filtered_df["Mã ticket"].nunique()
    
    # Sum of defective packs
    total_defective_packs = filtered_df["SL pack/ cây lỗi"].sum()
    
    # Display KPIs
    st.metric("Total Complaints", f"{total_complaints}")
    st.metric("Total Defective Packs", f"{total_defective_packs:,.0f}")

# Complaints by Product
with col2:
    st.subheader("Complaints by Product")
    
    if "Tên sản phẩm" in filtered_df.columns:
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

# Second row
col3, col4 = st.columns(2)

# Complaints by Defect Type
with col3:
    st.subheader("Complaints by Defect Type")
    
    if "Tên lỗi" in filtered_df.columns:
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

# Complaints by Line
with col4:
    st.subheader("Complaints by Production Line")
    
    if "Line" in filtered_df.columns:
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

# Third row
col5, col6 = st.columns(2)

# Complaints by Machine (MDG)
with col5:
    st.subheader("Complaints by Machine (MDG)")
    
    if "Máy" in filtered_df.columns and "Line" in filtered_df.columns:
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

# Complaints by Production Month
with col6:
    st.subheader("Complaints by Production Month")
    
    if "Production_Month" in filtered_df.columns:
        month_counts = filtered_df.groupby("Production_Month")["Mã ticket"].nunique().reset_index()
        month_counts.columns = ["Production Month", "Complaints"]
        
        # Sort by date
        month_counts["Sort_Date"] = pd.to_datetime(month_counts["Production Month"], format="%m/%Y")
        month_counts = month_counts.sort_values("Sort_Date")
        
        fig = px.line(
            month_counts, 
            x="Production Month", 
            y="Complaints",
            markers=True,
        )
        fig.update_layout(xaxis_title="Production Month", yaxis_title="Number of Complaints")
        st.plotly_chart(fig, use_container_width=True)

# Fourth row
col7, col8 = st.columns(2)

# Complaints by QA
with col7:
    st.subheader("Complaints by QA Personnel")
    
    if "QA" in filtered_df.columns:
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

# Complaints by Shift Leader
with col8:
    st.subheader("Complaints by Shift Leader")
    
    if "Tên Trưởng ca" in filtered_df.columns:
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

# Detailed complaint data table
st.subheader("Detailed Complaint Data")
st.dataframe(filtered_df, use_container_width=True)

# Add an auto-refresh mechanism
if st.sidebar.checkbox("Enable Auto-Refresh (30s)", value=False):
    st.empty()
    time.sleep(30)  # Wait for 30 seconds
    st.experimental_rerun()
