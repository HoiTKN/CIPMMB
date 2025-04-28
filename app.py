import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

# Set page configuration
st.set_page_config(
    page_title="Customer Complaint Dashboard",
    page_icon="⚠️",
    layout="wide"
)

# Title and description
st.title("Customer Complaint Dashboard")
st.markdown("Real-time dashboard for monitoring customer complaints in FMCG production")

# Debug section - will show on the main page for easier troubleshooting
st.markdown("### 🔍 Debug Information")
debug_expander = st.expander("Authentication Debugging")

with debug_expander:
    st.markdown("#### Secrets Check")
    if 'GOOGLE_CLIENT_SECRET' in st.secrets:
        st.success("✅ GOOGLE_CLIENT_SECRET exists in Streamlit secrets")
        # Try to display some info about the secret without showing sensitive data
        try:
            secret_content = json.loads(st.secrets["GOOGLE_CLIENT_SECRET"])
            safe_display = {
                "type": secret_content.get("type", "Not found"),
                "project_id": secret_content.get("project_id", "Not found"),
                "client_email": secret_content.get("client_email", "Not found").split("@")[0] + "@..." if secret_content.get("client_email") else "Not found",
                "has_private_key": "Yes" if secret_content.get("private_key") else "No"
            }
            st.json(safe_display)
        except Exception as e:
            st.error(f"Error parsing secret: {e}")
    else:
        st.error("❌ GOOGLE_CLIENT_SECRET not found in Streamlit secrets")
    
    st.markdown("#### Worksheet Check")
    sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit?gid=1495122288#gid=1495122288"
    sheet_key = sheet_url.split('/d/')[1].split('/')[0]
    st.write(f"Trying to access sheet key: {sheet_key}")
    
    st.markdown("#### List Worksheets")
    if st.button("List Available Worksheets in Spreadsheet"):
        try:
            # Use credentials stored in GitHub secrets
            if 'GOOGLE_CLIENT_SECRET' in st.secrets:
                # Create a credentials dictionary from the secret
                creds_dict = json.loads(st.secrets["GOOGLE_CLIENT_SECRET"])
                
                # Define the scope
                scope = ['https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive']
                
                # Create credentials from the dictionary
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
                client = gspread.authorize(creds)
                
                # Open the Google Sheet by key
                spreadsheet = client.open_by_key(sheet_key)
                
                # Get all worksheets
                worksheets = spreadsheet.worksheets()
                
                if worksheets:
                    st.success(f"✅ Found {len(worksheets)} worksheets in the spreadsheet")
                    for ws in worksheets:
                        st.write(f"- {ws.title} (rows: {ws.row_count}, cols: {ws.col_count})")
                else:
                    st.warning("No worksheets found in the spreadsheet")
            else:
                st.error("Cannot list worksheets: GOOGLE_CLIENT_SECRET not found")
        except Exception as e:
            st.error(f"Error listing worksheets: {e}")

# Function to connect to Google Sheets
def connect_to_sheets():
    try:
        # Check if we're running in Streamlit Cloud (in which case we need to use st.secrets)
        if 'GOOGLE_CLIENT_SECRET' in st.secrets:
            # Create a credentials dictionary from the secret
            creds_dict = json.loads(st.secrets["GOOGLE_CLIENT_SECRET"])
            
            # Define the scope
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            # Create credentials from the dictionary
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            
            # Open the Google Sheet by URL or key
            sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit?gid=1495122288#gid=1495122288"
            # Extract sheet key from URL
            sheet_key = sheet_url.split('/d/')[1].split('/')[0]
            
            # Open the spreadsheet and get the first worksheet
            # We'll try to get the "Integrated_Data" worksheet first, then fall back to the first sheet if that fails
            spreadsheet = client.open_by_key(sheet_key)
            
            try:
                worksheet = spreadsheet.worksheet('Integrated_Data')
            except gspread.exceptions.WorksheetNotFound:
                # If 'Integrated_Data' doesn't exist, get the first worksheet
                worksheet = spreadsheet.get_worksheet(0)
                st.warning(f"'Integrated_Data' worksheet not found. Using '{worksheet.title}' instead.")
            
            return worksheet
        # Fallback to local file (for local development)
        else:
            # Method 2: If you have a client_secret.json file in your repository
            if os.path.exists("client_secret.json"):
                creds_path = "client_secret.json"  # Path to your credentials file
                
                # Define the scope
                scope = ['https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive']
                
                # Authenticate
                creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
                client = gspread.authorize(creds)
                
                # Open the Google Sheet by URL or key
                sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit?gid=1495122288#gid=1495122288"
                # Extract sheet key from URL
                sheet_key = sheet_url.split('/d/')[1].split('/')[0]
                
                # Open the spreadsheet and the first worksheet
                spreadsheet = client.open_by_key(sheet_key)
                
                try:
                    worksheet = spreadsheet.worksheet('Integrated_Data')
                except gspread.exceptions.WorksheetNotFound:
                    # If 'Integrated_Data' doesn't exist, get the first worksheet
                    worksheet = spreadsheet.get_worksheet(0)
                    st.warning(f"'Integrated_Data' worksheet not found. Using '{worksheet.title}' instead.")
                
                return worksheet
            else:
                st.error("No authentication method available (neither Streamlit secrets nor client_secret.json file)")
                return None
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None

# Function to load and process data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    try:
        worksheet = connect_to_sheets()
        if worksheet is None:
            return pd.DataFrame()
            
        # Get all values
        data = worksheet.get_all_records()
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
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Load the data
df = load_data()

# Check if dataframe is empty
if df.empty:
    st.warning("No data available. Please check your Google Sheet connection.")
    
    # Show additional debug information if dataframe is empty
    st.markdown("### Additional Debug Information")
    st.markdown("Your dataframe is empty. Here are possible reasons:")
    
    st.markdown("1. Authentication failure - Check that your GOOGLE_CLIENT_SECRET is properly configured")
    st.markdown("2. Worksheet not found - Check that 'Integrated_Data' exists in your spreadsheet")
    st.markdown("3. Sheet permissions - Make sure your Google Sheet is shared with the service account email")
    st.markdown("4. Data format - Ensure the data in your spreadsheet is properly formatted")
    
    # Add a button to see raw attempt at fetching data
    if st.button("Attempt Raw Data Fetch"):
        try:
            worksheet = connect_to_sheets()
            if worksheet:
                raw_data = worksheet.get_all_values()
                st.write(f"Found worksheet: {worksheet.title}")
                st.write(f"Number of rows: {len(raw_data)}")
                st.write(f"Number of columns: {len(raw_data[0]) if raw_data else 0}")
                st.write("First few rows:")
                for i, row in enumerate(raw_data[:5]):
                    st.write(f"Row {i}: {row}")
            else:
                st.error("Could not connect to worksheet")
        except Exception as e:
            st.error(f"Error in raw data fetch: {e}")
    
    st.stop()

# Create a sidebar for filters
st.sidebar.header("Filters")

# Date filter - if you have a date range
if "Ngày SX" in df.columns:
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
