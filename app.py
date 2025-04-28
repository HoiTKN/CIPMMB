import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
from streamlit_gsheets import GSheetsConnection

# Set page configuration
st.set_page_config(
    page_title="Customer Complaint Dashboard",
    page_icon="⚠️",
    layout="wide"
)

# Title and description
st.title("Customer Complaint Dashboard")
st.markdown("Real-time dashboard for monitoring customer complaints in FMCG production")

# Create a connection to Google Sheets
# Note: You need to create a .streamlit/secrets.toml file with your credentials
conn = st.connection("gsheets", type=GSheetsConnection)

# Function to load and process data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    # Read the data from Google Sheets
    df = conn.read()
    
    # Basic data cleaning
    # Convert date columns to datetime if needed
    if "Ngày SX" in df.columns:
        df["Production_Month"] = pd.to_datetime(df["Ngày SX"], format="%d/%m/%Y", errors='coerce')
        df["Production_Month"] = df["Production_Month"].dt.strftime("%m/%Y")
    
    # Make sure numeric columns are properly typed
    if "SL pack/ cây lỗi" in df.columns:
        df["SL pack/ cây lỗi"] = pd.to_numeric(df["SL pack/ cây lỗi"], errors='coerce').fillna(0)
    
    return df

# Load the data
df = load_data()

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
        filtered_df["Line_Machine"] = filtered_df["Line"] + " - " + filtered_df["Máy"]
        
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
