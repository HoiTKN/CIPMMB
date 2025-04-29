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
from complaint_agent import HuggingFaceComplaintAgent

# Set page configuration with improved styling
st.set_page_config(
    page_title="AI-Enhanced Customer Complaint Dashboard",
    page_icon="🧠",
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
    .ai-insight-card {
        background-color: #f0f7ff;
        border-left: 5px solid #3b82f6;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
    }
    .ai-insight-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1e40af;
        margin-bottom: 8px;
    }
    .ai-insight-content {
        color: #334155;
        font-size: 0.95rem;
    }
    .anomaly-card {
        background-color: #fff1f2;
        border-left: 5px solid #e11d48;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
    }
    .anomaly-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #be123c;
        margin-bottom: 8px;
    }
    .hypothesis-card {
        background-color: #f0fdf4;
        border-left: 5px solid #22c55e;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 15px;
    }
    .hypothesis-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #15803d;
        margin-bottom: 8px;
    }
</style>
""", unsafe_allow_html=True)

# Title and description
st.markdown('<div class="main-header">AI-Enhanced Customer Complaint Dashboard</div>', unsafe_allow_html=True)
st.markdown("Advanced analytics and AI insights for FMCG production complaint management")

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
            
            # Create knowledge base for AI agent
            create_knowledge_base(df)
            
            # Hide connection status after successful load
            connection_status.empty()
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error accessing spreadsheet: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        return pd.DataFrame()

def create_knowledge_base(df):
    """Create knowledge base for AI agent"""
    try:
        # Create structured knowledge base
        complaints_data = df.to_dict('records')
        
        knowledge_base = {
            "complaints": complaints_data,
            "metadata": {
                "last_updated": datetime.now().isoformat(),
                "total_complaints": len(complaints_data),
                "date_range": {
                    "start": str(df['Ngày SX'].min()) if "Ngày SX" in df.columns and not pd.isna(df['Ngày SX'].min()) else None,
                    "end": str(df['Ngày SX'].max()) if "Ngày SX" in df.columns and not pd.isna(df['Ngày SX'].max()) else None
                },
                "products": df['Tên sản phẩm'].unique().tolist() if "Tên sản phẩm" in df.columns else [],
                "defect_types": df['Tên lỗi'].unique().tolist() if "Tên lỗi" in df.columns else [],
                "lines": df['Line'].unique().tolist() if "Line" in df.columns else []
            }
        }
        
        # Save knowledge base to file
        with open('complaint_knowledge_base.json', 'w', encoding='utf-8') as f:
            json.dump(knowledge_base, f, ensure_ascii=False, indent=2)
            
        return True
    except Exception as e:
        st.error(f"Error creating knowledge base: {str(e)}")
        return False

# Initialize AI agent
@st.cache_resource
def initialize_ai_agent():
    """Initialize the AI agent with the preferred model"""
    try:
        # Use Mistral 7B by default
        model_name = "mistralai/Mistral-7B-Instruct-v0.2"
        
        # Initialize agent
        agent = HuggingFaceComplaintAgent(model_name=model_name)
        return agent
    except Exception as e:
        st.error(f"Error initializing AI agent: {str(e)}")
        return None

# Get AI insights
def get_ai_insights(agent, insight_type="patterns", days_back=30):
    """Get insights from AI agent"""
    if not agent:
        return None
    
    try:
        if insight_type == "patterns":
            insights = json.loads(agent.identify_patterns(days_back=days_back))
        elif insight_type == "anomalies":
            insights = json.loads(agent.detect_anomalies())
        elif insight_type == "root_causes":
            insights = json.loads(agent.generate_root_cause_hypotheses())
        elif insight_type == "sampling_plan":
            insights = json.loads(agent.recommend_sampling_plan())
        else:
            insights = None
            
        return insights
    except Exception as e:
        st.error(f"Error getting AI insights ({insight_type}): {str(e)}")
        return None

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
    
    # AI Analysis settings
    st.markdown("<h3 style='text-align: center; color: #1E3A8A;'>AI Analysis</h3>", unsafe_allow_html=True)
    
    # Initialize AI agent
    agent = initialize_ai_agent()
    
    if not agent:
        st.warning("⚠️ AI agent not initialized. Check logs for errors.")
    else:
        # Analysis timeframe
        analysis_days = st.slider("Analysis Timeframe (days)", min_value=7, max_value=90, value=30, step=1)
        
        # AI analysis options
        analysis_options = st.multiselect(
            "Select Analysis Types",
            ["Pattern Identification", "Anomaly Detection", "Root Cause Analysis", "Sampling Plan"],
            default=["Pattern Identification"]
        )
        
        # Run AI analysis button
        if st.button("🧠 Run AI Analysis", use_container_width=True):
            with st.spinner("AI agent analyzing complaint data..."):
                # Create progress bar
                progress_bar = st.sidebar.progress(0)
                
                # Run selected analyses
                results = {}
                num_analyses = len(analysis_options)
                
                for i, analysis_type in enumerate(analysis_options):
                    if analysis_type == "Pattern Identification":
                        results["patterns"] = get_ai_insights(agent, "patterns", days_back=analysis_days)
                    elif analysis_type == "Anomaly Detection":
                        results["anomalies"] = get_ai_insights(agent, "anomalies")
                    elif analysis_type == "Root Cause Analysis":
                        results["root_causes"] = get_ai_insights(agent, "root_causes")
                    elif analysis_type == "Sampling Plan":
                        results["sampling_plan"] = get_ai_insights(agent, "sampling_plan")
                    
                    # Update progress
                    progress_bar.progress((i + 1) / num_analyses)
                
                # Save results to session state
                st.session_state.ai_results = results
                
                # Clear progress bar
                progress_bar.empty()
                
                st.sidebar.success("AI analysis completed!")
    
    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Show last update time
    st.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Add auto-refresh checkbox
    auto_refresh = st.checkbox("⏱️ Enable Auto-Refresh (30s)", value=False)

# Main dashboard layout
# First row - KPIs and AI Summary
st.markdown('<div class="sub-header">Complaint Overview & AI Insights</div>', unsafe_allow_html=True)

# Two-column layout for KPIs and Executive Summary
kpi_col, ai_col = st.columns([1, 2])

# KPIs in the left column
with kpi_col:
    # Create a 3x1 layout for KPIs
    with st.container():
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

# AI Executive Summary in the right column
with ai_col:
    if 'ai_results' in st.session_state and st.session_state.ai_results:
        # Check if pattern analysis is available
        if "patterns" in st.session_state.ai_results and st.session_state.ai_results["patterns"]:
            patterns = st.session_state.ai_results["patterns"]
            
            if "error" not in patterns and "ai_analysis" in patterns:
                st.markdown(f"""
                <div class="ai-insight-card">
                    <div class="ai-insight-title">AI Pattern Analysis</div>
                    <div class="ai-insight-content">
                        {patterns["ai_analysis"]}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
        # Check if anomaly detection is available
        if "anomalies" in st.session_state.ai_results and st.session_state.ai_results["anomalies"]:
            anomalies = st.session_state.ai_results["anomalies"]
            
            if "error" not in anomalies and "ai_analysis" in anomalies:
                st.markdown(f"""
                <div class="anomaly-card">
                    <div class="anomaly-title">AI Anomaly Detection</div>
                    <div class="ai-insight-content">
                        {anomalies["ai_analysis"]}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("Run AI analysis from the sidebar to get intelligent insights about complaint patterns, anomalies, root causes, and sampling recommendations.")

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

# Third row - Production metrics with AI insights
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
            
            # Add AI insights if available
            if 'ai_results' in st.session_state and "anomalies" in st.session_state.ai_results:
                anomalies = st.session_state.ai_results["anomalies"]
                if "anomalies" in anomalies and "high_defect_lines" in anomalies["anomalies"]:
                    high_defect_lines = anomalies["anomalies"]["high_defect_lines"]
                    if high_defect_lines:
                        st.markdown("#### 🧠 AI-Detected Line Anomalies")
                        for line, details in high_defect_lines.items():
                            st.markdown(f"""
                            <div class="anomaly-card">
                                <div class="ai-insight-content">
                                    <strong>Line {line}</strong>: {details["mean_defects"]:.1f} defects per complaint 
                                    (Z-score: {details["z_score"]:.2f})
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
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

# AI Analysis section for Root Cause and Sampling Plan
if 'ai_results' in st.session_state:
    ai_results = st.session_state.ai_results
    
    # Root Cause Analysis Tab
    if "root_causes" in ai_results and ai_results["root_causes"]:
        st.markdown('<div class="sub-header">AI Root Cause Analysis</div>', unsafe_allow_html=True)
        
        root_causes = ai_results["root_causes"]
        if "error" not in root_causes and "root_cause_hypotheses" in root_causes:
            st.markdown(f"""
            <div class="hypothesis-card">
                <div class="hypothesis-title">Root Cause Hypotheses</div>
                <div class="ai-insight-content">
                    {root_causes["root_cause_hypotheses"]}
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # Sampling Plan Tab
    if "sampling_plan" in ai_results and ai_results["sampling_plan"]:
        st.markdown('<div class="sub-header">AI-Recommended Sampling Plan</div>', unsafe_allow_html=True)
        
        sampling_plan = ai_results["sampling_plan"]
        if "error" not in sampling_plan and "recommended_sampling_plan" in sampling_plan:
            st.markdown(f"""
            <div class="ai-insight-card">
                <div class="ai-insight-title">Recommended QA Sampling Plan</div>
                <div class="ai-insight-content">
                    {sampling_plan["recommended_sampling_plan"]}
                </div>
            </div>
            """, unsafe_allow_html=True)

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

# Footer with information about the AI agent
if agent:
    st.markdown("""
    <div style="text-align: center; padding: 15px; margin-top: 30px; border-top: 1px solid #eee;">
        <p style="color: #555; font-size: 0.9rem;">
            This dashboard is enhanced with AI analytics using the Hugging Face Mistral-7B model.
            The AI agent analyzes complaint data to detect patterns, anomalies, and provide actionable insights.
        </p>
    </div>
    """, unsafe_allow_html=True)

# Add an auto-refresh mechanism
if auto_refresh:
    time.sleep(30)  # Wait for 30 seconds
    st.experimental_rerun()
