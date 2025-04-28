import streamlit as st
import pandas as pd

# Read data from GitHub (raw files)
@st.cache_data(ttl=300)
def load_data():
    github_csv_url = "https://raw.githubusercontent.com/yourusername/reponame/main/data.csv"
    return pd.read_csv(github_csv_url)

df = load_data()

# Continue with your existing dashboard code
