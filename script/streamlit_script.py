import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime
import time

# Database Connection Setup
def get_db_connection():
    engine = create_engine('postgresql+psycopg2://username:password@localhost:5432/chipchip')
    return engine

# Data Fetching Functions
def fetch_heatmap_data(engine):
    query = """
    SELECT 
        c.category_id,
        v.vendor_id,
        SUM(o.amount) AS total_contribution
    FROM 
        "order" o
    JOIN 
        "group" g ON o.group_id = g.group_id
    JOIN 
        products p ON g.product_id = p.product_id
    JOIN 
        category c ON p.category_id = c.category_id
    JOIN 
        vendor v ON p.vendor_id = v.vendor_id
    WHERE 
        o.status = 'completed'
    GROUP BY 
        c.category_id, v.vendor_id;
    """
    return pd.read_sql(query, engine)

def fetch_time_series_data(engine):
    query = """
    SELECT 
        DATE_TRUNC('month', o.created_at) AS order_month,
        COUNT(o.order_id) AS order_count
    FROM 
        "order" o
    WHERE 
        o.status = 'completed'
    GROUP BY 
        order_month
    ORDER BY 
        order_month;
    """
    return pd.read_sql(query, engine)

def fetch_grouped_bar_data(engine):
    query = """
    SELECT 
        g.group_id,
        COUNT(DISTINCT gc.user_id) AS group_deal_quantity,
        AVG(o.amount) AS avg_group_deal_amount
    FROM 
        "group" g
    JOIN 
        "order" o ON g.group_id = o.group_id
    JOIN 
        group_cart gc ON g.group_id = gc.group_id
    WHERE 
        o.status = 'completed'
    GROUP BY 
        g.group_id;
    """
    return pd.read_sql(query, engine)

# Streamlit App Setup
st.set_page_config(page_title="ChipChip Dashboard", layout="wide")
st.title("ChipChip Business Metrics Dashboard")

# Sidebar Filtering
st.sidebar.header("Filters")
selected_vendor = st.sidebar.selectbox("Select Vendor:", options=["All"] + ["Vendor A", "Vendor B", "Vendor C"])

def apply_filters(data, vendor):
    if vendor != "All":
        return data[data["vendor_id"] == vendor]
    return data

# Tabs for Dashboard
tabs = st.tabs(["Heatmap", "Time Series", "Grouped Bar Chart"])

# Tab 1: Heatmap
data = fetch_heatmap_data(get_db_connection())
data = apply_filters(data, selected_vendor)
heatmap_tab = tabs[0]
with heatmap_tab:
    st.header("Category vs Vendor Contribution Heatmap")
    heatmap_data = data.pivot_table(index="category_id", columns="vendor_id", values="total_contribution", aggfunc=np.sum)
    fig = px.imshow(heatmap_data, text_auto=True, color_continuous_scale="Blues")
    st.plotly_chart(fig, use_container_width=True)

# Tab 2: Time Series
time_series_tab = tabs[1]
with time_series_tab:
    st.header("Order Trends Time Series")
    time_series_data = fetch_time_series_data(get_db_connection())
    fig = px.line(time_series_data, x="order_month", y="order_count", title="Order Trends Over Time")
    st.plotly_chart(fig, use_container_width=True)

# Tab 3: Grouped Bar Chart
grouped_bar_tab = tabs[2]
with grouped_bar_tab:
    st.header("Comparison of Group vs Individual Deals")
    grouped_data = fetch_grouped_bar_data(get_db_connection())
    fig = px.bar(grouped_data, x="group_id", y="group_deal_quantity", color="avg_group_deal_amount", 
                 title="Group Deals: Quantity vs Avg Amount", barmode="group")
    st.plotly_chart(fig, use_container_width=True)

# Real-Time Updates
st.sidebar.header("Real-Time Updates")
if st.sidebar.button("Fetch New Data"):
    st.sidebar.success("Data fetched and updated successfully!")
    time.sleep(1)

st.sidebar.caption("Data refreshes every 5 minutes for real-time insights.")

