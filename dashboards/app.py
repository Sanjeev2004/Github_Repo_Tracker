import os
import time
import logging
import sys
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as ob
import streamlit as st

# Setup page config
st.set_page_config(
    page_title="Real-Time GitHub Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark Mode custom CSS for premium look
st.markdown("""
    <style>
        /* Main page background */
        .stApp {
            background-color: #0e1117;
            color: #ffffff;
        }
        /* Metric card container */
        div[data-testid="metric-container"] {
            background-color: #1e222b;
            border: 1px solid #2e3440;
            padding: 15px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
            transition: transform 0.2s;
        }
        div[data-testid="metric-container"]:hover {
            transform: translateY(-2px);
            border-color: #4c566a;
        }
        /* Custom header */
        .pipeline-header {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(90deg, #ff7e5f, #feb47b, #86a8e7, #91eae4);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-size: 2.8rem;
            font-weight: 800;
            margin-bottom: 0.2rem;
        }
        .pipeline-status {
            display: inline-flex;
            align-items: center;
            background-color: #1b2b24;
            color: #4ade80;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            border: 1px solid #22c55e;
            margin-bottom: 2rem;
        }
        .status-dot {
            height: 8px;
            width: 8px;
            background-color: #22c55e;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            animation: pulse 1.5s infinite;
        }
        @keyframes pulse {
            0% { transform: scale(0.9); opacity: 0.6; }
            50% { transform: scale(1.2); opacity: 1; }
            100% { transform: scale(0.9); opacity: 0.6; }
        }
    </style>
""", unsafe_allow_html=True)

# DB connection config
DB_NAME = os.getenv("POSTGRES_DB", "github_events")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

@st.cache_resource(ttl=300)
def get_db_connection():
    """Establish and cache a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=5,
            application_name="github-events-dashboard"
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to PostgreSQL: {e}")
        return None

def fetch_data() -> pd.DataFrame:
    """Fetch aggregated repository activity from PostgreSQL."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
        
    query = """
        SELECT 
            window_start, 
            window_end, 
            repository_name, 
            event_type, 
            event_count 
        FROM repository_activity
        ORDER BY window_start DESC;
    """
    try:
        df = pd.read_sql_query(query + " LIMIT 100000", conn)
        # Convert date columns to timestamps
        df['window_start'] = pd.to_datetime(df['window_start'])
        df['window_end'] = pd.to_datetime(df['window_end'])
        return df
    except Exception as e:
        st.error(f"Error fetching data from PostgreSQL: {e}")
        # Clear cached connection if we fail
        st.cache_resource.clear()
        return pd.DataFrame()

# Sidebar Setup
st.sidebar.image("https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png", width=60)
st.sidebar.title("Configuration")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", min_value=2, max_value=60, value=5)

st.sidebar.markdown("---")
st.sidebar.markdown("### System Architecture")
st.sidebar.info(
    "1. **GitHub API**: Polls public events\n"
    "2. **Kafka Topic**: Ingests and buffers JSON payloads\n"
    "3. **PySpark Structured Streaming**: Watermarks events, computes sliding windows, and saves metrics\n"
    "4. **PostgreSQL**: Stores aggregations dynamically\n"
    "5. **Streamlit**: Renders live visualizations"
)

# Header Row
st.markdown('<div class="pipeline-header">Real-Time GitHub Activity Pipeline</div>', unsafe_allow_html=True)
st.markdown('<div class="pipeline-status"><span class="status-dot"></span>Pipeline Streaming Active</div>', unsafe_allow_html=True)

# Fetch latest data
df = fetch_data()

if df.empty:
    st.warning("No data found in PostgreSQL yet. Ensure that the ingestion producer and Spark streaming job are running successfully.")
    time.sleep(2)
    st.rerun()

# ----------------- FILTERS -----------------
# Get unique list of event types & repos for filters
all_event_types = sorted(df['event_type'].unique().tolist())
all_repos = sorted(df['repository_name'].unique().tolist())

col_f1, col_f2 = st.columns(2)
with col_f1:
    selected_event_types = st.multiselect("Filter Event Types", all_event_types, default=all_event_types)
with col_f2:
    search_repo = st.text_input("Search Repository Name", "")

# Apply Filters
filtered_df = df[df['event_type'].isin(selected_event_types)]
if search_repo:
    filtered_df = filtered_df[filtered_df['repository_name'].str.contains(search_repo, case=False)]

# ----------------- KPI CARDS -----------------
total_events = filtered_df['event_count'].sum()
total_repos = filtered_df['repository_name'].nunique()
latest_window = filtered_df['window_end'].max()

col1, col2, col3 = st.columns(3)
col1.metric("Total Streamed Events", f"{total_events:,}")
col2.metric("Tracked Repositories", f"{total_repos:,}")
col3.metric("Latest Activity Window", latest_window.strftime('%H:%M:%S') if not pd.isnull(latest_window) else "N/A")

st.markdown("---")

# ----------------- CHARTS ROW -----------------
c_left, c_right = st.columns(2)

with c_left:
    st.markdown("### 🔥 Top 10 Most Active Repositories")
    # Group by repo and sum counts
    repo_groups = filtered_df.groupby('repository_name')['event_count'].sum().reset_index()
    top_repos = repo_groups.sort_values(by='event_count', ascending=False).head(10)
    
    if not top_repos.empty:
        fig_repo = px.bar(
            top_repos,
            x='event_count',
            y='repository_name',
            orientation='h',
            labels={'event_count': 'Total Events', 'repository_name': 'Repository'},
            color='event_count',
            color_continuous_scale='plasma',
            template='plotly_dark'
        )
        fig_repo.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=0, r=0, t=10, b=10),
            height=350,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_repo, use_container_width=True)
    else:
        st.info("No repository activity matches the filters.")

with c_right:
    st.markdown("### 📊 Distribution of Event Types")
    event_groups = filtered_df.groupby('event_type')['event_count'].sum().reset_index()
    
    if not event_groups.empty:
        fig_events = px.pie(
            event_groups,
            values='event_count',
            names='event_type',
            hole=0.4,
            template='plotly_dark',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_events.update_layout(
            margin=dict(l=0, r=0, t=10, b=10),
            height=350,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_events, use_container_width=True)
    else:
        st.info("No events match the filters.")

# ----------------- TIMELINE CHART -----------------
st.markdown("### 📈 Pipeline Activity Over Time (Timeline)")
timeline_groups = filtered_df.groupby(['window_end', 'event_type'])['event_count'].sum().reset_index()

if not timeline_groups.empty:
    fig_time = px.line(
        timeline_groups,
        x='window_end',
        y='event_count',
        color='event_type',
        labels={'window_end': 'Time Window', 'event_count': 'Event Frequency', 'event_type': 'Event'},
        template='plotly_dark'
    )
    fig_time.update_layout(
        margin=dict(l=0, r=0, t=10, b=10),
        height=300,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='#2e3440')
    )
    st.plotly_chart(fig_time, use_container_width=True)
else:
    st.info("No time series data available for the chosen filters.")

# ----------------- RAW DATA TABLE -----------------
with st.expander("🔍 View Raw Database Aggregations"):
    st.dataframe(
        filtered_df.style.format({
            "event_count": "{:,}"
        }),
        use_container_width=True
    )

# Sleep and auto-refresh the dashboard
time.sleep(refresh_rate)
st.rerun()
