# dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from ingest_sbb import fetch_station_data
from streamlit_autorefresh import st_autorefresh

# ================================
# --- Load external CSS ---
# ================================
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ================================
# --- Header ---
# ================================
st.markdown("""
<div class="header-container">
    <h2>🚆 Train Delay Dashboard</h2>
    <p>Real-time train delays · Data refreshes every 5 minutes</p>
</div>
""", unsafe_allow_html=True)

# ================================
# --- Railway selection ---
# ================================
railway = st.radio(
    "Select Railway:",
    options=["SBB (Switzerland)", "Deutsche Bahn (Germany)"],
    index=0
)

# Auto-refresh every 5 minutes
st_autorefresh(interval=300_000, limit=None, key="train_delay_autorefresh")

# ================================
# --- Helper functions ---
# ================================
def section_start():
    st.markdown('<div class="section-container">', unsafe_allow_html=True)

def section_end():
    st.markdown('</div>', unsafe_allow_html=True)

def kpi_block(avg, delayed, maximum):
    col1, col2, col3 = st.columns(3)
    col1.metric("Average Delay (min)", f"{avg:.1f}")
    col2.metric("Delayed Trains", f"{delayed}")
    col3.metric("Max Delay (min)", f"{maximum}")

# --- New: delay color function ---
def delay_color(val):
    try:
        v = float(val)
    except:
        return "#FFFFFF"  # white by default
    if v == 0:
        return "#FFFFFF"   # white — no delay
    elif v <= 5:
        return "#F2F2F2"   # light gray — minor delay
    else:
        return "#8B8B8B"   # gray — major delay

# ================================
# --- SBB Section ---
# ================================
if railway == "SBB (Switzerland)":
    st.header("🚄 SBB (Switzerland)")

    # Station selection
    swiss_cities = ["Zurich", "Geneva", "Bern", "Basel", "Lausanne",
                    "Lucerne", "St. Gallen", "Lugano", "Interlaken", "Winterthur"]
    station_name = st.selectbox("Select Station", swiss_cities)
    fetch_limit = st.number_input("Number of upcoming departures", min_value=5, max_value=50, value=15)
    fetch_station_data(station=station_name, limit=fetch_limit)

    # Info message
    section_start()
    st.markdown('<div class="custom-info">Data fetched automatically.</div>', unsafe_allow_html=True)
    section_end()

    # Load data
    conn = sqlite3.connect("sbb_data.db")
    df = pd.read_sql_query("""
        SELECT fetched_at, train_name, delay_minutes, station, category
        FROM stationboard
        WHERE station = ?
        ORDER BY fetched_at DESC
        LIMIT ?
    """, conn, params=(station_name, fetch_limit))

    df_history = pd.read_sql_query("""
        SELECT fetched_at, train_name, delay_minutes
        FROM stationboard
        WHERE station = ?
          AND fetched_at >= datetime('now', '-7 days')
        ORDER BY fetched_at ASC
    """, conn, params=(station_name,))

    total_rows = conn.execute("SELECT COUNT(*) FROM stationboard").fetchone()[0]
    conn.close()

    st.caption(f"📊 Records in database: {total_rows}")

    # Timestamps
    df['fetched_at'] = pd.to_datetime(df['fetched_at'])
    df_history['fetched_at'] = pd.to_datetime(df_history['fetched_at'])

    # Train type filter
    categories = df['category'].dropna().unique().tolist()
    selected_categories = st.multiselect("Select Train Types", options=categories, default=categories)
    if selected_categories:
        df = df[df['category'].isin(selected_categories)]

    # Last N hours
    hours_back = st.slider("Show data from last N hours", 1, 600, 240)
    df_filtered = df[df['fetched_at'] >= pd.Timestamp.now() - pd.Timedelta(hours=hours_back)]

    # KPI
    avg_delay = df_filtered['delay_minutes'].mean() if not df_filtered.empty else 0
    max_delay = df_filtered['delay_minutes'].max() if not df_filtered.empty else 0
    delayed_count = df_filtered[df_filtered['delay_minutes'] > 0].shape[0]
    kpi_block(avg_delay, delayed_count, max_delay)

    # Delayed trains table
    section_start()
    st.subheader("Delayed Trains")
    st.dataframe(
        df_filtered.sort_values(by='delay_minutes', ascending=False)
        .style.applymap(lambda v: f'background-color: {delay_color(v)}', subset=['delay_minutes'])
    )
    section_end()

    # Delay plot
    section_start()
    st.subheader(f"Train Delays Over Time at {station_name}")
    df_plot = df_filtered.tail(50)
    if not df_plot.empty:
        fig = px.line(df_plot, x='fetched_at', y='delay_minutes', color='train_name',
                      markers=True, hover_data=['train_name', 'delay_minutes', 'category'],
                      color_discrete_sequence=px.colors.sequential.Greys)
        fig.update_layout(
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            font=dict(color="#000000", family="Arial"),
            xaxis_title="Time of Fetch",
            yaxis_title="Delay (minutes)",
            legend_title="Train"
        )
        fig.update_traces(line=dict(width=1))
        st.plotly_chart(fig)
    section_end()

    # Predicted delays
    if not df_history.empty:
        section_start()
        st.subheader("Predicted Delays (last 7 days)")
        df_mean_delay = df_history.groupby('train_name')['delay_minutes'].mean().reset_index()
        df_mean_delay.rename(columns={'delay_minutes': 'predicted_delay'}, inplace=True)
        df_forecast = df.merge(df_mean_delay, on='train_name', how='left')
        st.dataframe(
            df_forecast[['train_name', 'delay_minutes', 'predicted_delay', 'category']]
            .style.background_gradient(subset=['predicted_delay'], cmap='Greys')
        )
        section_end()

# ================================
# --- Deutsche Bahn Section ---
# ================================
elif railway == "Deutsche Bahn (Germany)":
    st.header("🚄 Deutsche Bahn (Germany)")
    DB_FILE = "db_data.db"
    conn = sqlite3.connect(DB_FILE)
    stations = pd.read_sql_query(
        "SELECT DISTINCT station FROM stationboard ORDER BY station", conn
    )["station"].dropna().tolist()
    station_name_db = st.selectbox("Select DB Station", stations or ["No stations found"])
    limit = st.number_input("Number of recent departures", min_value=10, max_value=200, value=15, key="db_limit")

    df_db = pd.read_sql_query("""
        SELECT fetched_at, train_name, to_station, delay_minutes
        FROM stationboard
        WHERE station = ?
        ORDER BY datetime(fetched_at) DESC
        LIMIT ?
    """, conn, params=(station_name_db, limit))

    df_history_db = pd.read_sql_query("""
        SELECT train_name, delay_minutes
        FROM stationboard
        WHERE station = ? AND fetched_at >= date('now', '-30 day')
    """, conn, params=(station_name_db,))
    conn.close()

    df_db["fetched_at"] = pd.to_datetime(df_db["fetched_at"], errors="coerce")

    # KPI
    hours_back_db = st.slider("Show data from last N hours", 1, 600, 240, key="db_hours_back")
    df_filtered_db = df_db[df_db['fetched_at'] >= pd.Timestamp.now() - pd.Timedelta(hours=hours_back_db)]
    avg_delay_db = df_filtered_db['delay_minutes'].mean() if not df_filtered_db.empty else 0
    max_delay_db = df_filtered_db['delay_minutes'].max() if not df_filtered_db.empty else 0
    delayed_count_db = df_filtered_db[df_filtered_db['delay_minutes'] > 0].shape[0]
    kpi_block(avg_delay_db, delayed_count_db, max_delay_db)

    # Delayed trains table
    section_start()
    st.subheader(f"Delayed Trains at {station_name_db}")
    st.dataframe(
        df_filtered_db.sort_values(by='delay_minutes', ascending=False)
        .style.applymap(lambda v: f'background-color: {delay_color(v)}', subset=['delay_minutes'])
    )
    section_end()

    # Delay plot
    section_start()
    st.subheader(f"Train Delays at {station_name_db}")

    if not df_db.empty:
        # Copy data
        df_plot = df_db.copy()
        df_plot["delay_minutes"] = pd.to_numeric(df_plot["delay_minutes"], errors="coerce")
        df_plot = df_plot.dropna(subset=["delay_minutes"])

        # Aggregate by train_name (take the last delay)
        df_plot = df_plot.sort_values(by="fetched_at").groupby("train_name").tail(1)

        # Dynamic y-axis
        y_max = max(16, df_plot["delay_minutes"].max() + 2)

        # Build plot
        fig = px.bar(
            df_plot,
            x="train_name",
            y="delay_minutes",
            color="delay_minutes",
            color_continuous_scale="Greys",
            range_color=[0, 30],
            labels={"train_name": "Train", "delay_minutes": "Delay (min)"}
        )

        fig.update_layout(
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            font=dict(color="#000000", family="Arial"),
            xaxis_tickangle=-45,
            yaxis=dict(title="Delay (minutes)", range=[0, y_max], showgrid=True, gridcolor="#E5E5E5"),
            coloraxis_colorbar=dict(title="Delay (min)", tickvals=[0,5,10,15,20,25,30])
        )

        st.plotly_chart(fig, use_container_width=True)

    section_end()

    # Predicted delays
    if not df_history_db.empty:
        section_start()
        st.subheader("Predicted Delays (DB)")
        df_mean_delay_db = df_history_db.groupby("train_name")["delay_minutes"].mean().reset_index()
        df_mean_delay_db.rename(columns={"delay_minutes": "predicted_delay"}, inplace=True)
        np.random.seed(42)
        df_mean_delay_db["predicted_delay"] *= np.random.uniform(0.9, 1.1, len(df_mean_delay_db))
        df_mean_delay_db["predicted_delay"] = df_mean_delay_db["predicted_delay"].round(0).astype(int)
        df_forecast_db = df_db.merge(df_mean_delay_db, on="train_name", how="left")
        st.dataframe(
            df_forecast_db[["train_name", "to_station", "delay_minutes", "predicted_delay"]]
            .sort_values(by="predicted_delay", ascending=False)
            .style.background_gradient(subset=["predicted_delay"], cmap="Greys", vmin=0, vmax=30)
        )
        section_end()










