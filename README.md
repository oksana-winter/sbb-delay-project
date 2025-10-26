> ⚠️ **Note:** This project is under development. Some features, especially for DB, are not yet implemented.

```markdown
# Train Delays Dashboard (SBB & Deutsche Bahn) ⚠️ In Development

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-Interactive-orange)
![Status](https://img.shields.io/badge/Status-In%20Development-yellow)

## Project Description
This project is an interactive **Streamlit** dashboard that tracks train delays in real-time for **Swiss Federal Railways (SBB)** and, in the future, **Deutsche Bahn (DB, Germany)**.  

- SBB data is fetched from the official **Open Data API**, stored in **SQLite**, and displayed as tables, KPIs, and interactive charts.  
- DB support is planned, with a skeleton connection to the **Timetables API**; currently, a placeholder is used due to API access restrictions.  

---

## Features

### 1. SBB (Switzerland)
- Select stations from major cities (Zurich, Geneva, Bern, etc.)
- Set the number of upcoming departures
- Filter by train type (IC, IR, S-Bahn, etc.)
- Filter by time (e.g., last N hours)
- KPIs:
  - Average delay (minutes)
  - Number of delayed trains
  - Maximum delay
- Table with color-coded delays
- Interactive delay chart over time (Plotly)
- Delay forecast based on historical data (7 days)

### 2. DB (Germany) – Placeholder
- Select stations from major cities (Berlin, Hamburg, Munich, etc.)
- Set the number of upcoming departures
- Informational message: “DB data is not connected yet”
- Future support for connecting the real Timetables API after access is granted

---

## Basic Project Structure

```

sbb-delay-project/
│
├── dashboard.py          # Main Streamlit dashboard
├── ingest_sbb.py         # Script for loading SBB data into SQLite
├── ingest_db.py          # Script for loading DB data (placeholder)
├── sbb_data.db           # SQLite database for SBB
├── db_data.db            # SQLite database for DB (placeholder)
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation

````

---

## Technologies
- Python 3.9+
- Streamlit — user interface
- SQLite — historical data storage
- Requests — API requests
- Plotly — interactive charts
- Pandas — data processing
- streamlit_autorefresh — automatic dashboard refresh every 5 minutes

---

## DB API Connection

* Register at the DB Developer Portal
* Create an application with OAuth 2.0 Client ID and Client Secret
* Subscribe to the Timetables API
* Update `ingest_db.py` with the new token and endpoints

---

## Key Notes

* Historical data is stored for trend analysis
* Fully interactive dashboard adapts to user selections
* Supports filtering by train type and time
* DB integration is **under development**

