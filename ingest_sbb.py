# ingest_sbb.py
# Fetch data from transport.opendata.ch and store in SQLite

import requests
import sqlite3
import json
from datetime import datetime
import argparse

API_URL = "https://transport.opendata.ch/v1/stationboard"

def create_db(conn):
    """Create the stationboard table if it doesn't exist"""
    sql = """
    CREATE TABLE IF NOT EXISTS stationboard (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetched_at TEXT,
        station TEXT,
        train_name TEXT,
        category TEXT,
        to_station TEXT,
        operator TEXT,
        scheduled_time TEXT,
        actual_time TEXT,
        delay_seconds INTEGER,
        delay_minutes INTEGER,
        raw_json TEXT
    );
    """
    conn.execute(sql)
    conn.commit()

def fetch_stationboard(station, limit=20):
    """Fetch stationboard JSON from Open Data API"""
    params = {"station": station, "limit": limit}
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

def parse_and_store(json_data, station, conn):
    """Parse API JSON and store in SQLite"""
    entries = json_data.get("stationboard", [])
    inserted = 0
    cur = conn.cursor()
    fetched_at = datetime.utcnow().isoformat()

    for e in entries:
        name = e.get("name") or e.get("category") or ""
        category = e.get("category") or ""
        to_station = e.get("to") or ""
        operator = e.get("operator") or ""

        stop = e.get("stop") or {}
        scheduled = stop.get("departure") or stop.get("arrival") or None

        prognosis = stop.get("prognosis") or {}
        actual = prognosis.get("departure") or prognosis.get("arrival") or scheduled

        delay_seconds = stop.get("delay") or 0
        try:
            delay_seconds = int(delay_seconds)
        except:
            delay_seconds = 0
        delay_minutes = int(delay_seconds / 60) if delay_seconds else 0

        raw = json.dumps(e, ensure_ascii=False)

        cur.execute("""
            INSERT INTO stationboard (
                fetched_at, station, train_name, category, to_station, operator,
                scheduled_time, actual_time, delay_seconds, delay_minutes, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetched_at, station, name, category, to_station, operator,
            scheduled, actual, delay_seconds, delay_minutes, raw
        ))
        inserted += 1

    conn.commit()
    return inserted

def fetch_station_data(station="Zurich", limit=20, db="sbb_data.db"):
    """
    Wrapper: fetch data from API and store in SQLite.
    ✅ Each call appends new rows to the database to keep historical records.
    """
    try:
        # 1️⃣ Fetch data from SBB API
        json_data = fetch_stationboard(station, limit=limit)
    except Exception as e:
        print("Error fetching data:", e)
        return 0

    # 2️⃣ Connect to SQLite database
    conn = sqlite3.connect(db)
    # 3️⃣ Make sure the table exists (create if not)
    create_db(conn)
    # 4️⃣ Parse the JSON and insert new rows into the database
    n = parse_and_store(json_data, station, conn)
    # 5️⃣ Close the connection
    conn.close()
    
    print(f"Fetched and stored {n} rows for station {station}")
    return n


def main():
    parser = argparse.ArgumentParser(description="Ingest stationboard to SQLite")
    parser.add_argument("--station", "-s", default="Zurich", help="Station name (e.g. Zurich)")
    parser.add_argument("--db", default="sbb_data.db", help="SQLite DB filename")
    parser.add_argument("--limit", type=int, default=20, help="Number of upcoming departures to fetch")
    args = parser.parse_args()

    print(f"Fetching stationboard for: {args.station} ...")
    fetch_station_data(station=args.station, limit=args.limit, db=args.db)

if __name__ == "__main__":
    main()
