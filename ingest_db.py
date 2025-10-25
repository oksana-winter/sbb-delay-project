# ingest_db.py
# Fetch data from local Deutsche Bahn GitHub repo and store in SQLite

import sqlite3
import csv
import json
from datetime import datetime
import argparse
from pathlib import Path

# ------------------------
# SQLite DB functions
# ------------------------
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

# ------------------------
# Fetch CSV
# ------------------------
def fetch_csv(station, data_dir="data/stationboard"):
    """
    Find the latest CSV for a station in the local data repo.
    Returns the Path object or None if not found.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"Data directory not found: {data_dir}")
        return None

    # Search for files like YYYY-MM-DD_stationname.csv
    csv_files = sorted(data_path.glob(f"*_{station.lower().replace(' ', '_')}.csv"))
    if not csv_files:
        return None

    # Return the most recent file
    return csv_files[-1]

# ------------------------
# Parse CSV and store
# ------------------------
def parse_and_store(csv_file, station, conn):
    inserted = 0
    cur = conn.cursor()
    fetched_at = datetime.utcnow().isoformat()

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("train_name") or ""
            category = row.get("category") or ""
            to_station = row.get("to_station") or ""
            operator = row.get("operator") or ""
            scheduled = row.get("scheduled_time")
            actual = row.get("actual_time") or scheduled
            delay_seconds = int(row.get("delay_seconds") or 0)
            delay_minutes = int(delay_seconds / 60) if delay_seconds else 0
            raw = json.dumps(row, ensure_ascii=False)

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

# ------------------------
# Wrapper
# ------------------------
def fetch_station_data(station="berlin_hbf", db="db_data.db", data_dir="data/stationboard"):
    csv_file = fetch_csv(station, data_dir=data_dir)
    if not csv_file:
        print(f"No CSV found for station {station} in {data_dir}")
        return 0

    conn = sqlite3.connect(db)
    create_db(conn)
    n = parse_and_store(csv_file, station, conn)
    conn.close()
    print(f"Loaded {n} rows for station {station} from {csv_file.name}")
    return n

# ------------------------
# CLI
# ------------------------
def main():
    parser = argparse.ArgumentParser(description="Ingest DB Stationboard from local CSV to SQLite")
    parser.add_argument("--station", "-s", default="berlin_hbf", help="Station name (e.g. berlin_hbf)")
    parser.add_argument("--db", default="db_data.db", help="SQLite DB filename")
    parser.add_argument("--data_dir", default="data/stationboard", help="Local CSV data directory")
    args = parser.parse_args()

    fetch_station_data(station=args.station, db=args.db, data_dir=args.data_dir)

if __name__ == "__main__":
    main()

