# parse_xml_folder.py
import os
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime

# ------------------------
# Настройки
# ------------------------
DB_FILE = "db_data.db"
XML_DIR = "deutsche-bahn-data/data/2025-10-24/"  # поменяй на актуальную дату

# ------------------------
# Создание базы и таблицы
# ------------------------
def create_db(db_file):
    conn = sqlite3.connect(db_file)
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
        raw_xml TEXT
    );
    """
    conn.execute(sql)
    conn.commit()
    conn.close()

# ------------------------
# Парсинг одного XML
# ------------------------
def parse_xml_file(filepath, conn):
    tree = ET.parse(filepath)
    root = tree.getroot()
    
    station_name = root.attrib.get("station", "Unknown")
    fetched_at = datetime.utcnow().isoformat()
    
    cur = conn.cursor()
    inserted = 0
    
    departures = root.findall(".//departure")
    for departure in departures:
        train_info = departure.find("train")
        name = train_info.findtext("name", "") if train_info is not None else ""
        category = train_info.findtext("category", "") if train_info is not None else ""
        operator = train_info.findtext("operator", "") if train_info is not None else ""
        to_station = departure.findtext("direction", "")
        scheduled = departure.findtext("plannedDeparture")
        actual = departure.findtext("actualDeparture") or scheduled
        delay_seconds = int(departure.findtext("delay") or 0)
        delay_minutes = int(delay_seconds / 60)
        raw_xml = ET.tostring(departure, encoding="unicode")
        
        cur.execute("""
            INSERT INTO stationboard (
                fetched_at, station, train_name, category, to_station, operator,
                scheduled_time, actual_time, delay_seconds, delay_minutes, raw_xml
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetched_at, station_name, name, category, to_station, operator,
            scheduled, actual, delay_seconds, delay_minutes, raw_xml
        ))
        inserted += 1
    
    conn.commit()
    return inserted

# ------------------------
# Главная функция
# ------------------------
def main():
    create_db(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    total_inserted = 0

    for filename in os.listdir(XML_DIR):
        if filename.endswith(".xml"):
            filepath = os.path.join(XML_DIR, filename)
            inserted = parse_xml_file(filepath, conn)
            print(f"{filename}: inserted {inserted} rows")
            total_inserted += inserted

    conn.close()
    print(f"Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    main()

