# parse_xml.py
# Parse Deutsche Bahn XML stationboard files and store in SQLite

import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
import os

DB_FILE = "db_data.db"
DATA_FOLDER = "deutsche-bahn-data/data/2025-10-24/"  # укажи актуальную папку

# ------------------------
# Создаём таблицу, если её нет
# ------------------------
def create_db(db_file=DB_FILE):
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
# Парсим один XML файл
# ------------------------
def parse_xml_file(filepath, conn):
    tree = ET.parse(filepath)
    root = tree.getroot()
    
    station_name = root.attrib.get("station", "Unknown")
    fetched_at = datetime.utcnow().isoformat()
    
    cur = conn.cursor()
    inserted = 0

    for s in root.findall(".//s"):
        for tag in ["dp", "ar"]:
            for block in s.findall(tag):
                for m in block.findall("m"):
                    train_id = m.attrib.get("id", "")
                    category = m.attrib.get("cat", "")
                    t_type = m.attrib.get("t", "")
                    delay_sec = int(m.attrib.get("c", "0"))
                    ts = m.attrib.get("ts", "")
                    ts_human = m.attrib.get("ts-tts", "")
                    
                    raw_xml = ET.tostring(m, encoding="unicode")
                    
                    cur.execute("""
                        INSERT INTO stationboard (
                            fetched_at, station, train_name, category, to_station, operator,
                            scheduled_time, actual_time, delay_seconds, delay_minutes, raw_xml
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        fetched_at, station_name, train_id, category, t_type, "",
                        ts, ts_human, delay_sec, int(delay_sec/60), raw_xml
                    ))
                    inserted += 1

    conn.commit()
    return inserted

# ------------------------
# Основная функция: прогоняем все файлы в папке
# ------------------------
def main():
    create_db(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    total = 0

    for filename in os.listdir(DATA_FOLDER):
        if filename.endswith(".xml"):
            filepath = os.path.join(DATA_FOLDER, filename)
            n = parse_xml_file(filepath, conn)
            print(f"{filename}: inserted {n} rows")
            total += n

    conn.close()
    print(f"Total rows inserted: {total}")

if __name__ == "__main__":
    main()
