import os
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime

# Путь к папке с XML
DATA_DIR = "deutsche-bahn-data/data/2025-10-24"  # можно менять на любую дату
DB_FILE = "db_data.db"

# ------------------------
# Создаем таблицу
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
# Парсер fchg файлов
# ------------------------
def parse_fchg_xml(filepath, conn):
    tree = ET.parse(filepath)
    root = tree.getroot()
    
    station_name = root.attrib.get("station", "Unknown")
    fetched_at = datetime.utcnow().isoformat()
    
    cur = conn.cursor()
    inserted = 0
    
    for s in root.findall(".//s"):
        # Отправления и прибытия
        for tag in ["ar", "dp"]:
            dep = s.find(tag)
            if dep is None:
                continue
            for m in dep.findall("m"):
                train_type = m.attrib.get("t", "")
                category = m.attrib.get("c", "")
                name = m.attrib.get("id", "")
                operator = ""  # обычно нет в fchg
                
                scheduled = m.attrib.get("ts", "")
                actual = scheduled
                delay_seconds = int(m.attrib.get("c", 0)) if m.attrib.get("c") else 0
                delay_minutes = int(delay_seconds / 60)
                
                raw_xml = ET.tostring(m, encoding="unicode")
                
                cur.execute("""
                    INSERT INTO stationboard (
                        fetched_at, station, train_name, category, to_station, operator,
                        scheduled_time, actual_time, delay_seconds, delay_minutes, raw_xml
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fetched_at, station_name, name, category, "", operator,
                    scheduled, actual, delay_seconds, delay_minutes, raw_xml
                ))
                inserted += 1
                
    conn.commit()
    return inserted

# ------------------------
# Парсер plan файлов
# ------------------------
def parse_plan_xml(filepath, conn):
    tree = ET.parse(filepath)
    root = tree.getroot()
    
    station_name = root.attrib.get("station", "Unknown")
    fetched_at = datetime.utcnow().isoformat()
    
    cur = conn.cursor()
    inserted = 0
    
    for s in root.findall("s"):
        tl = s.find("tl")
        if tl is not None:
            category = tl.attrib.get("c", "")
            name = tl.attrib.get("n", "")
            operator = tl.attrib.get("o", "")
        else:
            category = name = operator = ""
        
        # Прибытие
        ar = s.find("ar")
        if ar is not None:
            scheduled = ar.attrib.get("pt", "")
            actual = scheduled
            delay_seconds = delay_minutes = 0
            raw_xml = ET.tostring(ar, encoding="unicode")
            cur.execute("""
                INSERT INTO stationboard (
                    fetched_at, station, train_name, category, to_station, operator,
                    scheduled_time, actual_time, delay_seconds, delay_minutes, raw_xml
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fetched_at, station_name, name, category, ar.attrib.get("ppth", ""), operator,
                scheduled, actual, delay_seconds, delay_minutes, raw_xml
            ))
            inserted += 1
        
        # Отправление
        dp = s.find("dp")
        if dp is not None:
            scheduled = dp.attrib.get("pt", "")
            actual = scheduled
            delay_seconds = delay_minutes = 0
            raw_xml = ET.tostring(dp, encoding="unicode")
            cur.execute("""
                INSERT INTO stationboard (
                    fetched_at, station, train_name, category, to_station, operator,
                    scheduled_time, actual_time, delay_seconds, delay_minutes, raw_xml
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fetched_at, station_name, name, category, dp.attrib.get("ppth", ""), operator,
                scheduled, actual, delay_seconds, delay_minutes, raw_xml
            ))
            inserted += 1
            
    conn.commit()
    return inserted

# ------------------------
# Главная функция
# ------------------------
def parse_all_xml(data_dir=DATA_DIR, db_file=DB_FILE):
    create_db(db_file)
    conn = sqlite3.connect(db_file)
    total_inserted = 0
    
    for fname in os.listdir(data_dir):
        if not fname.endswith(".xml"):
            continue
        filepath = os.path.join(data_dir, fname)
        if "fchg" in fname:
            inserted = parse_fchg_xml(filepath, conn)
        elif "plan" in fname:
            inserted = parse_plan_xml(filepath, conn)
        else:
            continue
        print(f"{fname}: inserted {inserted} rows")
        total_inserted += inserted
    
    conn.close()
    print(f"Total rows inserted: {total_inserted}")

# ------------------------
# Запуск
# ------------------------
if __name__ == "__main__":
    parse_all_xml()
