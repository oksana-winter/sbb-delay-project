import sqlite3
import os
from datetime import datetime

# ------------------------
# Параметры
# ------------------------
DB_FILE = "db_data.db"
DATA_DIR = "deutsche-bahn-data/data"
CUTOFF_DATE_STR = "2025-09-01T00:00:00"
CUTOFF_DATE = datetime.strptime("2025-09-01", "%Y-%m-%d")

# ------------------------
# 1️⃣ Очистка базы данных
# ------------------------
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM stationboard WHERE fetched_at < ?", (CUTOFF_DATE_STR,))
rows_to_delete = cur.fetchone()[0]
print(f"[DB] Rows to delete: {rows_to_delete}")

cur.execute("DELETE FROM stationboard WHERE fetched_at < ?", (CUTOFF_DATE_STR,))
conn.commit()
conn.close()
print(f"[DB] Deleted {rows_to_delete} rows from {DB_FILE}")

# ------------------------
# 2️⃣ Очистка старых XML-файлов
# ------------------------
deleted_files = 0
deleted_folders = 0

for folder_name in os.listdir(DATA_DIR):
    folder_path = os.path.join(DATA_DIR, folder_name)
    if not os.path.isdir(folder_path):
        continue
    try:
        folder_date = datetime.strptime(folder_name, "%Y-%m-%d")
    except ValueError:
        continue
    if folder_date < CUTOFF_DATE:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            os.remove(file_path)
            deleted_files += 1
        os.rmdir(folder_path)  # удаляем пустую папку
        deleted_folders += 1

print(f"[XML] Deleted {deleted_files} files and {deleted_folders} folders before {CUTOFF_DATE.date()}")
