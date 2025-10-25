import os
import sqlite3
import shutil

DB_PATH = "db_data.db"
DATA_DIR = "deutsche-bahn-data"

# --- 1️⃣ Очистка базы данных ---
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("[DB] Подсчитываем записи для удаления...")

cur.execute("""
SELECT COUNT(*) FROM stationboard
WHERE LOWER(station) NOT LIKE '%hbf%'
""")
to_delete = cur.fetchone()[0]
print(f"[DB] Найдено {to_delete} строк для удаления (не Hbf)")

if to_delete > 0:
    cur.execute("""
    DELETE FROM stationboard
    WHERE LOWER(station) NOT LIKE '%hbf%'
    """)
    conn.commit()
    print(f"[DB] Удалено {to_delete} строк из базы данных.")
else:
    print("[DB] Нечего удалять — все станции с Hbf.")

conn.close()

# --- 2️⃣ Очистка файлов XML ---
deleted_files = 0
deleted_folders = 0

print("[XML] Удаляем станции без Hbf из папки данных...")

for root, dirs, files in os.walk(DATA_DIR, topdown=False):
    for name in files:
        if not name.lower().endswith(".xml"):
            continue
        if "hbf" not in root.lower() and "hbf" not in name.lower():
            try:
                os.remove(os.path.join(root, name))
                deleted_files += 1
            except Exception:
                pass
    # удалить пустые папки
    if not os.listdir(root):
        try:
            os.rmdir(root)
            deleted_folders += 1
        except Exception:
            pass

print(f"[XML] Удалено {deleted_files} файлов и {deleted_folders} пустых папок.")

print("✅ Очистка завершена.")
