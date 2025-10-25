import sqlite3
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
import os

# -----------------------------
# Config
# -----------------------------
GITHUB_URL = "https://github.com/yourusername/yourrepo/raw/main/db_data.db"
LOCAL_DB_PATH = Path("db_data.db")
XML_FOLDER = Path("data")  # Папка с XML файлами
KEEP_DAYS = 7  # Сколько дней XML хранить

# -----------------------------
# Update DB
# -----------------------------
def update_db():
    response = requests.get(GITHUB_URL)
    if response.status_code == 200:
        with open(LOCAL_DB_PATH, "wb") as f:
            f.write(response.content)
        print(f"{datetime.now()}: DB updated successfully!")
    else:
        print(f"{datetime.now()}: Failed to download DB, status code {response.status_code}")

# -----------------------------
# Cleanup old XMLs
# -----------------------------
def cleanup_xml():
    cutoff_date = datetime.now() - timedelta(days=KEEP_DAYS)
    if not XML_FOLDER.exists():
        print(f"{XML_FOLDER} does not exist, skipping XML cleanup.")
        return

    removed = 0
    for file in XML_FOLDER.glob("*.xml"):
        if datetime.fromtimestamp(file.stat().st_mtime) < cutoff_date:
            file.unlink()
            removed += 1
    print(f"{datetime.now()}: Removed {removed} old XML files.")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    update_db()
    cleanup_xml()

