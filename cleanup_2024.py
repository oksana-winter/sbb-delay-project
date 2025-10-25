import sqlite3

DB_FILE = "db_data.db"

def delete_2024_rows():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Проверяем сколько строк попадает под условие
    cur.execute("""
        SELECT COUNT(*) FROM stationboard
        WHERE fetched_at LIKE '2024-%'
    """)
    count = cur.fetchone()[0]
    print(f"Rows to delete: {count}")

    # Удаляем строки за 2024 год
    cur.execute("""
        DELETE FROM stationboard
        WHERE fetched_at LIKE '2024-%'
    """)
    conn.commit()
    print(f"Deleted {count} rows from {DB_FILE}")
    conn.close()

if __name__ == "__main__":
    delete_2024_rows()
