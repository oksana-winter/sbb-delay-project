# auto_plot.py
import time
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

from ingest_sbb import fetch_station_data  # импорт функции из ingest_sbb.py

station_name = "Zurich"  # станция для отслеживания
fetch_limit = 15         # сколько ближайших рейсов забираем за раз
update_interval = 300    # 5 минут в секундах

def fetch_and_store(station_name):
    """Fetch new data and store in SQLite"""
    fetch_station_data(station=station_name, limit=fetch_limit)

def plot_delays(station_name="Zurich"):
    """Plot train delays from SQLite database (only delayed trains)"""
    conn = sqlite3.connect("sbb_data.db")
    df = pd.read_sql_query("""
        SELECT fetched_at, train_name, delay_minutes, station
        FROM stationboard
        WHERE station = ?
        ORDER BY fetched_at ASC
    """, conn, params=(station_name,))
    conn.close()

    # Фильтруем только поезда с задержкой
    df = df[df['delay_minutes'] > 0]

    if df.empty:
        print("No delayed trains to plot.")
        return

    df['fetched_at'] = pd.to_datetime(df['fetched_at'])
    plt.figure(figsize=(12,6))

    for train in df['train_name'].unique():
        train_data = df[df['train_name'] == train]
        plt.plot(train_data['fetched_at'], train_data['delay_minutes'],
                 marker='o', linestyle='-', label=train)
        for x, y in zip(train_data['fetched_at'], train_data['delay_minutes']):
            plt.text(x, y, str(y), fontsize=8, ha='center', va='bottom')

    avg_delay = df['delay_minutes'].mean()
    plt.title(f"Train Delays Over Time at {station_name} (avg {avg_delay:.1f} min)")
    plt.xlabel("Time of Fetch")
    plt.ylabel("Delay (minutes)")
    plt.grid(True)
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.8)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

# Основной цикл: каждые update_interval секунд обновляем данные и график
while True:
    print(f"Fetching new data for {station_name}...")
    fetch_and_store(station_name)
    print("Plotting delays...")
    plot_delays(station_name)
    print(f"Waiting {update_interval//60} minutes before next update...\n")
    time.sleep(update_interval)
