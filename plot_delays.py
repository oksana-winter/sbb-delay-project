import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Parameters
station_name = "Zurich"  # filter by station

# Connect to the database
conn = sqlite3.connect("sbb_data.db")

# Load data into pandas DataFrame
df = pd.read_sql_query("""
SELECT fetched_at, train_name, delay_minutes, station
FROM stationboard
WHERE station = ?
ORDER BY fetched_at ASC
""", conn, params=(station_name,))

conn.close()

# Convert fetched_at to datetime
df['fetched_at'] = pd.to_datetime(df['fetched_at'])

# Plot delays
plt.figure(figsize=(12,6))

# Plot each train separately
for train in df['train_name'].unique():
    train_data = df[df['train_name'] == train]
    plt.plot(train_data['fetched_at'], train_data['delay_minutes'], marker='o', linestyle='-', label=train)

plt.title(f"Train Delays Over Time at {station_name}")
plt.xlabel("Time of Fetch")
plt.ylabel("Delay (minutes)")
plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()
