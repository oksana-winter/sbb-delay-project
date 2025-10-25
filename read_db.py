import sqlite3  # библиотека для работы с SQLite

# Подключаемся к базе данных sbb_data.db
# Если база в другой папке, укажи полный путь
conn = sqlite3.connect("sbb_data.db")  

# Создаём объект курсора — через него выполняем запросы
cur = conn.cursor()

# SQL-запрос: выбрать последние 20 записей из таблицы stationboard
cur.execute("""
SELECT id, fetched_at, station, train_name, to_station, delay_minutes
FROM stationboard
ORDER BY id DESC
LIMIT 20;
""")

# Получаем все строки результата
rows = cur.fetchall()

# Выводим строки на экран
for row in rows:
    print(row)

# Закрываем соединение с базой
conn.close()
