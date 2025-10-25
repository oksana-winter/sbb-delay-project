import os
import shutil
from datetime import datetime, timedelta

# Путь к папке с XML
DATA_DIR = "./deutsche-bahn-data/data"

# Сегодня и дата неделю назад
today = datetime.today()
week_ago = today - timedelta(days=7)

deleted_files = 0
deleted_folders = 0

# Проходим по всем подпапкам с датами
for root, dirs, files in os.walk(DATA_DIR):
    for file in files:
        if file.endswith(".xml"):
            file_path = os.path.join(root, file)
            # Берём дату из имени файла, если возможно
            # Имя файла может быть например: 08000128_fchg_12.xml
            # Берём дату из родительской папки (формат YYYY-MM-DD)
            try:
                folder_date_str = os.path.basename(root)
                file_date = datetime.strptime(folder_date_str, "%Y-%m-%d")
            except ValueError:
                # Если папка не по формату даты, оставляем файл
                continue

            if file_date < week_ago:
                os.remove(file_path)
                deleted_files += 1

    # Если после удаления файлов папка пустая — удаляем папку
    if not os.listdir(root):
        shutil.rmtree(root)
        deleted_folders += 1

print(f"Deleted {deleted_files} XML files and {deleted_folders} empty folders before {week_ago.strftime('%Y-%m-%d')}")
