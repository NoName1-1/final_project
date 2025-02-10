В корневой папке хранятся необработанные CSV-файлы, Jupyter Notebook и скрипт для создания таблиц в формате SQL (create_tables.sql).  

В папке data находятся обработанные CSV-файлы, а также две папки для JSON-файлов:  
json_files — содержит необработанные JSON-файлы,  
processed_files — содержит обработанные JSON-файлы.  

В качестве сервера базы данных используется PostgreSQL из Docker с измененным портом 5433.  

В папке dag хранятся два файла:  
dag_csv_to_psql.py — отвечает за загрузку CSV-файлов в БД,  
dag_json.py — отвечает за загрузку JSON-файлов в БД.  

Также в проекте есть папки config, logs и plugins для Airflow.

Для работы сначала нужно запустить контейнер postgres на порту 5433, создать БД с именем project, запустить скрипт для создания таблиц в БД и затем можно запустить DAG в Airflow.

Вот скрипт docker run:

docker run -d \
  --name postgres_project \
  -e POSTGRES_USER=project \
  -e POSTGRES_PASSWORD=project \
  -e POSTGRES_DB=project \
  -p 5433:5432 \
  --network host \
  postgres:latest

Для запуска airflow нужно использовать docker-compose из папки проекта
