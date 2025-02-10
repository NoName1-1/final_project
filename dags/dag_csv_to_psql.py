from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import os

# Пути к файлам CSV
current_dir = os.path.dirname(os.path.abspath(__file__))
hits_path = os.path.join(current_dir, '..', 'data', 'df_hits_copy.csv')
sessions_path = os.path.join(current_dir, '..', 'data', 'df_session_copy.csv')

# Параметры подключения к БД
DB_CONN_STR = 'postgresql://project:project@host.docker.internal:5433/project'

# Функция для загрузки CSV в PostgreSQL
def copy_csv_to_db(file_path, table_name):
    engine = create_engine(DB_CONN_STR)
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        with open(file_path, 'r') as f:
            # Указываем явные столбцы в COPY запросе
            if table_name == "hits":
                cursor.copy_expert(f"COPY {table_name} (session_id, hit_date, hit_time, hit_number, hit_type, hit_referer, hit_page_path, event_category, event_action, event_label, event_value) FROM STDIN WITH CSV HEADER", f)
            elif table_name == "sessions":
                cursor.copy_expert(f"COPY {table_name} (session_id, client_id, visit_date, visit_time, visit_number, utm_source, utm_medium, utm_campaign, utm_adcontent, utm_keyword, device_category, device_os, device_brand, device_model, device_screen_resolution, device_browser, geo_country, geo_city) FROM STDIN WITH CSV HEADER", f)
        conn.commit()
    finally:
        conn.close()

# Определяем DAG
with DAG(
    dag_id='csv_to_postgres',
    description='Загрузка CSV в PostgreSQL',
    schedule_interval='@daily',  # Запуск каждый день
    start_date=datetime(2025, 1, 20),
    catchup=False
) as dag:

    # Задача для загрузки hits.csv
    load_hits_task = PythonOperator(
        task_id='load_hits_to_db',
        python_callable=copy_csv_to_db,
        op_kwargs={'file_path': hits_path, 'table_name': 'hits'},
    )


    # Задача для загрузки sessions.csv
    load_sessions_task = PythonOperator(
        task_id='load_sessions_to_db',
        python_callable=copy_csv_to_db,
        op_kwargs={'file_path': sessions_path, 'table_name': 'sessions'},
    )

    # Последовательность выполнения
    load_sessions_task >> load_hits_task

