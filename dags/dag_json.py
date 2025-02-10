from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import pandas as pd
from sqlalchemy import create_engine


def convert_seconds_to_time(seconds):
    """
    Преобразует секунды в формат HH:MM:SS.
    Если передано некорректное значение, возвращает "00:00:00".
    """
    try:
        seconds = pd.to_numeric(seconds, errors='coerce')
        if pd.isna(seconds):
            return "00:00:00"
        seconds = int(seconds) % 86400  # Ограничиваем значением 24 часов
        td = timedelta(seconds=seconds)
        return f"{td.seconds // 3600:02}:{(td.seconds % 3600) // 60:02}:{td.seconds % 60:02}"
    except Exception as e:
        print(f"Ошибка при обработке hit_time: {e}")
        return "00:00:00"


# Подключение к базе данных PostgreSQL
DB_CONN_STR = 'postgresql://project:project@host.docker.internal:5433/project'
engine = create_engine(DB_CONN_STR)

# Пути к JSON-файлам
JSON_DIR = '/opt/airflow/data/json_files'
PROCESSED_DIR = '/opt/airflow/data/processed_files'

# Целевые события (event_value = 1)
TARGET_EVENTS = [
    'sub_car_claim_click', 'sub_car_claim_submit_click',
    'sub_open_dialog_click', 'sub_custom_question_submit_click',
    'sub_call_number_click', 'sub_callback_submit_click', 'sub_submit_success',
    'sub_car_request_submit_click'
]

# Бренды Android для коррекции device_os
ANDROID_BRANDS = [
    'Huawei', 'Samsung', 'Xiaomi', 'Lenovo', 'Vivo', 'Meizu', 'OnePlus', 'BQ', 'Realme', 'OPPO', 'itel',
    'Nokia', 'Alcatel', 'LG', 'Tecno', 'Asus', 'Infinix', 'Sony', 'ZTE', 'Motorola', 'HTC', 'POCO'
]


def check_new_files(**kwargs):
    """
    Проверяет наличие JSON-файлов в директории.
    """
    files = [f for f in os.listdir(JSON_DIR) if f.endswith('.json')]
    if not files:
        raise ValueError("Нет новых JSON-файлов для обработки.")
    return files  # Возвращаем список файлов


def process_json_files(**kwargs):
    """
    Обрабатывает JSON-файлы, проверяет дубликаты и загружает данные в PostgreSQL.
    """
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids='check_files')

    # Загружаем существующие session_id из базы данных
    existing_sessions = pd.read_sql("SELECT session_id FROM sessions", engine)['session_id'].tolist()

    session_df_list = []
    hit_df_list = []

    for file in files:
        file_path = os.path.join(JSON_DIR, file)

        # Пропускаем пустые файлы
        if os.stat(file_path).st_size == 0:
            print(f"Пропускаем пустой файл: {file}")
            continue

        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Ошибка в JSON-файле: {file}")
                continue

        # Разбираем JSON (обрабатываем случаи, когда данные находятся внутри ключа даты)
        records = []
        for key, value in data.items():
            if isinstance(value, list):
                records.extend(value)
        df = pd.DataFrame(records)

        # Определяем, к какой таблице относится JSON-файл
        if 'session_id' in df.columns and 'hit_date' in df.columns:
            hit_df_list.append(df)
        else:
            session_df_list.append(df)

    # Обработка данных sessions
    if session_df_list:
        df_sessions = pd.concat(session_df_list, ignore_index=True)
        df_sessions.drop_duplicates(subset='session_id', inplace=True)

        # Убираем уже существующие сессии
        df_sessions = df_sessions[~df_sessions['session_id'].isin(existing_sessions)]

        if not df_sessions.empty:
            # Заполняем пропущенные значения
            df_sessions.fillna({
                'utm_keyword': 'unknown',
                'utm_campaign': 'unknown',
                'utm_source': 'unknown',
                'utm_adcontent': 'unknown',
                'device_model': 'unknown',
                'device_os': 'other'
            }, inplace=True)

            # Коррекция значений device_os
            df_sessions.loc[df_sessions['device_os'] == '(not set)', 'device_os'] = 'unknown'
            df_sessions.loc[df_sessions['device_brand'] == 'Apple', 'device_os'] = 'ios'
            df_sessions.loc[df_sessions['device_brand'].isin(ANDROID_BRANDS), 'device_os'] = 'Android'
            df_sessions.loc[df_sessions['device_brand'] == 'Microsoft', 'device_os'] = 'Windows'
            df_sessions.loc[df_sessions['device_brand'] == 'BlackBerry', 'device_os'] = 'BlackBerry'
            df_sessions.loc[df_sessions['device_brand'] == 'Nokia', 'device_os'] = 'Windows Phone'
            df_sessions.loc[df_sessions['device_os'] == 'unknown', 'device_os'] = 'other'

            # Загружаем в PostgreSQL
            df_sessions.to_sql('sessions', engine, if_exists='append', index=False)
            print(f"✅ Загружено в sessions: {len(df_sessions)} записей")

    # Обработка данных hits
    if hit_df_list:
        df_hits = pd.concat(hit_df_list, ignore_index=True)
        df_hits.drop_duplicates(inplace=True)

        # Фильтруем session_id, которые есть в базе
        df_hits = df_hits[df_hits['session_id'].isin(existing_sessions)]

        if not df_hits.empty:
            # Заполняем пропущенные значения
            df_hits['event_label'] = df_hits['event_label'].fillna('unknown')
            df_hits['hit_referer'] = df_hits['hit_referer'].fillna('unknown')

            # Обрабатываем hit_time
            df_hits['hit_time'] = df_hits['hit_time'].astype(str).str.strip().replace('', '0')
            df_hits['hit_time'] = df_hits['hit_time'].apply(convert_seconds_to_time)

            # Определяем целевые события
            df_hits['event_value'] = df_hits['event_action'].apply(lambda x: 1 if x in TARGET_EVENTS else 0)

            # Загружаем в PostgreSQL
            df_hits.to_sql('hits', engine, if_exists='append', index=False)
            print(f"✅ Загружено в hits: {len(df_hits)} записей")

    # Перемещаем обработанные файлы в архив
    for file in files:
        os.rename(os.path.join(JSON_DIR, file), os.path.join(PROCESSED_DIR, file))
        print(f"📂 Файл {file} перемещен в {PROCESSED_DIR}")


# DAG для обработки JSON-файлов
with DAG(
        'dag_json_processing',
        description='Обработка JSON-файлов и загрузка в PostgreSQL с проверкой дубликатов',
        start_date=datetime(2025, 1, 20),
        schedule_interval='@daily',
        catchup=False
) as dag:
    check_files = PythonOperator(
        task_id='check_files',
        python_callable=check_new_files,
        provide_context=True
    )

    process_files = PythonOperator(
        task_id='process_files',
        python_callable=process_json_files,
        provide_context=True
    )

    check_files >> process_files
