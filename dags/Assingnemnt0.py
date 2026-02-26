from __future__ import annotations
from datetime import datetime, timedelta
import json
import os
import urllib.parse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.custome_email import custom_failure_alert

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from utils.custome_email import custom_failure_alert, custom_success_alert

LOG_DIR = os.path.join(os.path.dirname(__file__), 'logsByairflowDag')  # saves alongside your DAG file

# ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)



def get_mongo_client_from_airflow_conn(conn_id):
    conn = BaseHook.get_connection(conn_id)
    user = urllib.parse.quote_plus(conn.login) if conn.login else ''
    password = urllib.parse.quote_plus(conn.password) if conn.password else ''
    auth_part = f'{user}:{password}@' if user and password else ''
    host = conn.host or 'localhost'
    port = f":{conn.port}" if conn.port else ''
    db_name = conn.schema or ''
    extras = conn.extra_dejson if conn.extra else {}
    extras.pop('uri', None)
    query_params = '&'.join([f"{k}={urllib.parse.quote_plus(str(v))}" for k, v in extras.items()]) if extras else ''
    query_string = f'?{query_params}' if query_params else ''
    mongo_uri = f"mongodb://{auth_part}{host}{port}/{db_name}{query_string}"
    print(f"Constructed MongoDB URI: {mongo_uri}")
    client = MongoClient(mongo_uri)
    return client, db_name


def fetch_all_tables(**kwargs):
    hook = PostgresHook(postgres_conn_id='Postgres_airflow_db')
    sql = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'"
    records = hook.get_records(sql)
    tables = [row[0] for row in records]
    path = os.path.join(LOG_DIR, 'enabled_tables.json')
    with open(path, 'w') as f:
        json.dump(tables, f)
    print(f"Fetched tables: {tables}")


def validate_tables(**kwargs):
    hook = PostgresHook(postgres_conn_id='Postgres_airflow_db')
    path_in = os.path.join(LOG_DIR, 'enabled_tables.json')
    with open(path_in, 'r') as f:
        tables = json.load(f)
    valid = []
    for table in tables:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        if count and count > 0:
            valid.append(table)
    path_out = os.path.join(LOG_DIR, 'valid_tables.json')
    with open(path_out, 'w') as f:
        json.dump(valid, f)
    print(f"Valid tables: {valid}")


def summarize_and_store(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.hooks.base import BaseHook
    from pymongo import MongoClient
    import urllib.parse
    import json
    import os

    LOG_DIR = '/home/hadoop/logsByairflowDag'  # Ensure this is a writable path
    os.makedirs(LOG_DIR, exist_ok=True)

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id='Postgres_airflow_db')
    tables = pg_hook.get_records("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'")
    tables = [t[0] for t in tables]

    # Connect to Mongo
    conn = BaseHook.get_connection('mongo_ariflow_db_localhost')
    user = urllib.parse.quote_plus(conn.login) if conn.login else ''
    password = urllib.parse.quote_plus(conn.password) if conn.password else ''
    auth_part = f'{user}:{password}@' if user and password else ''
    host = conn.host or 'localhost'
    port = f":{conn.port}" if conn.port else ''
    db_name = conn.schema or ''
    extras = conn.extra_dejson if conn.extra else {}
    query_string = ''
    if extras:
        extras.pop('uri', None)
        query_string = '?' + '&'.join(f"{k}={urllib.parse.quote_plus(str(v))}" for k, v in extras.items())
    mongo_uri = f"mongodb://{auth_part}{host}{port}/{db_name}{query_string}"
    client = MongoClient(mongo_uri)
    mongo_db = client[db_name]

    for table in tables:
        try:
            # Fetch rows
            rows = pg_hook.get_records(f"SELECT geo_scope, _id FROM {table}")
            total = len(rows)

            # Write to JSON
            summary = {table: total}
            json_path = os.path.join(LOG_DIR, f"{table}.json")
            with open(json_path, 'w') as jf:
                json.dump(summary, jf, default=str)
            print(f"Wrote summary for {table}: {summary}")

            # Insert to Mongo
            coll = mongo_db[table]
            doc = {'_id': table, 'total_rows': total}
            coll.insert_one(doc)
            print(f"Inserted Mongo document for {table}")

        except Exception as e:
            print(f"Error processing table {table}: {e}")



def default_args():
    return {
        'owner': 'Dhanesh',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': custom_failure_alert,
    }

with DAG(
    dag_id='Assignment0_v07',
    default_args=default_args(),
    start_date=datetime(2021, 11, 1),
    schedule='@daily',
    catchup=False,
    tags=['final-test'],
    on_success_callback=custom_success_alert,
) as dag:
    task0 = PythonOperator(
        task_id='fetch_all_tables',
        python_callable=fetch_all_tables,
    )

    task1 = PythonOperator(
        task_id='validate_tables',
        python_callable=validate_tables,
    )

    task2 = PythonOperator(
        task_id='etl_job',
        python_callable=summarize_and_store,
    )

    task0 >> task1 >> task2
