# ====================================================================
# --- Full DAG Code - All Logic in One File ---
# ====================================================================

from __future__ import annotations
from datetime import datetime, timedelta
import json
import ast
import urllib.parse
from collections import defaultdict
import os
import sys
import smtplib
from email.mime.text import MIMEText
import traceback
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.custome_email import custom_failure_alert
from utils.custome_email import custom_success_alert
from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator






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
    query_params = '&'.join([f"{k}={urllib.parse.quote_plus(str(v))}" for k,v in extras.items()]) if extras else ''
    query_string = f'?{query_params}' if query_params else ''
    mongo_uri = f"mongodb://{auth_part}{host}{port}/{db_name}{query_string}"
    print(f"Constructed MongoDB URI: {mongo_uri}")
    client = MongoClient(mongo_uri)
    return client, db_name


def write_rows_to_json(ti):
    rows = ti.xcom_pull(task_ids='fetch_table_data')
    filtered_rows = [{'geo_Scope': row[0], '_id': row[1]} for row in rows]
    file_path = '/tmp/webstories_data.json'
    print(filtered_rows)
    with open(file_path, 'w') as f:
        json.dump(filtered_rows, f, default=str)
    print(f"Data written to {file_path}")


def filterdata_and_save_to_mongo():
    file_path = '/tmp/webstories_data.json'
    with open(file_path, 'r') as f:
        data = json.load(f)
    geoid_count = defaultdict(int)
    for item in data:
        geo_scope_str = item.get('geo_Scope', '[]')
        try:
            geo_scope_list = ast.literal_eval(geo_scope_str)
        except Exception as e:
            print(f"Failed to parse geo_Scope: {e}")
            continue
        if not geo_scope_list:
            continue
        geoid = geo_scope_list[1].get('_id') if len(geo_scope_list) > 1 else geo_scope_list[0].get('_id')
        if geoid:
            geoid_count[geoid] += 1
    result = dict(geoid_count)
    client, db_name = get_mongo_client_from_airflow_conn("mongo_ariflow_db_localhost")
    db = client[db_name]
    collection = db['geoid_post_counts']
    documents = [{'_id': geoid, 'count': cnt} for geoid, cnt in result.items()]
    if documents:
        collection.insert_many(documents)
    print(f"Inserted {len(documents)} documents for geoid post counts into MongoDB.")
    return result



default_args = {
    'owner': 'Dhanesh',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': custom_failure_alert,
}

with DAG(
    dag_id='Task0_finalsteps_02',
    default_args=default_args,
    start_date=datetime(2021, 11, 1),
    schedule='@daily',
    catchup=False,
    tags=['final-test'],
    on_success_callback=custom_success_alert
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='fetch_table_data',
        # To test the failure alert, keep this as 'Postgres_airflow_d'
        # For a successful run, change it back to 'Postgres_airflow_db'
        conn_id='Postgres_airflow_db',
        sql="SELECT geo_Scope,_id FROM webstories",
        do_xcom_push=True
    )
    task2 = PythonOperator(
        task_id='write_json_data',
        python_callable=write_rows_to_json
    )
    task3 = PythonOperator(
        task_id='custome_filter',
        python_callable=filterdata_and_save_to_mongo
    )

    task1 >> task2 >> task3
