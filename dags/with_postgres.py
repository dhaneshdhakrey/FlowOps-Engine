from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'Dhanesh',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='with_postgres_v05',
    description='this is dag to test the time params',
    default_args=default_args,
    start_date=datetime(2021, 11, 1),
    schedule='@daily',   # <-- fixed
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_postgres_table',   # <-- fixed spelling
        conn_id='Postgres_airflow_db',     # <-- make sure this matches your Airflow connection ID
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt DATE,
                dag_id CHARACTER VARYING,
                PRIMARY KEY (dt, dag_id)
            )
        """
    )
    task2 = SQLExecuteQueryOperator(
    task_id='save_row_in_the_table',
    conn_id='Postgres_airflow_db',
    sql="""
        INSERT INTO dag_runs (dt, dag_id) VALUES ('{{ ds }}', '{{ dag.dag_id }}')
    """
    )
    task3=SQLExecuteQueryOperator(
        task_id='to_delete_the_same_id_dag',
        conn_id="Postgres_airflow_db",
        sql="""
        DELETE FROM dag_runs 
WHERE dt = '{{ ds }}' 
AND dag_id = '{{ dag.dag_id }}'
         """
    )
    

    

    task1>>task3>>task2
