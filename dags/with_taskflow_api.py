from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'dhanesh',
    'retries': 2,  # Fix: was a string, should be an integer
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='with_taskflow_api_v01',
    default_args=default_args,
    start_date=datetime(2025, 7, 27, 2),
    schedule='@daily',
    catchup=False  # optional: prevents backfilling unless needed
)
def hello_world_etl():

    @task()
    def get_name():
        return "dhanesh"

    @task()
    def get_age():
        return 23

    @task()
    def print_name(name: str, age: int):
        for i in range(1, age + 1):
            print(f"My name is {name} and I am {i} years old.")

    name = get_name()
    age = get_age()
    print_name(name=name, age=age)

# Instantiate the DAG
greetdag = hello_world_etl()
