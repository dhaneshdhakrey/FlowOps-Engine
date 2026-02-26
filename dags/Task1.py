import sys
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.custome_email import custom_failure_alert

sys.path.append("/home/hadoop/Documents/Codes/FlaskAPis")  
from datetime import datetime,timedelta
from Handlers.fetch_data_handler import fetch_data_Handler  
from utils.custome_email import custom_failure_alert
from utils.custome_email import custom_success_alert
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.logging_config1 import start_logging
from logging_config import start_Logging
from datetime import datetime

logger = start_logging("etlslogs.log")
# logger=start_Logging()
# logger.info("ETL job started")

def run_fetch_data(execution_date, **context):
    # today_str = datetime.today().strftime("%Y-%m-%d")
    #run_date = execution_date.strftime("%Y-%m-%d")
    run_date = execution_date 

    logger.info(f"dag is run is started for date {run_date}")
    result=fetch_data_Handler("2025-08-27")
    print(result)
    if result=="ok":
        logger.info(f"dag is successfull for date {run_date}")
        
    else:
        logger.waring(f"dag not ran for date {run_date}")
    return result
default_args = {
    'owner': 'Dhanesh',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': custom_failure_alert,
    "on_success_callback":custom_success_alert
}

with DAG(
    dag_id="task1_v03",
    default_args=default_args,
    start_date=datetime(2025, 8,19),
    schedule="@daily",
    catchup=True
) as dag:

    fetch_data_task = PythonOperator(
        task_id="calling_func",
        python_callable=run_fetch_data,
         op_kwargs={"execution_date": "{{ ds }}"},
    )
