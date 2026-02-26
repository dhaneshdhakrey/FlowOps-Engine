import sys
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.custome_email import custom_failure_alert

from datetime import datetime,timedelta 
from utils.cron_job_email import custom_failure_alert
from utils.cron_job_email import custom_success_alert
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.logging_config1 import start_logging

from scripts.unit_city_stories_metrics_monthly import save_monthly_metrics
from scripts.unit_city_stories_metrics_lastN import save_lastN_metrics

#check this one once 



from datetime import datetime

logger = start_logging("etl_author.log")
# logger=start_Logging()
# logger.info("ETL job started")



# def driver_function(execution_date, **context):
#     run_date = execution_date
#     logger.info(f"DAG run started for date {run_date}")

    
#     # result=save_monthly_metrics()
#     result=save_lastN_metrics()
    
#     print(f"result is ---------{result}")

#     # Push result into XCom so callbacks can access it
#     context["ti"].xcom_push(key="process_result", value=result)

#     if result["status"] == "ok":
#         logger.info(f"DAG successful for date  values were found{run_date}")
#     elif result["status"] == "no_data":
#         logger.warning(f"No data found for {run_date}, treating as soft success")
#     else:
#         logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

#     return result

def save_monthly_metrics_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=save_monthly_metrics()
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def save_lastN_metrics_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=save_lastN_metrics()
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result





default_args = {
    'owner': 'Dhanesh',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': custom_failure_alert,
    "on_success_callback":custom_success_alert
}

with DAG(
    dag_id="cron_type_2",
    default_args=default_args,
    start_date=datetime(2025, 8,19),
    # schedule="0,15,30,45,58 * * * *",
    schedule="@daily",
    catchup=False
) as dag:

    monthly_metrics = PythonOperator(
        task_id="save_monthly_metrics_task",
        python_callable=save_monthly_metrics_func,
         op_kwargs={"execution_date": "{{ ds }}"},
    )
    lastN_metrics = PythonOperator(
        task_id="save_lastN_metrics_task",
        python_callable=save_lastN_metrics_func,
         op_kwargs={"execution_date": "{{ ds }}"},
    )
    
[
    monthly_metrics,
    lastN_metrics
]
