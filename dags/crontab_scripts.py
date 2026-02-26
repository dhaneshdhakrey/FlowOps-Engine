import sys
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.custome_email import custom_failure_alert
from airflow.models.baseoperator import chain
from airflow.models.baseoperator import cross_downstream


from datetime import datetime,timedelta 
from utils.cron_job_email import custom_failure_alert
from utils.cron_job_email import custom_success_alert
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.logging_config1 import start_logging
#check this one once 
from scripts.partner import process_au_partners
from scripts.unit_report_7days import process_unit_report_7days
from scripts.unit_report_15days import process_unit_report_15days
from scripts.unit_report_30days import process_unit_report_30days
from scripts.unit_report_60days import process_unit_report_60days
from scripts.unit_report import process_unit_report
from scripts.unit_wise_report_monthly import process_unit_wise_report_monthly
from scripts.unit_report_monthly import process_unit_report_monthly
from scripts.unit_city_stories_metrics_daily import process_unit_city_stories_metrics_daily
from scripts.city_level_story_report import process_city_level_story_report

from scripts.unit_wise_report_7days import process_unit_wise_report_7days
from scripts.unit_wise_report_15days import process_unit_wise_report_15days
from scripts.unit_wise_report_30days import process_unit_wise_report_30days
from scripts.unit_wise_report_60days import process_unit_wise_report_60days

from scripts.daily_cvr import process_daily_cvr

from scripts.unit_wise_report import process_unit_wise_report
from scripts.child_category import process_child_category
from scripts.feedversion_hourly import process_feed_feedversion_hourly
from scripts.feed_daily import process_feed_daily
from scripts.stories_views import process_stories_views
from scripts.author import process_author
from scripts.generic_report import process_generic_report
from scripts.feedversion_daily import process_feed_feedversion_daily

from scripts.feed_hourly import process_feed_hourly
from scripts.unit_metrics import fetch_overall_unit_metrics


from datetime import datetime

logger = start_logging("etl_author.log")
# logger=start_Logging()
# logger.info("ETL job started")



def driver_function(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    #result=process_generic_report("2025-08-25")
    
    #result=process_stories_views("2025-08-26")
    #result=process_feed_hourly("2025-08-26")
    #result-process_feed_daily(run_date)
    
    #result=process_feed_feedversion_daily("2025-08-26")
    #result=process_feed_feedversion_hourly("2025-08-26")
    #result=process_unit_wise_report()
    # result=process_daily_cvr(run_date)
    
    #result=process_child_category(run_date)
    # result = process_author("2025-08-16")
    #result=process_au_partners(run_date)
    #result=process_unit_wise_report_7days(run_date)
    #result=process_unit_wise_report_15days(run_date)
    # result=process_unit_wise_report_30days(run_date)
    # result=process_unit_wise_report_60days(run_date)
   
    #result=process_unit_report_7days(run_date)
    #result=process_unit_report_15days(run_date)
    #result=process_unit_report_30days(run_date)
    #result=process_unit_report_60days(run_date)
    #result=process_unit_report(run_date)
    #result=process_unit_wise_report_monthly("August")
    #result=process_unit_report_monthly("August")
    #result=process_unit_city_stories_metrics_daily(run_date,agg_type="daily")
    #result=process_city_level_story_report(run_date)
    
    
    # result=unit
    
    
    
    result=process_unit_report()
    
    print(f"result is ---------{result}")

    # Push result into XCom so callbacks can access it
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def stories_views(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_stories_views(run_date)
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def feed_hourly(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_feed_hourly(run_date)
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def feed_daily(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_feed_daily(run_date)
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def feedversion_hourly(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_feed_feedversion_hourly(run_date)
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result


def feedversion_daily_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_feed_feedversion_daily(run_date)
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_wise_report_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_unit_wise_report()
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def daily_cvr_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    result=process_daily_cvr(run_date)
    
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def child_category_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_child_category(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def author_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_author(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def au_partners_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_au_partners(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_wise_report_7days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_wise_report_7days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_wise_report_15days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_wise_report_15days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_wise_report_30days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_wise_report_30days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_wise_report_60days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_wise_report_60days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_report_7days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_report_7days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_report_15days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_report_15days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_report_30days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_report_30days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_report_60days_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_report_60days(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_report_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    result=process_unit_report(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_wise_report_monthly_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")   # 'August'
    result=process_unit_wise_report_monthly(month_name)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_report_monthly_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")   # 'August'
    result=process_unit_report_monthly(month_name)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def unit_city_stories_metrics_daily_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")   # 'August'
    result=process_unit_city_stories_metrics_daily(run_date,agg_type="daily")
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def city_level_story_report_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")   # 'August'
    result=process_city_level_story_report(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def generic_report_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")   # 'August'
    result=process_generic_report(run_date)
    
    print(f"result is ---------{result}")
    context["ti"].xcom_push(key="process_result", value=result)

    if result["status"] == "ok":
        logger.info(f"DAG successful for date  values were found{run_date}")
    elif result["status"] == "no_data":
        logger.warning(f"No data found for {run_date}, treating as soft success")
    else:
        logger.error(f"DAG failed for {run_date} with error: {result.get('error')}")

    return result

def fetch_overall_unit_metrics_func(execution_date, **context):
    run_date = execution_date
    logger.info(f"DAG run started for date {run_date}")
    
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")   # 'August'
    result=fetch_overall_unit_metrics()
    
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
    dag_id="cron_type_1",
    default_args=default_args,
    start_date=datetime(2025, 8,19),
    # schedule="0,15,34,45,58 * * * *",
    schedule="@daily",
    catchup=False
) as dag:

    # fetch_data_task = PythonOperator(
    #     task_id="calling_func",
    #     python_callable=driver_function,
    #      op_kwargs={"execution_date": "{{ ds }}"},
    # )
    stories_views=PythonOperator(
        task_id="stories_views_task",
        python_callable=stories_views,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    feed_hourly=PythonOperator(
        task_id="feed_hourly_task",
        python_callable=feed_hourly,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    feed_daily=PythonOperator(
        task_id="feed_daily_task",
        python_callable=feed_daily,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    feedversion_daily=PythonOperator(
        task_id="feedversion_daily_task",
        python_callable=feedversion_daily_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    feedversion_hourly=PythonOperator(
        task_id="feedversion_hourly_task",
        python_callable=feedversion_hourly,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_wise_report=PythonOperator(
        task_id="unit_wise_report_task",
        python_callable=unit_wise_report_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    daily_cvr=PythonOperator(
        task_id="daily_cvr_task",
        python_callable=daily_cvr_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    child_category=PythonOperator(
        task_id="child_category_task",
        python_callable=child_category_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    author=PythonOperator(
        task_id="author_task",
        python_callable=author_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    partners=PythonOperator(
        task_id="partners_task",
        python_callable=au_partners_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_wise_report_7days=PythonOperator(
        task_id="unit_wise_report_7days_task",
        python_callable=unit_wise_report_7days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_wise_report_15days=PythonOperator(
        task_id="unit_wise_report_15days_task",
        python_callable=unit_wise_report_15days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_wise_report_30days=PythonOperator(
        task_id="unit_wise_report_30days_task",
        python_callable=unit_wise_report_30days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_wise_report_60days=PythonOperator(
        task_id="unit_wise_report_60days_task",
        python_callable=unit_wise_report_60days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_report_7days=PythonOperator(
        task_id="unit_report_7days_task",
        python_callable=unit_report_7days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_report_15days=PythonOperator(
        task_id="unit_report_15days_task",
        python_callable=unit_report_15days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_report_30days=PythonOperator(
        task_id="unit_report_30days_task",
        python_callable=unit_report_30days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_report_60days=PythonOperator(
        task_id="unit_report_60days_task",
        python_callable=unit_report_60days_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_report=PythonOperator(
        task_id="unit_report_task",
        python_callable=unit_report_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_wise_report_monthly=PythonOperator(
        task_id="unit_wise_report_monthly_task",
        python_callable=unit_wise_report_monthly_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_report_monthly=PythonOperator(
        task_id="unit__report_monthly_task",
        python_callable=unit_report_monthly_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    unit_city_stories_metrics_daily=PythonOperator(
        task_id="unit_city_stories_metrics_daily_task",
        python_callable=unit_city_stories_metrics_daily_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    city_level_story_report=PythonOperator(
        task_id="city_level_story_report_task",
        python_callable=city_level_story_report_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    genric_report=PythonOperator(
        task_id="generic_report_task",
        python_callable=generic_report_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    fetch_overall_unit_metrics_task=PythonOperator(
        task_id="fetch_overall_unit_metrics_task",
        python_callable=fetch_overall_unit_metrics_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    
    
    
    
    
    
    
group1=[
    stories_views,
    feed_hourly,
    feed_daily,
    feedversion_daily,
    feedversion_hourly,
    # unit_wise_report,
    daily_cvr,
    child_category,
    author,
    partners,
    # unit_wise_report_7days,
    # unit_wise_report_15days,
    # unit_wise_report_30days,
    # unit_wise_report_60days,
    # unit_report_7days,
    # unit_report_15days,
    # unit_report_30days,
    # unit_report_60days,
    # unit_report,
    # unit_wise_report_monthly,
    # unit_report_monthly,
    genric_report,
    unit_city_stories_metrics_daily,
    city_level_story_report
    # fetch_overall_unit_metrics
]

group2=[
    unit_report_7days,
    unit_report_15days,
    unit_report_30days,
    unit_report_60days,
    unit_report_monthly,
    unit_report,
    fetch_overall_unit_metrics_task
]

group3=[
    unit_wise_report_7days,
    unit_wise_report_15days,
    unit_wise_report_30days,
    unit_wise_report_60days,
    unit_wise_report_monthly,
    unit_wise_report
]
cross_downstream(group1, group2)
cross_downstream(group2, group3)