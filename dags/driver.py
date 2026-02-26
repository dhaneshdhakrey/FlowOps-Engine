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


import paramiko



from datetime import datetime

logger = start_logging("etl_author.log")






def stories_views(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 stories_views.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def feed_hourly(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 feed_hourly.py {run_date}"

    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def feed_daily(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 feed_daily.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def feedversion_hourly(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 feedversion_hourly.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return


def feedversion_daily_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 feedversion_daily.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_wise_report_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_wise_report.py"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def daily_cvr_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 daily_cvr.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def child_category_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 child_category.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def author_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 author.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def au_partners_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 partner.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_wise_report_7days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_wise_report_7days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_wise_report_15days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_wise_report_15days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_wise_report_30days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_wise_report_30days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_wise_report_60days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_wise_report_60days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_report_7days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_report_7days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_report_15days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_report_15days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_report_30days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_report_30days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_report_60days_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_report_60days.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_report_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_report.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_wise_report_monthly_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_wise_report_monthly.py {month_name}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_report_monthly_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_report_monthly.py {month_name}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_city_stories_metrics_daily_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_city_stories_metrics_daily.py {run_date} daily"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def city_level_story_report_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 city_level_story_report.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def generic_report_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 generic_report.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def fetch_overall_unit_metrics_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_metrics.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return

def unit_npc_report_func(execution_date, **context):
    run_date = execution_date
    print(f"rund date is {run_date}")
    logger.info("job started")
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    month_name = dt.strftime("%B")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"cd dhanesh && cd scripts && source py_venv/bin/activate && python3 unit_npc.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("this is smtg-------------------------------")
    result = stdout.read().decode()
    context["ti"].xcom_push(key="process_result", value=result)
    print(result)
    print(stderr.read().decode())
    logger.info("job completed")
    ssh.close()
    return


default_args = {
    'owner': 'Dhanesh',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': custom_failure_alert,
    "on_success_callback":custom_success_alert
}

with DAG(
    dag_id="ssh_crontab_driver_dag_3",
    default_args=default_args,
    start_date=datetime(2025,9,8),
    #schedule="0,15,34,45,58 * * * *",
    schedule="@daily",
    catchup=False
) as dag:

    
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
    unit_npc_report=PythonOperator(
        task_id="unit_npc_report_task",
        python_callable=unit_npc_report_func,
        op_kwargs={"execution_date":"{{ds}}"},
         trigger_rule="all_done"
    )
    
    
    
    
    
    
    
group1=[
    stories_views,
    feed_hourly,
    feed_daily,
    feedversion_daily,
    feedversion_hourly,
    
    daily_cvr,
    child_category,
    author,
    partners,
    
    genric_report,
    unit_city_stories_metrics_daily,
    city_level_story_report
    
]

group2=[
    unit_report_7days,
    unit_report_15days,
    unit_report_30days,
    unit_report_60days,
    unit_report_monthly,
    unit_report,
    fetch_overall_unit_metrics_task,
    unit_npc_report
]

group3=[
    unit_wise_report_7days,
    unit_wise_report_15days,
    unit_wise_report_30days,
    unit_wise_report_60days,
    unit_wise_report_monthly,
    unit_wise_report
]
# cross_downstream(group1, group2)
# cross_downstream(group2, group3)