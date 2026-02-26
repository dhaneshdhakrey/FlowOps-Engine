from airflow import DAG
import sys
import os

import pendulum
import paramiko

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator

from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime

from utils.cron_job_email import custom_success_alert
from utils.logging_config1 import start_logging
from utils.cron_job_email import custom_failure_alert



logger = start_logging("etl_ssh.log")

def call_ssh(execution_date, **context):
    
    run_date=execution_date
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.59.***", username="username", password="****")
    password = "****"
    command = f"echo {password} | sudo -S python3 /home/hadoop/dhanesh/feed_hourly.py {run_date}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read().decode())
    print(stderr.read().decode())

    ssh.close()
    return 
    


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "on_success_callback": custom_success_alert,
    "on_failure_callback": custom_failure_alert,
}

with DAG(
    dag_id="ssh_remote_hadoop_spark_job",
    default_args=default_args,
    description="Run remote Python script on Hadoop/Spark node via SSH",
    schedule=None,   
    start_date=datetime(2021, 11, 1),
    catchup=False,
    tags=["ssh", "hadoop", "spark", "remote"],
) as dag:

    # run_remote_script = SSHOperator(
    #     task_id="connect_ssh_",
    #     ssh_conn_id="ssh_application_server",
    #     command='cd dhanesh && echo "****" | sudo -S python3 feed_hourly.py 2025-09-09',
    #     do_xcom_push=True,  # Ensures stdout/stderr are pushed to XCom and visible in logs
    # )
    second_way=PythonOperator(
        task_id="shh_pyhton_operator",
        python_callable=call_ssh,
        op_kwargs={"execution_date":"{{ds}}"},
    )
    
second_way
