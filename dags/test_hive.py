import sys
import os 
import requests
from datetime import datetime,timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import paramiko
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG

#status array pass karke dekho  when second task will always run
#conditional run when in first task we use and second task run conditiionally
# status=[0]*


def check_all_hive(ti):
    # checking hive ui first
    
    status=[0]*3
    try:
        response=requests.get('http://10.59.***:10002/',timeout=10)
        if response.ok:
            print("response is ok")
            status[0]=1

        else:
            print("response is not ok")
            status[0]=0
    except Exception as e:
        print("Error sending request:", e)
        status[0] = 0
        
    
    
    ti.xcom_push(key="hive_status", value=status)
    #check other two also if required 
    #this 
    print(status)
    return status


def restart_hive(ti):
    status = ti.xcom_pull(task_ids='check_hive_components_task', key='hive_status')
    print(f"Hive status: {status}")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
            hostname="10.59.***",
            username="username",
            password="****",
            port=2122
    )
    cmd1=f"cd /usr/local/hive/bin && pwd "
    #  export HIVE_HOME=/usr/local/hive && \
    #export PATH=$HIVE_HOME/bin:$PATH && \
        
    hive_commands = """
    cd /usr/local/hive/bin && pwd &&./start-hive.sh
    """
    stdin, stdout, stderr = ssh.exec_command(hive_commands)
    output=stdout.read().decode()
    print(output)
    

    # if status and status[0] == 0:
    #     print("Restarting Hive...")

    #     ssh = paramiko.SSHClient()
    #     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #     ssh.connect(
    #         hostname="10.59.***",
    #         username="username",
    #         password="****",
    #         port=2122
    #     )

    #     # Step 1: Find PIDs
    #     command1 = "ps aux | grep 'hive --service metastore' | grep -v grep | awk '{print $2}'"
    #     command2 = "ps aux | grep 'hiveserver2' | grep -v grep | awk '{print $2}'"

    #     for cmd in [command1, command2]:
    #         stdin, stdout, stderr = ssh.exec_command(cmd)
    #         pids = stdout.read().decode().strip().splitlines()
    #         if pids:
    #             print(f"Found Hive processes: {pids}")
    #             # Step 2: Kill processes
    #             kill_cmd = f"kill -9 {' '.join(pids)}"
    #             ssh.exec_command(kill_cmd)
    #             print(f"Killed processes: {pids}")
    #         else:
    #             print(f"No processes found for: {cmd}")

        
    #     # Step 2: start Hive Metastore in a screen session
    #     restart_metastore = "cd /usr/local/hive && screen -dmS metastore bash -c 'hive --service metastore'"
    #     ssh.exec_command(restart_metastore)

    #     # Step 3: start HiveServer2 in a screen session
    #     # restart_hiveserver2 = "cd /usr/local/hive && screen -dmS hiveserver2 bash -c 'hiveserver2'"
    #     # ssh.exec_command(restart_hiveserver2)
    #     print("--------------- Hive restarted ---------------")
    #     ssh.close()

    # else:
    #     print("All good, no restart needed.")

    
    

default_args={
    'owner':'Dhanesh',
    'retries':1,
    'retry_delay':timedelta(minutes=1),
    # 'on_failure_callback':null
}

with DAG(
    dag_id='test_hive',
    default_args=default_args,
    start_date=datetime(2025,9,19),
    schedule='@daily',
    catchup=False,
    # on_success_callback=
    
)as dag:
    check_hive=PythonOperator(
        task_id='check_hive_components_task',
        python_callable=check_all_hive,
        # provide_context=True
    )
    restart_hive=PythonOperator(
        task_id="restart_hive_task",
        python_callable=restart_hive,
        # provide_context=True
    )
    
check_hive>>restart_hive
# status=check_all_hive()
# result=restart_hive(status)


    
    
    
    
    
    