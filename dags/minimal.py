#!/usr/bin/env python3
from __future__ import annotations
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
import traceback

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# — expand ~ correctly —
AIRFLOW_HOME = os.path.expanduser(os.environ.get("AIRFLOW_HOME", "~/airflow"))
LOG_FILE    = os.path.join(AIRFLOW_HOME, 'logs', 'custom_failure_log.txt')

def send_custom_email(subject: str, body: str):
    SMTP_HOST = "smtp.sendgrid.net"
    SMTP_PORT = 587
    SMTP_SENDER = "no-reply@auw.co.in"
    SMTP_USERNAME = "username"
    SMTP_PASSWORD = "****"

    RECIPIENT_EMAILS = ['aman.satya.auw@gmail.com', 'dsddhakrey421@gmail.com']

    try:
        msg = MIMEText(body, 'html')
        msg['Subject'] = subject
        msg['From'] = SMTP_SENDER
        msg['To'] = ", ".join(RECIPIENT_EMAILS)

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
        server.set_debuglevel(1)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)  # Corrected line
        server.sendmail(SMTP_SENDER, RECIPIENT_EMAILS, msg.as_string())
        server.quit()
        print("--- Custom email sent successfully! ---")
    except Exception as e:
        print("--- FAILED to send custom email. ---")
        print(f"Error: {e}")
        traceback.print_exc()


def custom_failure_alert(context):
    """Task‐level failure callback—logs and emails."""
    print("--- CUSTOM FAILURE ALERT INITIATED ---")
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    ti = context['task_instance']
    exc = context.get('exception')
    # fetch date from context, not from ti
    date_for_log = (
        context.get("logical_date")
        or context.get("data_interval_start")
        or datetime.utcnow()
    )
    date_iso = date_for_log.isoformat()

    log_message = (
        f"[{date_iso}] FAILURE: DAG={ti.dag_id}, TASK={ti.task_id}\n"
    )
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(log_message)
        print(f"--- Wrote to log: {LOG_FILE} ---")
    except Exception as e:
        print(f"!!! Could not write to log file: {e}")
        traceback.print_exc()

    subject = f"[Airflow] Task Failed: {ti.dag_id}.{ti.task_id}"
    body = f"""
    <h3>Task Failure</h3>
    <b>DAG:</b> {ti.dag_id}<br>
    <b>Task:</b> {ti.task_id}<br>
    <b>Run Date:</b> {date_iso}<br>
    <b>Error:</b><pre>{exc}</pre>
    <b>Logs:</b> <a href="{ti.log_url}">View Logs</a>
    """
    send_custom_email(subject, body)


# default_args to attach the callback to every task
default_args = {
    'on_failure_callback': custom_failure_alert,
}

with DAG(
    dag_id='minimal_failure_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=['minimal-test']
) as dag:

    failing_task = BashOperator(
        task_id='intentional_failure_task',
        bash_command='echo "This will fail" && exit 1',
        retries=0,
        # no need to set on_failure_callback again
    )
