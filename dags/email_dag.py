
from __future__ import annotations
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

from airflow.models.dag import DAG
# Note: This is the updated, non-deprecated import path for PythonOperator
from airflow.operators.python import PythonOperator


# ========== PART 1: Your Email Script as a Callable Function ==========
def send_standalone_email_alert():
    """
    This function contains the exact logic from your test script.
    It will be executed by the PythonOperator.
    """
    # --- Credentials from your script ---
    SMTP_HOST = "smtp.gmail.com"
    SMTP_PORT = 587
    SMTP_SENDER = "dhaneshdhakrey@gmail.com" 
    SMTP_PASSWORD = "****" 
    RECIPIENT_EMAIL = "dsddhakrey421@gmail.com" 

    print("--- Task Started: Attempting to send an email from within an Airflow task... ---")

    try:
        # Create the email message
        msg = MIMEText("This is a test email sent directly from an Airflow PythonOperator.")
        msg['Subject'] = 'Airflow PythonOperator SMTP Test'
        msg['From'] = SMTP_SENDER
        msg['To'] = RECIPIENT_EMAIL

        # Connect to the server
        print(f"Connecting to {SMTP_HOST}:{SMTP_PORT}...")
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        
        # Start TLS encryption
        print("Starting TLS...")
        server.starttls()
        
        # Log in to the server
        print(f"Logging in as {SMTP_SENDER}...")
        server.login(SMTP_SENDER, SMTP_PASSWORD)
        
        # Send the email
        print(f"Sending email to {RECIPIENT_EMAIL}...")
        server.sendmail(SMTP_SENDER, [RECIPIENT_EMAIL], msg.as_string())
        
        print("\n--- SUCCESS! Email sent successfully from Airflow task. ---")
    
    except Exception as e:
        print(f"\n--- FAILED: An error occurred within the Airflow task. ---")
        print(f"Error type: {type(e).__name__}")
        print(f"Error details: {e}")
        # Raising the exception will cause the Airflow task to fail
        raise
    
    finally:
        if 'server' in locals() and server:
            server.quit()
            print("Connection closed.")

# ========== PART 2: The DAG Definition ==========
with DAG(
    dag_id='standalone_email_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['standalone-test'],
) as dag:
    # This is the task that will call your function
    send_email_task = PythonOperator(
        task_id='send_standalone_email_task',
        # The 'python_callable' argument points to the function to execute
        python_callable=send_standalone_email_alert,
    )

