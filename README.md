# FlowOps-Engine

A comprehensive Apache Airflow-based ETL orchestration engine for data pipeline management and workflow automation. This project handles data ingestion, transformation, and reporting for the AU 360 analytics platform.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Overview](#project-overview)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Running the Project](#running-the-project)
- [Project Structure](#project-structure)
- [Features](#features)
- [Troubleshooting](#troubleshooting)
- [Security Notes](#security-notes)

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python**: 3.7 or higher
- **Apache Airflow**: 2.x or higher
- **Git**: Latest version
- **Virtual Environment**: Python venv or Conda
- **Database**: PostgreSQL (for Airflow metadata)
- **Hive/Hadoop**: Access to Hive server for data queries
- **SSH Access**: To remote servers (10.59.\*\*\* hosts)
- **pip**: Python package manager

## Project Overview

FlowOps-Engine is an Airflow-based data orchestration platform that automates:

- **Data Ingestion**: From various sources (feeds, stories, metrics)
- **Data Transformation**: Processing through Hive queries
- **Report Generation**: Creating analytics reports
- **Workflow Scheduling**: Using cron expressions and Airflow DAGs
- **Error Handling**: With custom alerting and retry logic
- **Logging**: Comprehensive logging for debugging

### Key Components

- **DAGs**: Workflow definitions for various ETL processes
- **Scripts**: Python scripts for data processing and transformations
- **Utilities**: Logging, email alerts, and helper functions
- **Templates**: HTML email templates for notifications

## Installation & Setup

### Step 1: Clone the Repository

```bash
git clone
cd FlowOps-Engine
```

### Step 2: Create a Virtual Environment

**Using Python venv:**

```bash
python -m venv airflow_env
source airflow_env/bin/activate  # On Windows: airflow_env\Scripts\activate
```

**Using Conda:**

```bash
conda create -n flowops-engine python=3.9
conda activate flowops-engine
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

If `requirements.txt` is not present, install Airflow and dependencies:

```bash
pip install apache-airflow==2.5.0
pip install apache-airflow-providers-ssh
pip install apache-airflow-providers-postgres
pip install pyhive[presto]
pip install paramiko
pip install requests
pip install pandas
pip install sqlalchemy
```

### Step 4: Initialize Airflow

Set the Airflow home directory:

```bash
# On Linux/Mac
export AIRFLOW_HOME=~/airflow

# On Windows (PowerShell)
$env:AIRFLOW_HOME = "$env:USERPROFILE\airflow"
```

Initialize the database:

```bash
airflow db init
```

### Step 5: Create an Airflow Admin User

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password <your_password>
```

### Step 6: Copy Project Files

Copy the DAGs and scripts to the Airflow environment:

```bash
cp -r dags/ $AIRFLOW_HOME/dags/
cp -r scripts/ $AIRFLOW_HOME/dags/
cp -r utils/ $AIRFLOW_HOME/dags/
cp -r templates/ $AIRFLOW_HOME/dags/
```

## Configuration

### 1. Database Connection

Update the Airflow configuration for the metadata database:

**File**: `airflow.cfg`

```ini
[database]
sql_alchemy_conn = postgresql://user:password@localhost/airflow
```

### 2. Hive Connection

Create a Hive connection in Airflow UI:

1. Navigate to **Admin → Connections**
2. Click **Create**
3. Fill in the following:
   - **Conn Id**: `hive_default`
   - **Conn Type**: `Hive`
   - **Host**: `10.59.***` (Your Hive server)
   - **Port**: `10000`
   - **Database**: `au_360`

### 3. SSH Connection

Create an SSH connection for remote execution:

1. Navigate to **Admin → Connections**
2. Click **Create**
3. Fill in the following:
   - **Conn Id**: `ssh_default`
   - **Conn Type**: `SSH`
   - **Host**: `10.59.***` (Your SSH server)
   - **Port**: `2122`
   - **Username**: (Use Airflow Variables or Secrets)
   - **Password**: (Use Airflow Variables or Secrets)

### 4. Email Configuration

Update email settings in `airflow.cfg`:

```ini
[email]
email_backend = airflow.providers.smtp.utils.EmailBackend
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_user = your_email@gmail.com
smtp_password = your_app_password
smtp_mail_from = your_email@gmail.com
```

### 5. Environment Variables

Create a `.env` file in the project root:

```bash
AIRFLOW_HOME=~/airflow
AIRFLOW_UID=1000
AIRFLOW_GID=0
HIVE_HOST=10.59.***
HIVE_PORT=10000
SSH_HOST=10.59.***
SSH_PORT=2122
```

## Running the Project

### Start Airflow Services

**Start the Scheduler:**

```bash
airflow scheduler
```

**Start the Web Server (in another terminal):**

```bash
airflow webserver --port 8080
```

The Web UI will be available at: `http://localhost:8080`

### Enable DAGs

1. Go to the Airflow Web UI
2. Look for your DAGs in the list
3. Click the toggle to enable them

### Trigger a DAG

```bash
# List available DAGs
airflow dags list

# Trigger a specific DAG
airflow dags trigger ssh_crontab_driver_dag_test
```

### Monitor Execution

- **Web UI**: Navigate to your DAG and view task status
- **Logs**: Check logs in `$AIRFLOW_HOME/logs/`

## Project Structure

```
FlowOps-Engine/
├── dags/                          # Airflow DAG definitions
│   ├── test_driver.py            # Main workflow driver
│   ├── driver.py                 # Backup driver
│   ├── hadoop.py                 # Hadoop operations
│   ├── hive_health.py            # Hive health check
│   ├── email_dag.py              # Email testing
│   ├── minimal.py                # Minimal DAG example
│   └── ...
├── scripts/                       # ETL processing scripts
│   ├── stories_views.py          # Story views metrics
│   ├── feed_*.py                 # Feed-related metrics
│   ├── unit_*.py                 # Unit-level reports
│   ├── author.py                 # Author metrics
│   ├── partner.py                # Partner analytics
│   ├── generic_report.py         # Generic reporting
│   └── ...
├── utils/                         # Utility modules
│   ├── logging_config1.py        # Logging configuration
│   ├── cron_job_email.py         # Email alerting
│   └── testmail.py               # Email testing
├── templates/                     # HTML email templates
│   └── task_fail_alert.html      # Failure notification template
├── data/                          # Data storage (local)
├── requirements.txt               # Python dependencies
├── start-airflow.sh              # Quick start script
└── README.md                      # This file
```

## Features

- **Automated Workflow Scheduling**: Cron-based task scheduling
- **Error Handling & Alerts**: Email notifications on task failures
- **Remote Execution**: SSH-based task execution on remote servers
- **Data Pipeline Orchestration**: Multi-step ETL processes
- **Health Checks**: Hive and system health monitoring
- **Custom Logging**: Structured logging for all operations
- **Flexible Reporting**: Generate various analytics reports
- **Task Retry Logic**: Automatic retry mechanisms

## Troubleshooting

### Issue: Airflow Database Not Initialized

**Solution:**

```bash
airflow db init
airflow db upgrade
```

### Issue: Connection Refused to Hive

**Solution:**

- Verify Hive server is running
- Check network connectivity to Hive host
- Verify connection credentials in Airflow UI
- Check firewall rules

### Issue: SSH Connection Timeouts

**Solution:**

- Verify SSH host and port are correct
- Check SSH credentials
- Ensure SSH key has proper permissions (400)
- Verify network connectivity

### Issue: DAGs Not Appearing

**Solution:**

1. Check DAG files are in `$AIRFLOW_HOME/dags/`
2. Verify DAG syntax: `python dags/your_dag.py`
3. Check Airflow logs: `$AIRFLOW_HOME/logs/`
4. Restart scheduler: `airflow scheduler`

### Issue: Email Not Sending

**Solution:**

- Verify SMTP configuration in `airflow.cfg`
- Check email credentials and app passwords
- Enable "Less secure app access" if using Gmail
- Test email with: `python utils/testmail.py`

### Issue: Python Module Import Errors

**Solution:**

```bash
# Reinstall dependencies
pip install -r requirements.txt

# Add project to Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

## Security Notes

### Important: Credentials Management

⚠️ **All hardcoded credentials have been masked with placeholders:**

- Usernames → `username`
- Passwords → `****`
- IP Addresses → `10.59.***`

### To Use This Project Safely:

1. **Never commit credentials** with actual values
2. **Use Airflow Secrets** or environment variables for sensitive data
3. **Use SSH keys** instead of passwords for SSH connections
4. **Enable two-factor authentication** on sensitive accounts
5. **Review and update** the `simple_auth_manager_passwords.json.generated` file

### Setting Credentials via Environment Variables:

```bash
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql://user:pass@localhost/airflow"
export AIRFLOW__SMTP__SMTP_USER="your_email@gmail.com"
export AIRFLOW__SMTP__SMTP_PASSWORD="your_app_password"
```

## Support & Maintenance

- Review logs regularly: `$AIRFLOW_HOME/logs/`
- Monitor DAG performance in the Web UI
- Schedule regular database maintenance
- Update dependencies periodically: `pip install --upgrade -r requirements.txt`

## License

[Add your license information here]

## Contact

For questions or issues, please contact the project maintainers or create an issue in the repository.

---

**Last Updated**: February 2026
