#!/bin/bash
airflow scheduler &
airflow dag-processor &
airflow triggerer &
airflow api-server &
