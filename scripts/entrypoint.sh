#!/usr/bin/env bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command -v pip) install --user -r requirements.txt
fi

# Initialize the database
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@domain.com \
    --password admim

fi

exec airflow webserver
