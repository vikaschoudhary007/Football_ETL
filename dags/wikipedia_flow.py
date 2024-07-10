from airflow import DAG
from datetime import datetime
import sys
import os

from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

default_args = {
    "owner": "Vikas",
    "start_date": datetime(2024,6,24)
}

dag = DAG(
    dag_id='wikipedia_flow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Extract
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

# Transform/preprocessing
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag
)

# Load/write
write_wikipedia_data = PythonOperator(
    task_id='write_wikipedia_data',
    python_callable=write_wikipedia_data,
    provide_context=True,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data
