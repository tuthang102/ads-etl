from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.executor import prepare_tables, transform_data, load_data

from datetime import datetime

with DAG(
    dag_id="etl_dag", start_date=datetime(2023, 4, 21), schedule_interval=None
) as dag:

    prep_tables = PythonOperator(
        task_id="prep_tables",
        python_callable=prepare_tables,
    )

    transform = PythonOperator(task_id="transform", python_callable=transform_data)

    load = PythonOperator(task_id="load", python_callable=load_data)


prep_tables >> transform
transform >> load
