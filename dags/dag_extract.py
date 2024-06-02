import os
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract_orders import main as extract_orders
from extract_sales import main as extract_sales
from transform_sales_orders import main as transform_sales_orders

# test


def get_name():
    return os.path.splitext(os.path.basename(__file__))[0]


with DAG(
    dag_id=get_name(),
    catchup=False,
    start_date=datetime(2024, 5, 25),
    schedule="@hourly",
    tags=["produces", "dataset-scheduled"],
) as dag_orders:
    extract_orders_task = PythonOperator(
        task_id="extract_orders_task", python_callable=extract_orders
    )
    extract_sales_task = PythonOperator(
        task_id="extract_sales_task", python_callable=extract_sales
    )
    transform_sales_orders_task = PythonOperator(
        task_id="transform_sales_orders_task", python_callable=transform_sales_orders
    )


[extract_orders_task, extract_sales_task] >> transform_sales_orders_task
