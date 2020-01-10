from datetime import datetime, timedelta
import time
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pprint import pprint


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("tutorial_print_task", default_args=default_args)


def print_times(string):
    print(string)

task_first = PythonOperator(
    task_id="first_task",
    python_callable=print_times,
    op_kwargs={"string": "this is first task"},
    dag=dag,
)

task_second = PythonOperator(
    task_id="second_task",
    python_callable=print_times,
    op_kwargs={"string": "this is second task"},
    dag=dag,
)

task_third = PythonOperator(
    task_id="third_task",
    python_callable=print_times,
    op_kwargs={"string": "this is third task"},
    dag=dag,
)

task_first >> task_second >> task_third