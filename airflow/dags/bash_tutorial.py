from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'bash_tutorial',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
)

bash_command = """
    echo output_st_start >> /root/airflow/output/test.txt
    date >> /root/airflow/output/test.txt
    echo output_st_end >> /root/airflow/output/test.txt
"""

t1 = BashOperator(
    task_id='output_start',
    bash_command=bash_command,
    dag=dag,
)

bash_command = """
    echo output1start >> /root/airflow/output/test.txt
    date >> /root/airflow/output/test.txt
    echo output1end >> /root/airflow/output/test.txt
    sleep 1
"""

t2 = BashOperator(
    task_id='output1',
    depends_on_past=False,
    bash_command=bash_command,
    dag=dag,
)

bash_command = """
    echo output2start >> /root/airflow/output/test.txt
    date >> /root/airflow/output/test.txt
    echo output2end >> /root/airflow/output/test.txt
    sleep 1
"""

t3 = BashOperator(
    task_id='output2',
    depends_on_past=False,
    bash_command=bash_command,
    dag=dag,
)

bash_command = """
    echo output_fin_start >> /root/airflow/output/test.txt
    date >> /root/airflow/output/test.txt
    echo output_fin_end >> /root/airflow/output/test.txt
"""

t4 = BashOperator(
    task_id='output_finish',
    depends_on_past=False,
    bash_command=bash_command,
    dag=dag,
)

t1 >> [t2, t3] >> t4