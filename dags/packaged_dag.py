from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from dags.functions.helpers import first_task, second_task, third_task

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow',
    'email': 'hieulata26102002@gmail.com'
}

with DAG(dag_id='packaged_dag', schedule="0 0 * * *", default_args=default_args) as dag:

    # Task 1
    python_task_1 = PythonOperator(task_id='python_task_1', python_callable=first_task)

    # Task 2
    python_task_2 = PythonOperator(task_id='python_task_2', python_callable=second_task)

    # Task 3
    python_task_3 = PythonOperator(task_id='python_task_3', python_callable=third_task)

    python_task_1 >> python_task_2 >> python_task_3