from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'owner': 'Airflow'
}


def second_task():
    print('Hello from second_task')
    # raise ValueError('This will turns the python task in failed state')


def third_task():
    # print('Hello from third_task')
    raise ValueError('This will turns the python task in failed state')


@dag(
    schedule="@daily",
    default_args=default_args,
    tags=['depend_tasks'],
    catchup=False
)
def depend_tasks():
    # Task 1
    bash_task_1 = BashOperator(task_id='bash_task_1', bash_command="echo 'first task'", wait_for_downstream=True)

    # Task 2
    python_task_2 = PythonOperator(task_id='python_task_2', python_callable=second_task, wait_for_downstream=True)

    # Task 3
    python_task_3 = PythonOperator(task_id='python_task_3', python_callable=third_task)

    bash_task_1 >> python_task_2 >> python_task_3


depend_tasks()
