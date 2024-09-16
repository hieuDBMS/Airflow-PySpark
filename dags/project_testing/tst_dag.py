import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
default_args = {
    'start_date': datetime(2024, 1, 1),
}


def process():
    return 'process'


@dag(
    start_date=pendulum.datetime(2024, 1, 1),
    schedule='0 0 * * *',
    default_args=default_args,
    catchup=False,
    tags=['tst_dag']
)
def tst_dag():
    task_1 = EmptyOperator(task_id='task_1')
    task_2 = EmptyOperator(task_id='task_2')

    # Tasks dynamically generated
    tasks = [EmptyOperator(task_id=f"task_{t}") for t in range(3, 6)]

    task_6 = EmptyOperator(task_id='task_6')

    task_1 >> task_2 >> tasks >> task_6


tst_dag()
