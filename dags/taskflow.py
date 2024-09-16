from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator


def _task_d():
    print("task_d")


def _task_e():
    print("task_e")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["taskflow"]
)
def taskflow():
    @task
    def task_a():
        print("task A")
        return 42

    @task
    def task_b(value):
        print("task B")
        # print(ti.xcom_pull(task_ids='task_a'))
        print(value)

    task_b(task_a())
    # task_a() >> task_b()


taskflow()
