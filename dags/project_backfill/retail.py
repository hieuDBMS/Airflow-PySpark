from airflow.decorators import dag, task
from datetime import datetime, timedelta
from time import sleep


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['test'],
)
def retail():
    @task()
    def start(**context):
        print(f"This is date: {context['data_interval_end']}")
        # sleep(60*5)
        # raise ValueError("Error")

    start()


retail()
