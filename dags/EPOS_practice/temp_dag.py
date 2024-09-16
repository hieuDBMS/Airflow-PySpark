from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'email': 'hieulata26102002@gmail.com'
}


@dag(
    start_date=datetime.now(),
    schedule='@daily',
    catchup=False,
    tags=['temp_dag'],
    default_args=default_args
)
def temp_dag():
    @task()
    def print_schema():
        print("duma")

    print_schema()


temp_dag()
