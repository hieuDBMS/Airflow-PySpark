from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(hours=2)
)
def my_dag():
    pass

my_dag()