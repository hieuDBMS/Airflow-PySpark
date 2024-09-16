from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import DagBag
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'email': ['hieulata26102002@gmail.com']
}


@dag(
    start_date=datetime.now(),
    schedule='@daily',
    catchup=False,
    tags=['show_dag_bag'],
    default_args=default_args
)
def show_dag_bag():
    @task
    def show_dag():
        dag_bag = DagBag()
        dags = dag_bag.dags
        print(dag_bag.dagbag_stats) # [FileLoadStat(file='/EPOS_practice/EPOS_practice.py', duration=datetime.timedelta(microseconds=936408), dag_num=1, task_num=12, dags="['EPOS_practice']"),...]
        print(dag_bag.get_dag("tst_dag").task)

        # Print information about each DAG
        for dag_id, dag in dags.items():
            print(f"DAG ID: {dag_id}")
            print(f"  Description: {dag.description}")
            print(f"  Schedule Interval: {dag.schedule_interval}")
            print(f"  Start Date: {dag.start_date}")
            print(f"  Number of Tasks: {len(dag.tasks)}")
            print("This is email:", dag.default_args.get('email', []))
            print("-" * 30)

    show_dag()


show_dag_bag()
