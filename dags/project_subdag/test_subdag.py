import airflow
from include.subdags.subdag import factory_subdag
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators.task_group import task_group
from airflow.executors.local_executor import LocalExecutor

DAG_NAME = "test_subdag"

default_args = {
    'owner': "Airflow",
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(dag_id=DAG_NAME, default_args=default_args, schedule="@daily") as dag:
    start = EmptyOperator(
        task_id='start'
    )

    subdag_1 = SubDagOperator(
        task_id='subdag-1',
        subdag=factory_subdag(DAG_NAME, 'subdag-1', default_args),
        executor_config=LocalExecutor(),
    )

    some_other_task = EmptyOperator(
        task_id='check',
    )

    subdag_2 = SubDagOperator(
        task_id='subdag-2',
        subdag=factory_subdag(DAG_NAME, 'subdag-2', default_args),
        executor_config=LocalExecutor(),
    )

    @task_group(tooltip="Tasks for subdag_3")
    def subdag_3():
        for i in range(5):
            EmptyOperator(
                task_id=f'subdag-3-task-{i}'
            )

    end = EmptyOperator(
        task_id='final'
    )

    start >> subdag_1 >> some_other_task >> subdag_2 >> subdag_3() >> end
