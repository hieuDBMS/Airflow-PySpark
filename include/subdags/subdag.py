from airflow.decorators import dag
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


def factory_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
            # dag_id='%s.%s' %(parent_dag_name, child_dag_name)
            dag_id=f"{parent_dag_name}.{child_dag_name}",
            default_args=default_args
    ) as dag:
        for i in range(5):
            EmptyOperator(
                task_id=f"{child_dag_name}-task-{i + 1}"
            )

    return dag
