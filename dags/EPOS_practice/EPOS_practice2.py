from airflow.decorators import dag, task
from airflow.decorators.task_group import task_group
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.providers.discord.notifications.discord import DiscordNotifier
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow.models.taskinstance import TaskInstance
from include.EPOS_practice.epos_tasks import _get_file_names, _get_number_of_files, _load_to_dw_first_file, \
    _load_to_dw_left_files
from include.EPOS_practice.minio import _create_bucket, _get_minio_client
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
import oracledb
from docker.types import Mount

default_args = {
    'email': 'hieulata26102002@gmail.com'
}


@dag(
    start_date=datetime.now(),
    schedule='@daily',
    catchup=False,
    tags=['EPOS_practice2'],
    on_success_callback=DiscordNotifier(
        discord_conn_id='discord',
        text="The EPOS practice is succeeded"
    ),
    on_failure_callback=DiscordNotifier(
        discord_conn_id='discord',
        text="The EPOS practice is failed"
    ),
    default_args=default_args
)
def EPOS_practice2():

    join_tables = DockerOperator(
        task_id='test_connect',
        image='airflow/stock-app',
        container_name='test_connect',
        api_version='auto',
        auto_remove='force',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                target="/app/execute_file.py",
                source="C:/MyDataProject/Airflow/spark/notebooks/EPOS_practice/load_to_oracle.py",
                type="bind",
                read_only=False
            )
        ]
    )


    join_tables
    # load_tasks


EPOS_practice2()
