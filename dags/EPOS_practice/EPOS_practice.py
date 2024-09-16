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
    tags=['EPOS_practice'],
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
def EPOS_practice():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available(ti):
        api = BaseHook.get_connection('oracle')
        login = api.login
        password = api.password
        host = api.host
        port = api.port
        schema = api.schema

        try:
            # Attempt to establish a connection using oracledb
            connection = oracledb.connect(
                user=login,
                password=password,
                dsn=f"{host}:{port}/{schema}"
            )
            # If connection is successful, close it
            connection.close()
            print("Successfully connected to Oracle database")
            # Push XCom Value
            ti.xcom_push(key="oracle_status", value="Connection successful")
            return True
        except oracledb.Error as e:
            error_message = f"Error connecting to Oracle database: {str(e)}"
            print(error_message)
            ti.xcom_push(key="oracle_status", value=error_message)
            return False

    create_minio_bucket = PythonOperator(
        task_id='create_minio_bucket',
        python_callable=_create_bucket,
        op_kwargs={'bucket_name': 'epos-practice'},
    )

    extract_to_minio = DockerOperator(
        task_id='extract_to_minio',
        image='airflow/stock-app',
        container_name='extract_to_minio',
        api_version='auto',
        auto_remove='force',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_PYTHON_LOCATION': '/app/execute_file.py',
            # 'BUCKET_NAME': '{{task_instance.xcom_pull(task_ids="create_minio_bucket")}}'
        },
        mounts=[
            Mount(
                target="/app/execute_file.py",
                source="C:/MyDataProject/Airflow/spark/notebooks/EPOS_practice/extract2minio.py",
                type="bind",
                read_only=False
            )
        ]
    )

    join_tables = DockerOperator(
        task_id='join_tables',
        image='airflow/stock-app',
        container_name='join_tables',
        api_version='auto',
        auto_remove='force',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_PYTHON_LOCATION': '/app/execute_file.py',
            # 'BUCKET_NAME': '{{task_instance.xcom_pull(task_ids="create_minio_bucket")}}'
        },
        mounts=[
            Mount(
                target="/app/execute_file.py",
                source="C:/MyDataProject/Airflow/spark/notebooks/EPOS_practice/join_tables.py",
                type="bind",
                read_only=False
            )
        ]
    )

    drop_target_table = aql.drop_table(
        table=Table(
            name='join_tables',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        )
    )

    load_to_dw_first_file = _load_to_dw_first_file("epos-practice/JOIN_TABLES")

    @task_group(
        tooltip="Load all parquet file to postgres DB"
    )
    def load_to_dw_left():
        num_of_files = _get_number_of_files("epos-practice/JOIN_TABLES")
        if num_of_files > 1:
            _load_to_dw_left_files("epos-practice/JOIN_TABLES")
        else:
            pass

    is_api_available() >> create_minio_bucket >> extract_to_minio >> join_tables >> drop_target_table >> load_to_dw_first_file >> load_to_dw_left()
    # load_tasks


EPOS_practice()
