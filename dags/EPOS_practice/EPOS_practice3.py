import json

import oracledb
from airflow.decorators import dag, task
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.discord.notifications.discord import DiscordNotifier
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import pendulum

from include.EPOS_practice.epos_tasks import _create_table
from include.EPOS_practice.minio import _create_bucket

vietnam_tz = 'Asia/Ho_Chi_Minh'
default_args = {
    'email': 'hieulata26102002@gmail.com'
}


@dag(
    start_date=pendulum.datetime(2024, 8, 22).in_tz(vietnam_tz),
    schedule='@daily',
    catchup=False,
    tags=['EPOS_practice3'],
    default_args=default_args,
    on_success_callback=DiscordNotifier(
        discord_conn_id='discord',
        text="The EPOS practice is succeeded"
    ),
    on_failure_callback=DiscordNotifier(
        discord_conn_id='discord',
        text="The EPOS practice is failed"
    )
)
def EPOS_practice3():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_oracle_available(ti):
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

    get_parquet_schema = DockerOperator(
        task_id='get_parquet_schema',
        image='airflow/stock-app',
        container_name='get_parquet_schema',
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
                source="C:/MyDataProject/Airflow/spark/notebooks/EPOS_practice/init_create_table_script.py",
                type="bind",
                read_only=False
            ),
            Mount(
                target="/app/return.json",
                source="C:/MyDataProject/Airflow/include/tmp/create_table.json",
                type="bind",
                read_only=False
            )
        ]
    )

    @task()
    def read_create_table_json():
        # print(ti.xcom_pull(task_ids='get_parquet_schema', key="return_value"))
        file_path = "/usr/local/airflow/include/tmp/create_table.json"
        with open(file_path, 'r') as file:
            content = json.load(file)

        return content

    @task()
    def create_table(ti):
        tmp_dict = ti.xcom_pull(task_ids="read_create_table_json")
        if tmp_dict is None:
            print("The script to create table is null")
        else:
            _create_table(tmp_dict)

    load_data_to_oracle_table = DockerOperator(
        task_id='load_data_to_table',
        image='airflow/stock-app',
        container_name='load_data_to_table',
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

    is_api_oracle_available() >> create_minio_bucket >> extract_to_minio >> join_tables >> get_parquet_schema >> read_create_table_json() >> create_table() >> load_data_to_oracle_table


EPOS_practice3()
