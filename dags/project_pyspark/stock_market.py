from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.providers.discord.notifications.discord import DiscordNotifier
from airflow.models import Variable
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from docker.types import Mount

SYMBOL = 'AAPL'


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=DiscordNotifier(
        discord_conn_id='discord',
        text="""The DAG stock_market has succeeded""",
        username='Bộ Công An',
        avatar_url="https://www.google.com/imgres?q=sausage%20dog&imgurl=https%3A%2F%2Fcdn.britannica.com%2F13%2F234213-050-45F47984%2Fdachshund-dog.jpg&imgrefurl=https%3A%2F%2Fwww.britannica.com%2Fanimal%2Fdachshund&docid=v3Kq4mjqJr5axM&tbnid=4R-YZcOIC3qwiM&vet=12ahUKEwiqk_nTjPSHAxUyk1YBHc83HX4QM3oECB0QAA..i&w=1600&h=1041&hcb=2&ved=2ahUKEwiqk_nTjPSHAxUyk1YBHc83HX4QM3oECB0QAA"
    ),
    on_failure_callback=DiscordNotifier(
        discord_conn_id='discord',
        text='The DAG stock_market has failed',
        username='Bộ Công An',
        avatar_url="https://www.google.com/imgres?q=sausage%20dog&imgurl=https%3A%2F%2Fcdn.britannica.com%2F13%2F234213-050-45F47984%2Fdachshund-dog.jpg&imgrefurl=https%3A%2F%2Fwww.britannica.com%2Fanimal%2Fdachshund&docid=v3Kq4mjqJr5axM&tbnid=4R-YZcOIC3qwiM&vet=12ahUKEwiqk_nTjPSHAxUyk1YBHc83HX4QM3oECB0QAA..i&w=1600&h=1041&hcb=2&ved=2ahUKEwiqk_nTjPSHAxUyk1YBHc83HX4QM3oECB0QAA"
    )
    # on_success_callback=SlackNotifier(
    #     slack_conn_id='slack',
    #     text='The DAG stock_market has succeeded',
    #     channel='general'
    # ),
    # on_failure_callback=SlackNotifier(
    #         slack_conn_id='slack',
    #         text='The DAG stock_market has failed',
    #         channel='general'
    # )
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url=url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}',
            'symbol': SYMBOL
        }
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={
            'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}' # ti / task_instance the code will run (
        }
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='force',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        },
        mounts=[
            Mount(
                target="/app/stock_transform.py",  # Path inside the container
                source="C:/MyDataProject/Airflow/spark/notebooks/stock_transform/stock_transform.py",  # Path on the host machine
                type="bind",  # Mount type (bind is used for mounting host files)
                read_only=False  # Set to True if you want the mount to be read-only
            )
        ]

    )
    # Define the Mount object

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs= {
            'path': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }

    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path=f"s3://{BUCKET_NAME}/{{{{task_instance.xcom_pull(task_ids='get_formatted_csv')}}}}", conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        if_exists="replace"
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
    # store_stock_prices


stock_market()
