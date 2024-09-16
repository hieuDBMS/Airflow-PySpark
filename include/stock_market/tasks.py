import json
import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO

BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[-1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metric=high?&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock):
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)  # Convert json string to json python dictionary
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')  # Convert the json python dictionary to json string
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),  # creates an in-memory binary stream for the data
        length=len(data)  # This tells MiniO the exact size of data in bytes
    )

    return f'{objw.bucket_name}/{symbol}'


def _get_formatted_csv(path):
    # path = 'stock-market/AAPL'
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[-1]}/formatted_prices/"
    objects = client.list_objects(bucket_name=BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    raise AirflowNotFoundException('The csv file does not exist')