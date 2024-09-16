from airflow.hooks.base import BaseHook
from minio import Minio


def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[-1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


def _create_bucket(bucket_name):
    print(bucket_name)
    client = _get_minio_client()
    if not client.bucket_exists(bucket_name=bucket_name):
        client.make_bucket(bucket_name=bucket_name)
    return bucket_name


# def _store_files(bucket_name, file, file_name):
#     client = _get_minio_client()
#     _create_bucket(bucket_name)
#     objw = client.put_object(
#         bucket_name=bucket_name,
#         object_name=file_name,
#         data=BytesIO(file),
#         length=len(file)
#     )
