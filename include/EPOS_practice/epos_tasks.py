from airflow.exceptions import AirflowNotFoundException, AirflowException
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from include.EPOS_practice.minio import _get_minio_client
from airflow.hooks.base import BaseHook
import pyarrow.parquet as pq
import oracledb


def _get_file_names(path):
    files = []
    client = _get_minio_client()
    bucket_name = path.split("/")[0]
    prefix_name = "/".join(path.split("/")[1:])
    objects = client.list_objects(
        bucket_name=bucket_name,
        prefix=prefix_name,
        recursive=True
    )

    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            print(obj.object_name)
            files.append(bucket_name + "/" + obj.object_name)
    return files


def _get_number_of_files(path):
    count = 0
    client = _get_minio_client()
    bucket_name = path.split("/")[0]
    prefix_name = "/".join(path.split("/")[1:])
    objects = client.list_objects(
        bucket_name=bucket_name,
        prefix=prefix_name,
        recursive=True
    )

    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            count += 1
    return count


def _load_to_dw_first_file(path):
    files = _get_file_names(path)
    tmp = aql.load_file(
        task_id=f'load_to_dw_0',
        input_file=File(
            path=f"s3://{files[0]}",
            conn_id='minio'
        ),
        output_table=Table(
            name='join_tables',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        use_native_support=True,
        if_exists="replace"
    )
    return tmp


def _load_to_dw_left_files(path):
    my_list = []
    files = _get_file_names(path)

    for i in range(1, len(files)):
        print(files[i])
        tmp = aql.load_file(
            task_id=f'load_to_dw_{i}',
            input_file=File(
                path=f"s3://{files[i]}",
                conn_id='minio'
            ),
            output_table=Table(
                name='join_tables',
                conn_id='postgres',
                metadata=Metadata(
                    schema='public'
                )
            ),
            use_native_support=True,
            if_exists="append"
        )
        my_list.append(tmp)
    return my_list


def _create_table(create_table_dict):
    table_name = create_table_dict['table_name']
    create_table_script = create_table_dict["create_table_script"]
    print(create_table_script)
    api = BaseHook.get_connection('oracle')
    login = api.login
    password = api.password
    host = api.host
    port = api.port
    schema = api.schema
    connection_parameter = {
        'user': login,
        'password': password,
        'dsn': f"{host}:{port}/{schema}"
    }
    try:
        with oracledb.connect(**connection_parameter) as conn:
            with conn.cursor() as cursor:
                # Execute the CREATE TABLE statement
                cursor.execute(create_table_script)
                print(f"Table {table_name} is created successfully.")
    except oracledb.Error as e:
        print(f"Error creating table {table_name}: {e}")


