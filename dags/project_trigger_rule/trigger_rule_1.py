from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 23),
    'retries': 1,
}

with DAG('website_data_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    download_website_A = EmptyOperator(
        task_id='download_website_A',
    )

    download_website_B = EmptyOperator(
        task_id='download_website_B',
    )

    process_website_A = EmptyOperator(
        task_id='process_website_A',
    )

    process_website_B = EmptyOperator(
        task_id='process_website_B',
    )

    merge_data = EmptyOperator(
        task_id='merge_data',
    )

    email_notification = EmptyOperator(
        task_id='email_notification'
    )

    slack_notification = EmptyOperator(
        task_id='slack_notification'
    )

    # Setting up dependencies
    download_website_A >> process_website_A
    download_website_B >> process_website_B

    process_website_A >> merge_data
    process_website_B >> merge_data

    merge_data >> [email_notification, slack_notification]
