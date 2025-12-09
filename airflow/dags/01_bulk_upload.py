import os
import glob
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def upload_files_to_s3():
    s3_endpoint = 'http://localstack:4566'
    access_key = 'test'
    secret_key = 'test'
    bucket_name = 'my-helsinki-bikes-bucket'
    source_dir = '/opt/airflow/data/processed'

    s3 = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'
    )

    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception:
        pass

    files = glob.glob(os.path.join(source_dir, '*.csv'))

    for file_path in files:
        file_name = os.path.basename(file_path)
        print(f"Uploading {file_name} to s3://{bucket_name}/raw/")
        s3.upload_file(file_path, bucket_name, f"raw/{file_name}")

with DAG(
    dag_id='01_bulk_upload_to_s3',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_all_files',
        python_callable=upload_files_to_s3
    )