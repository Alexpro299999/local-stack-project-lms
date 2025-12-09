import os
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

SOURCE_DIR = '/opt/airflow/data/processed'
TARGET_FILE = '2017-05.csv'
BUCKET_NAME = 'my-helsinki-bikes-bucket'
S3_ENDPOINT = 'http://localstack:4566'
AWS_CREDS = {'aws_access_key_id': 'test', 'aws_secret_access_key': 'test', 'region_name': 'us-east-1'}

def ensure_bucket_exists():
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception:
        pass

def upload_raw_file():
    ensure_bucket_exists()
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
    file_path = os.path.join(SOURCE_DIR, TARGET_FILE)
    
    s3.upload_file(file_path, BUCKET_NAME, f"raw/{TARGET_FILE}")

def calculate_and_upload_metrics():
    ensure_bucket_exists()
    file_path = os.path.join(SOURCE_DIR, TARGET_FILE)
    df = pd.read_csv(file_path)

    dep_counts = df['departure_name'].value_counts().reset_index()
    dep_counts.columns = ['station_name', 'departure_count']

    ret_counts = df['return_name'].value_counts().reset_index()
    ret_counts.columns = ['station_name', 'return_count']

    metrics_df = pd.merge(dep_counts, ret_counts, on='station_name', how='outer').fillna(0)
    
    metrics_filename = f"metrics_{TARGET_FILE}"
    metrics_path = os.path.join(SOURCE_DIR, metrics_filename)
    metrics_df.to_csv(metrics_path, index=False)

    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
    s3.upload_file(metrics_path, BUCKET_NAME, f"metrics/{metrics_filename}")

with DAG(
    dag_id='02_process_single_file_metrics',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task_upload_raw = PythonOperator(
        task_id='upload_raw_csv',
        python_callable=upload_raw_file
    )

    task_calc_metrics = PythonOperator(
        task_id='calculate_and_upload_metrics',
        python_callable=calculate_and_upload_metrics
    )

    task_upload_raw >> task_calc_metrics