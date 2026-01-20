import os
import glob
import logging
import boto3
from airflow.decorators import dag, task
from datetime import datetime
from typing import List

try:
    from dag_config import (
        S3_ENDPOINT, AWS_CREDS, BUCKET_NAME,
        PROCESSED_DIR, RAW_PREFIX
    )
except ImportError:
    from airflow.dags.dag_config import (
        S3_ENDPOINT, AWS_CREDS, BUCKET_NAME,
        PROCESSED_DIR, RAW_PREFIX
    )

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dag(
    dag_id='01_bulk_upload_to_s3_v6',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['s3', 'upload']
)
def bulk_upload_dag():
    """
    dag to upload all csv files to s3 using dynamic task mapping.
    """

    @task
    def ensure_bucket():
        """
        checks if bucket exists and creates it if necessary.
        """
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            logger.info(f"bucket {BUCKET_NAME} exists")
        except Exception:
            logger.info(f"creating bucket {BUCKET_NAME}")
            s3.create_bucket(Bucket=BUCKET_NAME)

    @task
    def get_file_list() -> List[str]:
        """
        scans the directory and returns a list of csv file paths.
        """
        files = glob.glob(os.path.join(PROCESSED_DIR, '*.csv'))
        logger.info(f"found {len(files)} files to upload")
        return files

    @task
    def upload_single_file(file_path: str):
        """
        uploads a single file to s3.
        """
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)

        file_name = os.path.basename(file_path)
        object_key = f"{RAW_PREFIX}/{file_name}"

        logger.info(f"uploading {file_name} to {BUCKET_NAME}/{object_key}")
        s3.upload_file(file_path, BUCKET_NAME, object_key)

    bucket_ready = ensure_bucket()
    files_to_process = get_file_list()

    upload_tasks = upload_single_file.expand(file_path=files_to_process)
    bucket_ready >> files_to_process >> upload_tasks


bulk_upload_dag()