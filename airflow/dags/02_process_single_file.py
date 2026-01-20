import os
import boto3
import logging
import glob
import shutil
from airflow.decorators import dag, task
from datetime import datetime
from typing import List

try:
    from pyspark.sql import SparkSession
except ImportError:
    pass

try:
    from dag_config import (
        S3_ENDPOINT, AWS_CREDS, BUCKET_NAME,
        PROCESSED_DIR, RAW_PREFIX, METRICS_PREFIX
    )
except ImportError:
    from airflow.dags.dag_config import (
        S3_ENDPOINT, AWS_CREDS, BUCKET_NAME,
        PROCESSED_DIR, RAW_PREFIX, METRICS_PREFIX
    )

logger = logging.getLogger(__name__)


@dag(
    dag_id='02_process_files_metrics_spark_v3',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def process_files_dag():
    """
    dag to process csv files using spark and upload metrics to s3.
    """

    @task
    def ensure_bucket_exists():
        """
        ensures the s3 bucket exists.
        """
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
        except Exception:
            logger.info(f"creating bucket {BUCKET_NAME}")
            s3.create_bucket(Bucket=BUCKET_NAME)

    @task
    def get_raw_files() -> List[str]:
        """
        gets list of files to process simulating partitions.
        """
        files = glob.glob(os.path.join(PROCESSED_DIR, '*.csv'))
        if not files:
            logger.warning("no files found to process")
        return files

    @task
    def upload_raw_file(file_path: str) -> str:
        """
        uploads the raw csv to s3 and returns the local file path.
        """
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
        filename = os.path.basename(file_path)
        key = f"{RAW_PREFIX}/{filename}"

        logger.info(f"uploading raw file {filename}...")
        s3.upload_file(file_path, BUCKET_NAME, key)

        return file_path

    @task
    def calculate_and_upload_metrics(file_path: str):
        """
        calculates metrics using pyspark and uploads result to s3.
        """
        logger.info(f"starting spark session for {file_path}")

        spark = SparkSession.builder \
            .appName("HelsinkiBikesMetrics") \
            .master("local[*]") \
            .getOrCreate()

        try:
            df = spark.read.option("header", "true").csv(file_path)

            dep_counts = df.groupBy("departure_name").count() \
                .withColumnRenamed("count", "departure_count") \
                .withColumnRenamed("departure_name", "station_name")

            ret_counts = df.groupBy("return_name").count() \
                .withColumnRenamed("count", "return_count") \
                .withColumnRenamed("return_name", "station_name")

            metrics_df = dep_counts.join(ret_counts, on="station_name", how="outer").na.fill(0)

            filename = os.path.basename(file_path)
            metrics_filename = f"metrics_{filename}"

            output_dir = os.path.join(PROCESSED_DIR, f"temp_{metrics_filename}")
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)

            metrics_df.coalesce(1).write.option("header", "true").csv(output_dir)

            generated_file = glob.glob(os.path.join(output_dir, "*.csv"))[0]
            final_metrics_path = os.path.join(PROCESSED_DIR, metrics_filename)

            os.rename(generated_file, final_metrics_path)
            shutil.rmtree(output_dir)

            logger.info(f"metrics saved locally to {final_metrics_path}")

            s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
            s3_key = f"{METRICS_PREFIX}/{metrics_filename}"
            s3.upload_file(final_metrics_path, BUCKET_NAME, s3_key)
            logger.info(f"metrics uploaded to s3: {s3_key}")

        except Exception as e:
            logger.error(f"spark processing failed: {e}")
            raise e
        finally:
            spark.stop()

    bucket_ready = ensure_bucket_exists()
    raw_files = get_raw_files()

    uploaded_paths = upload_raw_file.expand(file_path=raw_files)

    bucket_ready >> raw_files
    calculate_and_upload_metrics.expand(file_path=uploaded_paths)


process_files_dag()