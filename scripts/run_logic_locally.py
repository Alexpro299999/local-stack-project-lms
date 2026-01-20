import boto3
import logging
import sys
import os
import shutil
from script_config import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_ENDPOINT,
    BUCKET_NAME, DYNAMO_TABLE_NAME, RAW_PREFIX, JAVA_HOME
)

os.environ['JAVA_HOME'] = JAVA_HOME

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("error: pyspark is not installed")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_local_spark():
    """
    simulates pipeline logic locally using pyspark.
    fixes dynamodb keys and encoding issues.
    """
    creds = {
        'aws_access_key_id': AWS_ACCESS_KEY,
        'aws_secret_access_key': AWS_SECRET_KEY,
        'region_name': AWS_REGION
    }

    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **creds)
    dynamodb = boto3.client('dynamodb', endpoint_url=S3_ENDPOINT, **creds)

    logger.info(f"looking for files in s3 ({BUCKET_NAME}/{RAW_PREFIX})...")

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX)
    except Exception as e:
        logger.error(f"failed to list objects: {e}")
        return

    raw_key = None
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv'):
                raw_key = obj['Key']
                break

    if not raw_key:
        logger.warning("no csv files found.")
        return

    logger.info(f"found file: {raw_key}")

    file_basename = os.path.basename(raw_key)
    date_str = file_basename.replace('.csv', '')

    current_dir = os.path.dirname(os.path.abspath(__file__))
    local_temp_file = os.path.join(current_dir, "temp_raw_data.csv")

    try:
        s3.download_file(BUCKET_NAME, raw_key, local_temp_file)
    except Exception as e:
        logger.error(f"failed to download: {e}")
        return

    logger.info("starting spark session...")
    spark = SparkSession.builder \
        .appName("HelsinkiBikesLocalDebug") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    try:
        logger.info("processing with spark...")
        df = spark.read.option("header", "true").option("encoding", "UTF-8").csv(local_temp_file)

        dep_counts = df.groupBy("departure_name").count() \
            .withColumnRenamed("count", "departure_count") \
            .withColumnRenamed("departure_name", "station_name")

        ret_counts = df.groupBy("return_name").count() \
            .withColumnRenamed("count", "return_count") \
            .withColumnRenamed("return_name", "station_name")

        metrics_df = dep_counts.join(ret_counts, on="station_name", how="outer").na.fill(0)

        results = metrics_df.collect()

        logger.info(f"calculated metrics for {len(results)} stations.")
        logger.info("writing to dynamodb...")

        for row in results:
            item = {
                'date': {'S': str(date_str)},
                'metric_type': {'S': f"STATION_STATS_{row['station_name']}"},
                'station_name': {'S': str(row['station_name'])},
                'departure_count': {'N': str(row['departure_count'])},
                'return_count': {'N': str(row['return_count'])}
            }
            try:
                dynamodb.put_item(TableName=DYNAMO_TABLE_NAME, Item=item)
            except Exception as e:
                logger.error(f"error putting item: {e}")

        logger.info("success! data is in dynamodb.")

    except Exception as e:
        logger.error(f"spark processing error: {e}")
    finally:
        spark.stop()
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)


if __name__ == "__main__":
    run_local_spark()