import boto3
import json
import logging
import sys
import os
from typing import Dict, Any

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '../airflow/plugins')
sys.path.append(config_path)

try:
    from common_config import (
        AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_ENDPOINT,
        SQS_QUEUE_NAME, BUCKET_NAME, METRICS_PREFIX
    )
except ImportError:
    print("error: could not import common_config")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger():
    """
    sends a manual s3 event trigger to the sqs queue using an existing file from s3.
    """
    sqs = boto3.client(
        'sqs',
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    try:
        queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
    except Exception as e:
        logger.error(f"error: could not find queue {SQS_QUEUE_NAME}. {e}")
        return

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=METRICS_PREFIX)
        target_key = None
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    target_key = obj['Key']
                    break

        if not target_key:
            logger.warning(f"no metrics files found in {BUCKET_NAME}/{METRICS_PREFIX}. run the processing dag first.")
            return

    except Exception as e:
        logger.error(f"error listing s3 objects: {e}")
        return

    logger.info(f"sending trigger for file: {target_key} to {queue_url}")

    message: Dict[str, Any] = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "awsRegion": AWS_REGION,
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": BUCKET_NAME
                    },
                    "object": {
                        "key": target_key
                    }
                }
            }
        ]
    }

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )
    logger.info("done! lambda should start processing now.")


if __name__ == "__main__":
    trigger()