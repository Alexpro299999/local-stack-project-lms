import subprocess
import boto3
import logging
from script_config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_REGION,
    S3_ENDPOINT,
    SQS_QUEUE_NAME,
    DYNAMO_TABLE_NAME,
    BUCKET_NAME
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def deploy():
    """
    builds docker image and deploys lambda function using settings from script_config.
    """
    function_name = "ProcessBikesData"
    image_name = "bikes-lambda"
    role_arn = "arn:aws:iam::000000000000:role/lambda-role"

    lambda_client = boto3.client(
        'lambda',
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    sqs = boto3.client(
        'sqs',
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    logger.info("building docker image...")
    subprocess.run(f"docker build -t {image_name} ./lambda_functions", shell=True, check=True)

    logger.info("deleting old function...")
    try:
        lambda_client.delete_function(FunctionName=function_name)
    except Exception:
        pass

    logger.info("creating lambda function...")
    lambda_client.create_function(
        FunctionName=function_name,
        PackageType='Image',
        Code={'ImageUri': image_name},
        Role=role_arn,
        Timeout=60,
        Environment={
            'Variables': {
                'AWS_ENDPOINT_URL': 'http://localstack:4566',
                'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY,
                'AWS_SECRET_ACCESS_KEY': AWS_SECRET_KEY,
                'AWS_DEFAULT_REGION': AWS_REGION,
                'DYNAMO_TABLE_NAME': DYNAMO_TABLE_NAME,
                'BUCKET_NAME': BUCKET_NAME
            }
        }
    )

    logger.info("mapping trigger...")
    try:
        queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
        queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
        queue_arn = queue_attrs['Attributes']['QueueArn']

        lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=1
        )
    except Exception as e:
        logger.error(f"failed to map trigger: {e}")

    logger.info("deployment complete!")


if __name__ == "__main__":
    deploy()