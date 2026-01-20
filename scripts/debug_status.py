import boto3
import logging
from script_config import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_ENDPOINT,
    SQS_QUEUE_NAME
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def status():
    """
    checks and logs the status of sqs queues and lambda event source mappings.
    """
    creds = {
        'aws_access_key_id': AWS_ACCESS_KEY,
        'aws_secret_access_key': AWS_SECRET_KEY,
        'region_name': AWS_REGION
    }

    sqs = boto3.client('sqs', endpoint_url=S3_ENDPOINT, **creds)
    lambda_client = boto3.client('lambda', endpoint_url=S3_ENDPOINT, **creds)

    logger.info(" sqs status ")
    try:
        q_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
        attrs = sqs.get_queue_attributes(
            QueueUrl=q_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        logger.info(f"queue: {SQS_QUEUE_NAME}")
        logger.info(f"messages available: {attrs['Attributes']['ApproximateNumberOfMessages']}")
        logger.info(f"messages in flight: {attrs['Attributes']['ApproximateNumberOfMessagesNotVisible']}")
    except Exception as e:
        logger.error(f"error checking sqs: {e}")

    logger.info("\n trigger status ")
    try:
        mappings = lambda_client.list_event_source_mappings()
        if not mappings['EventSourceMappings']:
            logger.info("no triggers found!")
        for m in mappings['EventSourceMappings']:
            logger.info(f"state: {m['State']}")
            logger.info(f"function: {m['FunctionArn']}")
    except Exception as e:
        logger.error(f"error checking triggers: {e}")


if __name__ == "__main__":
    status()