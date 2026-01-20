import boto3
import json
import logging
import sys
from script_config import (
    S3_ENDPOINT, AWS_REGION, AWS_CREDS,
    BUCKET_NAME, SNS_TOPIC_NAME, SQS_QUEUE_NAME, DYNAMO_TABLE_NAME
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_infra():
    """
    creates s3 buckets, sns topics, sqs queues, and dynamodb tables.
    """
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
    sns = boto3.client('sns', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
    sqs = boto3.client('sqs', endpoint_url=S3_ENDPOINT, **AWS_CREDS)
    dynamodb = boto3.client('dynamodb', endpoint_url=S3_ENDPOINT, **AWS_CREDS)

    logger.info("creating s3 bucket...")
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"bucket {BUCKET_NAME} created or exists")
    except Exception:
        pass

    logger.info("creating sns topic...")
    topic_response = sns.create_topic(Name=SNS_TOPIC_NAME)
    topic_arn = topic_response['TopicArn']
    logger.info(f"topic created: {topic_arn}")

    logger.info("creating sqs queue...")
    queue_response = sqs.create_queue(QueueName=SQS_QUEUE_NAME)
    queue_url = queue_response['QueueUrl']

    queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
    queue_arn = queue_attrs['Attributes']['QueueArn']
    logger.info(f"queue created: {queue_url}")

    logger.info("subscribing sqs to sns...")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn
    )

    policy = {
        "Version": "2012-10-17",
        "Id": "SNStoSQS",
        "Statement": [
            {
                "Sid": "Allow-SNS-SendMessage",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": topic_arn
                    }
                }
            }
        ]
    }
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'Policy': json.dumps(policy)}
    )
    logger.info("subscription active.")

    logger.info("configuring s3 notifications...")
    notification_config = {
        'TopicConfigurations': [
            {
                'TopicArn': topic_arn,
                'Events': ['s3:ObjectCreated:*']
            }
        ]
    }
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration=notification_config
    )
    logger.info("s3 notifications configured.")

    logger.info("creating dynamodb table...")
    try:
        dynamodb.create_table(
            TableName=DYNAMO_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'date', 'KeyType': 'HASH'},
                {'AttributeName': 'metric_type', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'date', 'AttributeType': 'S'},
                {'AttributeName': 'metric_type', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        logger.info(f"table {DYNAMO_TABLE_NAME} created.")
    except Exception:
        pass

    logger.info("infrastructure setup complete!")


if __name__ == "__main__":
    setup_infra()