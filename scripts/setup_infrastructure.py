import boto3
import json

def setup_infra():
    endpoint_url = 'http://localhost:4566'
    region = 'us-east-1'
    creds = {'aws_access_key_id': 'test', 'aws_secret_access_key': 'test'}
    
    s3 = boto3.client('s3', endpoint_url=endpoint_url, region_name=region, **creds)
    sns = boto3.client('sns', endpoint_url=endpoint_url, region_name=region, **creds)
    sqs = boto3.client('sqs', endpoint_url=endpoint_url, region_name=region, **creds)
    dynamodb = boto3.client('dynamodb', endpoint_url=endpoint_url, region_name=region, **creds)

    bucket_name = 'my-helsinki-bikes-bucket'
    topic_name = 'BikesUploadTopic'
    queue_name = 'BikesProcessingQueue'
    table_name = 'HelsinkiDailyMetrics'

    print("Creating S3 Bucket...")
    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception:
        pass

    print("1. Creating SNS Topic...")
    topic_response = sns.create_topic(Name=topic_name)
    topic_arn = topic_response['TopicArn']
    print(f"Topic created: {topic_arn}")

    print("2. Creating SQS Queue...")
    queue_response = sqs.create_queue(QueueName=queue_name)
    queue_url = queue_response['QueueUrl']
    
    queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
    queue_arn = queue_attrs['Attributes']['QueueArn']
    print(f"Queue created: {queue_url}")

    print("3. Subscribing SQS to SNS...")
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
    print("Subscription active.")

    print("4. Configuring S3 Notifications...")
    notification_config = {
        'TopicConfigurations': [
            {
                'TopicArn': topic_arn,
                'Events': ['s3:ObjectCreated:*']
            }
        ]
    }
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=notification_config
    )
    print("S3 Notifications configured.")

    print("5. Creating DynamoDB Table...")
    try:
        dynamodb.create_table(
            TableName=table_name,
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
        print(f"Table {table_name} created.")
    except Exception:
        pass

    print("\nInfrastructure setup complete!")

if __name__ == "__main__":
    setup_infra()