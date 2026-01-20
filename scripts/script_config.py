import os

S3_ENDPOINT = 'http://localhost:4566'
AWS_ACCESS_KEY = 'test'
AWS_SECRET_KEY = 'test'
AWS_REGION = 'us-east-1'

BUCKET_NAME = 'my-helsinki-bikes-bucket'
SNS_TOPIC_NAME = 'BikesUploadTopic'
SQS_QUEUE_NAME = 'BikesProcessingQueue'
DYNAMO_TABLE_NAME = 'HelsinkiDailyMetrics'

RAW_PREFIX = 'raw'
METRICS_PREFIX = 'metrics'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

JAVA_HOME = r'C:\Program Files\Java\jdk-17'

AWS_CREDS = {
    'aws_access_key_id': AWS_ACCESS_KEY,
    'aws_secret_access_key': AWS_SECRET_KEY,
    'region_name': AWS_REGION
}