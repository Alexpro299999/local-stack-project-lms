import os

S3_ENDPOINT = 'http://localstack:4566'
AWS_ACCESS_KEY = 'test'
AWS_SECRET_KEY = 'test'
AWS_REGION = 'us-east-1'
BUCKET_NAME = 'my-helsinki-bikes-bucket'

RAW_PREFIX = 'raw'
METRICS_PREFIX = 'metrics'

DATA_DIR = '/opt/airflow/data'
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')

AWS_CREDS = {
    'aws_access_key_id': AWS_ACCESS_KEY,
    'aws_secret_access_key': AWS_SECRET_KEY,
    'region_name': AWS_REGION
}