import subprocess
import boto3
import time

def deploy():
    function_name = "ProcessBikesData"
    image_name = "bikes-lambda"
    role_arn = "arn:aws:iam::000000000000:role/lambda-role"
    
    lambda_client = boto3.client(
        'lambda', 
        endpoint_url='http://localhost:4566', 
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    sqs = boto3.client(
        'sqs', 
        endpoint_url='http://localhost:4566', 
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    print("Building Docker Image...")
    subprocess.run(f"docker build -t {image_name} ./lambda_functions", shell=True, check=True)

    print("Deleting old function...")
    try:
        lambda_client.delete_function(FunctionName=function_name)
    except:
        pass

    print("Creating Lambda Function...")
    lambda_client.create_function(
        FunctionName=function_name,
        PackageType='Image',
        Code={'ImageUri': image_name},
        Role=role_arn,
        Timeout=60,
        Environment={
            'Variables': {
                'AWS_ENDPOINT_URL': 'http://localstack:4566',
                'AWS_ACCESS_KEY_ID': 'test',
                'AWS_SECRET_ACCESS_KEY': 'test',
                'AWS_DEFAULT_REGION': 'us-east-1'
            }
        }
    )

    print("Mapping Trigger...")
    queue_url = sqs.get_queue_url(QueueName='BikesProcessingQueue')['QueueUrl']
    queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
    queue_arn = queue_attrs['Attributes']['QueueArn']

    try:
        lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=1
        )
    except Exception:
        pass

    print("Deployment Complete!")

if __name__ == "__main__":
    deploy()