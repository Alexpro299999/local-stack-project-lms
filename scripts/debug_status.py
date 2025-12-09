import boto3

def status():
    endpoint = 'http://localhost:4566'
    creds = {'aws_access_key_id': 'test', 'aws_secret_access_key': 'test', 'region_name': 'us-east-1'}
    
    sqs = boto3.client('sqs', endpoint_url=endpoint, **creds)
    lambda_client = boto3.client('lambda', endpoint_url=endpoint, **creds)
    
    print(" SQS STATUS ")
    try:
        q_url = sqs.get_queue_url(QueueName='BikesProcessingQueue')['QueueUrl']
        attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'])
        print(f"Queue URL: {q_url}")
        print(f"Messages Available: {attrs['Attributes']['ApproximateNumberOfMessages']}")
        print(f"Messages in Flight (Processing): {attrs['Attributes']['ApproximateNumberOfMessagesNotVisible']}")
    except Exception as e:
        print(f"Error checking SQS: {e}")

    print("\n TRIGGER STATUS ")
    try:
        mappings = lambda_client.list_event_source_mappings()
        if not mappings['EventSourceMappings']:
            print("No triggers found!")
        for m in mappings['EventSourceMappings']:
            print(f"State: {m['State']}")
            print(f"Last Result: {m.get('LastProcessingResult', 'None')}")
    except Exception as e:
        print(f"Error checking triggers: {e}")

if __name__ == "__main__":
    status()