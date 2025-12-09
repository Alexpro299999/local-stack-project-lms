import boto3
import json

def trigger():
    sqs = boto3.client(
        'sqs', 
        endpoint_url='http://localhost:4566',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    
    try:
        queue_url = sqs.get_queue_url(QueueName='BikesProcessingQueue')['QueueUrl']
    except Exception as e:
        print(f"Error: Could not find queue. {e}")
        return
    
    print(f"Sending trigger to: {queue_url}")

    message = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": "my-helsinki-bikes-bucket"
                    },
                    "object": {
                        "key": "raw/2017-05.csv"
                    }
                }
            }
        ]
    }
    
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )
    print("Done! Lambda should start processing now.")

if __name__ == "__main__":
    trigger()