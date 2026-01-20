import json
import boto3
import pandas as pd
import os
import io
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))
dynamodb = boto3.client('dynamodb', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))

TABLE_NAME = os.environ.get('DYNAMO_TABLE_NAME', 'HelsinkiDailyMetrics')


def get_metrics_key(key: str) -> Optional[str]:
    if key.startswith('metrics/'):
        return key
    return None


def save_to_dynamo(stats_df: pd.DataFrame, date_str: str):
    """
    converts dataframe to dict and writes to dynamodb table.
    adds required keys 'date' and 'metric_type'.
    """
    records = stats_df.to_dict(orient='records')

    for row in records:
        station_name = str(row.get('station_name', 'unknown'))
        item = {
            'date': {'S': date_str},
            'metric_type': {'S': f"STATION_STATS_{station_name}"},
            'station_name': {'S': station_name},
            'departure_count': {'N': str(row.get('departure_count', 0))},
            'return_count': {'N': str(row.get('return_count', 0))}
        }

        try:
            dynamodb.put_item(TableName=TABLE_NAME, Item=item)
        except Exception as e:
            logger.error(f"failed to write item: {e}")


def handler(event: Dict[str, Any], context: Any):
    for record in event.get('Records', []):
        try:
            body = record.get('body')
            if body:
                message = json.loads(body)
                if 'Event' in message and message['Event'] == 's3:TestEvent':
                    continue
                if 'Records' in message:
                    s3_event = message['Records'][0]
                else:
                    continue
            else:
                s3_event = record

            bucket = s3_event['s3']['bucket']['name']
            key = s3_event['s3']['object']['key']

            if not get_metrics_key(key):
                continue

            filename = os.path.basename(key)
            date_str = filename.replace('metrics_', '').replace('.csv', '')

            logger.info(f"processing file: {key} for date: {date_str}")

            obj = s3_client.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read()

            df = pd.read_csv(io.BytesIO(data), low_memory=False, encoding='utf-8')

            save_to_dynamo(df, date_str)

        except Exception as e:
            logger.error(f"error: {e}")
            continue

    return {"statusCode": 200, "body": "Success"}