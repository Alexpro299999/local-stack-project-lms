import json
import boto3
import pandas as pd
import os
import io

s3_client = boto3.client('s3', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))
dynamodb = boto3.client('dynamodb', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))

TABLE_NAME = 'HelsinkiDailyMetrics'

def get_files_keys(key):
    filename = os.path.basename(key)
    
    if key.startswith('raw/'):
        raw_key = key
        metrics_key = f"metrics/metrics_{filename}"
        base_name = filename
    elif key.startswith('metrics/'):
        metrics_key = key
        base_name = filename.replace('metrics_', '')
        raw_key = f"raw/{base_name}"
    else:
        return None, None, None
        
    return raw_key, metrics_key, base_name

def process_data(df):
    df.columns = df.columns.str.lower()
    
    date_col = 'departure'
    if date_col not in df.columns:
        raise ValueError("Column 'departure' not found")

    df[date_col] = pd.to_datetime(df[date_col])
    df['date_str'] = df[date_col].dt.strftime('%Y-%m-%d')

    daily_stats = df.groupby('date_str').agg({
        'distance (m)': 'mean',
        'duration (sec.)': 'mean',
        'avg_speed (km/h)': 'mean',
        'air temperature (degc)': 'mean'
    }).reset_index()

    return daily_stats

def save_to_dynamo(stats_df):
    for _, row in stats_df.iterrows():
        item = {
            'date': {'S': str(row['date_str'])},
            'metric_type': {'S': 'DAILY_AVG'},
            'avg_distance': {'N': str(round(row['distance (m)'], 2))},
            'avg_duration': {'N': str(round(row['duration (sec.)'], 2))},
            'avg_speed': {'N': str(round(row['avg_speed (km/h)'], 2))},
            'avg_temperature': {'N': str(round(row['air temperature (degc)'], 2))}
        }
        
        try:
            dynamodb.put_item(TableName=TABLE_NAME, Item=item)
        except Exception:
            pass

def handler(event, context):
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            if 'Event' in body and body['Event'] == 's3:TestEvent':
                continue
                
            if 'Records' in body:
                s3_event = body['Records'][0]
                bucket = s3_event['s3']['bucket']['name']
                key = s3_event['s3']['object']['key']
            else:
                continue
                
        except Exception:
            continue

        raw_key, metrics_key, base_name = get_files_keys(key)
        if not raw_key:
            continue

        try:
            s3_client.head_object(Bucket=bucket, Key=raw_key)
            s3_client.head_object(Bucket=bucket, Key=metrics_key)
        except:
            raise Exception("Pair not ready yet")

        obj = s3_client.get_object(Bucket=bucket, Key=raw_key)
        data = obj['Body'].read()
        
        df = pd.read_csv(io.BytesIO(data), low_memory=False)
        
        stats = process_data(df)
        
        save_to_dynamo(stats)
        
    return {"statusCode": 200, "body": "Success"}