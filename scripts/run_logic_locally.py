import boto3
import pandas as pd
import io

def run_local():
    endpoint = 'http://localhost:4566'
    creds = {'aws_access_key_id': 'test', 'aws_secret_access_key': 'test', 'region_name': 'us-east-1'}
    
    s3 = boto3.client('s3', endpoint_url=endpoint, **creds)
    dynamodb = boto3.client('dynamodb', endpoint_url=endpoint, **creds)
    
    BUCKET_NAME = 'my-helsinki-bikes-bucket'
    TABLE_NAME = 'HelsinkiDailyMetrics'
    
    raw_key = 'raw/2017-05.csv'
    metrics_key = 'metrics/metrics_2017-05.csv'
    
    print(f"1. Checking files in S3 ({BUCKET_NAME})...")
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=raw_key)
        print(f" - Found: {raw_key}")
        s3.head_object(Bucket=BUCKET_NAME, Key=metrics_key)
        print(f" - Found: {metrics_key}")
    except Exception as e:
        print(f"Error: File not found in S3. Did Airflow finish? {e}")
        return

    print("2. Downloading RAW data...")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=raw_key)
    data = obj['Body'].read()
    
    print("3. Processing with Pandas (this may take time)...")
    df = pd.read_csv(io.BytesIO(data), low_memory=False)
    
    df.columns = df.columns.str.lower()
    date_col = 'departure'
    
    if date_col not in df.columns:
        print(f"Error: Column {date_col} not found in columns: {df.columns}")
        return

    df[date_col] = pd.to_datetime(df[date_col])
    df['date_str'] = df[date_col].dt.strftime('%Y-%m-%d')

    daily_stats = df.groupby('date_str').agg({
        'distance (m)': 'mean',
        'duration (sec.)': 'mean',
        'avg_speed (km/h)': 'mean',
        'air temperature (degc)': 'mean'
    }).reset_index()
    
    print(f"Calculated stats for {len(daily_stats)} days.")

    print("4. Writing to DynamoDB...")
    for _, row in daily_stats.iterrows():
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
        except Exception as e:
            print(f"Error putting item: {e}")
            
    print("Success! Data is in DynamoDB.")

if __name__ == "__main__":
    run_local()