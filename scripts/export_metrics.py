import boto3
import pandas as pd
import os

def export_data():
    dynamodb = boto3.client(
        'dynamodb', 
        endpoint_url='http://localhost:4566',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    
    table_name = 'HelsinkiDailyMetrics'
    
    try:
        response = dynamodb.scan(TableName=table_name)
    except Exception:
        print("Error: Could not scan table. Is LocalStack running?")
        return

    items = response.get('Items', [])
    
    if not items:
        print("Table is empty. Run the Airflow DAG '02_process_single_file_metrics' again and wait a minute.")
        return

    data = []
    for item in items:
        row = {
            'date': item['date']['S'],
            'avg_distance': float(item['avg_distance']['N']),
            'avg_duration': float(item['avg_duration']['N']),
            'avg_speed': float(item['avg_speed']['N']),
            'avg_temperature': float(item['avg_temperature']['N'])
        }
        data.append(row)

    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(base_dir, 'data', 'final_metrics_for_tableau.csv')
    
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} rows to {output_path}")

if __name__ == "__main__":
    export_data()