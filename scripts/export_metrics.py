import boto3
import pandas as pd
import os
import logging
from typing import List, Dict, Any
from script_config import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_ENDPOINT,
    DYNAMO_TABLE_NAME, DATA_DIR
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def export_data():
    """
    scans dynamodb table and exports metrics to a local csv file for tableau.
    """
    dynamodb = boto3.client(
        'dynamodb',
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    try:
        response = dynamodb.scan(TableName=DYNAMO_TABLE_NAME)
    except Exception:
        logger.error("error: could not scan table. is localstack running?")
        return

    items = response.get('Items', [])

    if not items:
        logger.warning(f"table {DYNAMO_TABLE_NAME} is empty.")
        return

    data: List[Dict[str, Any]] = []
    for item in items:
        row = {
            'station_name': item.get('station_name', {}).get('S'),
            'departure_count': int(item.get('departure_count', {}).get('N', 0)),
            'return_count': int(item.get('return_count', {}).get('N', 0))
        }
        if row['station_name']:
            data.append(row)

    df = pd.DataFrame(data)

    output_path = os.path.join(DATA_DIR, 'final_metrics_for_tableau.csv')

    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"exported {len(df)} rows to {output_path}")


if __name__ == "__main__":
    export_data()