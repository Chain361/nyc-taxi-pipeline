import os
import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

#base url = https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet 

#base url = https://d37ci6vzurychx.cloudfront.net/trip-data/{TAXI_TYPE}_tripdata_{YEAR}-{MONTH}.parquet 


#Download parquet from each month of year of taxitype
# Type -> Year -> Month loop

#Upload function: Stream into s3 storage (idempotency)

BUCKET_NAME = 'nyc-taxi-lakehouse-6709616681'
BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
TAXI_TYPES = ['yellow', 'green']
YEARS = 2024 # 2024
MONTHS = range(1,7) #Jan to Jun

s3_client = boto3.client('s3', region_name='us-east-1')

def is_s3_key_existed(key: str, bucket: str) -> bool:
    """Check if the object already exists in S3 (Idempotency)."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

def upload_taxi_data(taxi_type: str, year: int, month: str):
    month_str = f"{month:02d}"
    file_name = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
    source_url = f"{BASE_URL}/{file_name}"

    s3_key = f"bronze/taxi_type={taxi_type}/year={year}/{file_name}"

    if is_s3_key_existed(s3_key, BUCKET_NAME):
        print(f"[SKIP] s3://{BUCKET_NAME}/{s3_key} already exists.")
        return

    # stream HTTP content straight into S3 without writing on local disk
    with requests.get(source_url, stream=True) as response:
        if response.status_code == 200:
            s3_client.upload_fileobj(response.raw, BUCKET_NAME, s3_key)
            print(f"[SUCCESS] Uploaded {file_name}")
        else:
            print(f"[FAILED] HTTP {response.status_code} for {source_url}")
    

def main():
    for taxi_type in TAXI_TYPES:
        for month in MONTHS:
            upload_taxi_data(taxi_type, YEARS, month)

if __name__ == "__main__":
    main()



