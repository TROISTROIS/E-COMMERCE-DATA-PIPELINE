import boto3
from datetime import datetime

s3 = boto3.client("s3")
bucket_name = "e-commerce-bucket-fn"

def upload_to_s3(filename, current_date):
    """Uploads a CSV file to S3 with Hive-style partitioning"""
    # Extract year, month, day from current_date
    year = current_date.year
    month = current_date.month
    day = current_date.day
    file_path = f"transactions/year={year}/month={month}/day={day}/{filename}"  # Hive-style partitioning
    s3.upload_file(f'/tmp/{filename}', bucket_name, file_path)
    print(f"File uploaded to S3 Bucket {bucket_name}/{file_path}")
    return