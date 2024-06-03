import boto3
from datetime import datetime

s3 = boto3.client("s3")
bucket_name = "your-bucket"

def upload_to_s3(filename, current_date, path=f"transactions/"):
    """Uploads a CSV file to S3 with Hive-style partitioning"""
    # Extract year, month, day from current_date
    year = current_date.year
    month = current_date.month
    day = current_date.day
    # Hive-style partitioning
    file_path = f"{path}year={year}/month={month}/day={day}/{filename}"
    s3.upload_file(f'/tmp/{filename}', bucket_name, file_path)

    print(f"File uploaded to S3 Bucket {bucket_name}/{file_path}")
    return
