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
    files = ["full_transactions.csv", "dim_products.csv", "dim_customers.csv"]
    # Hive-style partitioning
    for file in files:
        file_path2 = f'raw_data/{file}'
        # Hive-style partitioning
        s3.upload_file(f'/tmp/{file}', bucket_name, file_path2)
    # Upload the last file to a different path
    file_path1 = f"transactions/year={year}/month={month}/day={day}/{filename}"
    s3.upload_file(f'/tmp/{filename}', bucket_name, file_path1)

    print(f"File uploaded to S3 Bucket {bucket_name}/{file_path1}")
    return