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
    file_path2 = f"raw_data/"
    for file in files[:-1]:
        file_path1 = f"transactions/year={year}/month={month}/day={day}/{filename}"  # Hive-style partitioning
        upload_to_s3(f"/tmp/{filename}", bucket_name, file_path1)
    # Upload the last file to a different path
    filename = files[-1]
    file_path2 = f"raw_data/{filename}"
    upload_to_s3(f"/tmp/{filename}", bucket_name, file_path2)

    print(f"File uploaded to S3 Bucket {bucket_name}/{file_path2}")
    return