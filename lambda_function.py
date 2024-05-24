import json
from generate_mock_data import generate_data
from upload_to_s3 import upload_to_s3
from datetime import date, timedelta

# Define date range (modify as needed)
start_date = date(2024, 5, 19)  # Adjust start date
end_date = date.today()

def lambda_handler(event, context):
    for current_date in range((end_date - start_date).days + 1):
        # Generate Date
        current_date = start_date + timedelta(days=current_date)
        date_str = str(current_date)
        generate_data(current_date, date_str)
        # Upload the generated CSV to S3
        files = ["full_transactions.csv", "dim_products.csv", "dim_customers.csv", f'transactions_{date_str}.csv']
        for file in files[:-1]:
            upload_to_s3(f"{file}", start_date, path=f"transactions/raw_data/{file}")
        upload_to_s3(f"transactions_{date_str}.csv", current_date)


    return {
        'statusCode': 200,
        'body': json.dumps(f'Data Successfully Dumped in S3')
    }