import json
import boto3

source_bucket = 'SOURCE-BUCKET'
target_bucket = 'TARGET-BUCKET'


def lambda_handler(event, context):
    print(f"Event: {event}")

    # extract the message from the SNS notification
    message = json.loads(event['Records'][0]['Sns']['Message'])

    # access the 'detail' dictionary within the message
    detail = message['detail']

    # extract the value of "state" key
    glue_job_status = detail['state']
    print(f"Glue Job Status:{glue_job_status}")

    if glue_job_status == "SUCCEEDED":

        if not source_bucket:
            return {
                'statusCode': 400,
                'body': json.dumps("Source Bucket Name is missing in the event!")
            }
        # create s3 client
        s3 = boto3.client('s3')

        try:
            # call transfer_files function
            transfer_files(s3, source_bucket, target_bucket)
            return {
                'statusCode': 200,
                'body': json.dumps('Files transferred successfully!')
            }


        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error transferring files: {e}")
            }

    else:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Glue Job {glue_job_status}: {event}")
        }


def transfer_files(s3, source_bucket_name, target_bucket_name):
    for obj in s3.list_objects_v2(Bucket=source_bucket_name)['Contents']:
        source_key = obj['Key']
        target_key = source_key
        # Optional: Modify destination_key if needed
        s3.copy_object(CopySource={'Bucket': source_bucket_name, 'Key': source_key},
                       Bucket=target_bucket_name, Key=target_key)
        print(f"Copied {source_key} to {target_bucket_name}/{target_key}")