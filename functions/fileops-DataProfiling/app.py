"""This module is for DataProfiling"""
import json
import boto3
import os
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
def lambda_handler(event, context):
    print("Received Event: ", str(event))
    """This method is for handling data profiling"""
    query_params = event.get('queryStringParameters', None)
    if query_params is not None and "job_id" in query_params:
        query_params = event['queryStringParameters']
        job_id = query_params['job_id']
        # getting file from s3
        get_file = getfilefromS3(job_id)
        if get_file is False:
            statuscode = 400
        else:
            statuscode = 200
        return response_body(statuscode,"",get_file)
    else:
        return response_body(400, "", "queryParams not found")


def getfilefromS3(uuid):
    """Creating a S3 client"""
    bucket_name = s3_bucket
    folder_path = 'profiling/' + uuid + "/"
    s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    for obj in response['Contents']:
        if obj is not None and obj["Key"].endswith("html"):
            url = get_s3_object_url(bucket_name=bucket_name, object_key=obj['Key'])
            return url
        else :
            return False


def get_s3_object_url(bucket_name, object_key):
    """Generating presigned URL"""
    s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
    url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': object_key}
    )
    return url

def response_body(statuscode, message, body):
    """This is a response method"""
    response = {
        "statusCode": statuscode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": body})
    }
    return response
