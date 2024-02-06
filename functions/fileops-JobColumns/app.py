import os
import boto3
import json
import pandas as pd
from dbconnection import cursor, conn
import psycopg2

env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    query_params = event.get('queryStringParameters', None)
    print(query_params)
    if event['resource'] == "/job/columns" and event[
        'httpMethod'] == 'GET' and query_params is not None and "job_id" in query_params:
        uuid = query_params['job_id']
        s3_object = getfilefromS3(uuid)
        if s3_object is None:
            return response_body(400, "", body="No Sample file found neither format not support")
        try:
            sample_df = readings3file(s3_object['Body'], s3_object['file'], uuid)
            columns = sample_df.columns.tolist() if sample_df is not None else []
            print(type(columns))
            return response_body(200, "retrive succesfully", columns)
        except Exception as error:
            return response_body(200, "Invalid file data", str(error))


def getfilefromS3(uuid):
    """This method is getting file from S3"""
    bucket_name = s3_bucket
    folder_path = 'sample-files/' + uuid + "/"
    s3_client = boto3.client('s3')
    try:

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        for obj in response['Contents']:
            if obj['Key'].endswith(".csv"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'csv'}
            elif obj['Key'].endswith(".xlsx"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'xlsx'}
            elif obj['Key'].endswith(".json"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'json'}
        return None
    except Exception as error:
        print("The Error:", str(error))
        return str(error)


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


def readings3file(body, file, job_id):
    """This method is for reading file from S3"""
    try:
        if file == 'csv':
            # Here calling a method to get the delimiter
            delimiter = getDelimiter(job_id)
            if delimiter:
                return pd.read_csv(body, delimiter=delimiter)
            else:
                return pd.read_csv(body)
        elif file == 'xlsx':
            return pd.read_excel(body)
        return None
    except Exception as error:
        print("The Error: ", str(error))
        return response_body(500, str(error), None)


def getDelimiter(job_uuid):
    """This method is for getting the delimiter given in file configurations"""
    try:
        query = f"SELECT delimiter from jobs WHERE uuid ='{job_uuid}'"
        cursor.execute(query)
        data = cursor.fetchone()
        if data is not None:
            delimiter = data[0]
            print("delimiter: ", delimiter)
            return delimiter
        else:
            delimiter = None
            return delimiter
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response_body(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error: ", error)
        return response_body(500, str(error), None)

