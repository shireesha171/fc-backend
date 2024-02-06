import pandas as pd
import boto3
from io import StringIO
from dbconnection import cursor, conn
import psycopg2
import json

import os

env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'

def lambda_handler(event, context):
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job/transformation" and event['httpMethod'] == 'POST':
            if 'body' in event:
                # body = event['body']
                body = json.loads(event['body'])
                target_data = body['target_data']
                job_id = body['job_id']
                target_values = sorted((item for item in target_data if item['status']),
                                       key=lambda x: x['order'])
                target_df = forming_target_df(target_values)
                s3_object = getfilefromS3(job_id)
                if s3_object is None:
                    return response(400, "No Sample file found neither format not support", None)
                source_df = creatingDataframe(s3_object['file'], s3_object['Body'])
                target_df = adding_data_df(target_df, source_df, target_values)
                target_df = applyingTransformation(target_df, target_values)
                sending_file_s3(job_id, target_df)
                updateMetadataRecord(target_data, job_id)
                return response(200, "updated Successfully", "")
            else:
                return response(400, "body is empty Successfully", "")

        elif event['resource'] == "/job/transformation" and event['httpMethod'] == 'GET':
            query_params = event.get('queryStringParameters', None)
            job_id = query_params.get('job_id', None)
            if query_params is not None and job_id is not None and job_id != '':
                return get_transformations(job_id)
            else:
                return response(400, "Invalid query parameters(Job_id)", None)

    except Exception as err:
        print(err)
        return response(400, str(err), "")


def get_transformations(job_id):
    """This method is for getting the transformations applied on columns"""
    try:
        get_transformations_data = """
                                    SELECT sfc.target_metadata
                                    FROM source_file_config as sfc
                                    JOIN jobs as j ON sfc.job_id = j.id
                                    where j.uuid  = %s;
                                   """
        cursor.execute(get_transformations_data, (job_id,))
        data = cursor.fetchone()
        if data is None:
            return response(400, "No data is found for the given job_id", None)
        else:
            return response(200, "The transformation data is ready", data[0])

    except Exception as error:
        print(error)
        return response(500, str(error), None)


def forming_target_df(target_values):
    df = pd.DataFrame(columns=[item['target_column'] for item in target_values])
    return df


def getfilefromS3(db_uuid):
    """This method is getting file from S3"""
    try:
        bucket_name = s3_bucket
        folder_path = 'sample-files/' + db_uuid + "/"
        s3_client = boto3.client('s3')
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


def creatingDataframe(key, body):
    """This method is creatingDataframe"""
    if key.endswith("csv"):
        return pd.read_csv(body)
    elif key.endswith("xlsx"):
        return pd.read_excel(body)
    elif key.endswith(".json"):
        return pd.read_json(body)
    else:
        return None


def updateMetadataRecord(target_metadata, job_id):
    """This method is for updating Metadata Record"""
    try:
        sql = """
                  UPDATE source_file_config  SET target_metadata = %s from jobs as j
                  WHERE j.id  = job_id and j.uuid = %s
              """
        data = [
            (json.dumps(target_metadata), job_id)
        ]
        cursor.executemany(sql, data)
        conn.commit()
        return None
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(400, str(e), None)


def applyingTransformation(target_df, target_values):
    try:
        for item in target_values:
            if "transformation" in item and item['transformation'] is not None:
                if "transformation_type" in item['transformation'] and item['transformation']['transformation_type'] == "concatenation":
                    target_df = concatenation_of_columns(item, target_df)
                elif "transformation_type" in item['transformation'] and item['transformation']['transformation_type'] == "derived_value":
                    target_df = derive_value_of_columns(item, target_df)
        return target_df
    except Exception as error:
        print(error)
        return response(500, str(error), None)


def sending_file_s3(job_id, target_df):
    s3 = boto3.client('s3')
    bucket_name = s3_bucket
    file_key = "target-files/" + str(job_id) + '/data.csv'
    if isinstance(target_df, dict):
        target_df = pd.DataFrame.from_dict(target_df)
    csv_buffer = StringIO()
    target_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_data)


def adding_data_df(target_df, source_df, target_values):
    """adding Data to Target DataFrame"""
    for item in target_values:
        if "column_type" in item and item['column_type'] == 'New':
            item['source_column'] = item['target_column']
            source_df.loc[0:(len(item['values']) - 1) if 'values' in item else -1, item['target_column']] = item['values'] if 'values' in item else []

    target_df[[item['target_column'] for item in target_values]] = source_df[[item['source_column'] for item in target_values]].copy()
    return target_df


def concatenation_of_columns(item, df):
    """This method is for concatenating the source columns and adding the new column and returning the updated DF."""
    try:
        transformation = item['transformation']
        source_columns = transformation['source_columns']
        separator = transformation['seperator']
        target_column = item['target_column']

        for col in source_columns:
            df[col] = df[col].astype(str).fillna('')

        df[target_column] = df[source_columns].apply(lambda x: separator.join(x), axis=1)
        return df
    except Exception as error:
        print("error type ", error)
        return response(500, str(error), None)


def derive_value_of_columns(item, df):
    """This method is for creating a new column with derived values of existing columns."""
    try:
        transformation = item['transformation']
        source_columns = transformation['source_columns']
        operators = transformation['operator']
        target_column = item['target_column']

        new_column = df[source_columns[0]]
        for operator, column in zip(operators, source_columns[1:]):
            if operator == "+":
                new_column = new_column + df[column]
            elif operator == "-":
                new_column = new_column - df[column]
            elif operator == "*":
                new_column = new_column * df[column]

        df[target_column] = new_column
        return df
    except Exception as error:
        print(error)
        return response(500, str(error), None)


def response(stautsCode, message, data):
    """This is response handling method"""
    return {
        "statusCode": stautsCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": data})
    }