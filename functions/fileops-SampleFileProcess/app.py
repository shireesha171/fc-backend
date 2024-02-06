"""This module is for sample file process"""
import json
import boto3
import pandas as pd
import psycopg2
from dbconnection import cursor, conn
from botocore.exceptions import ClientError
import os
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    """This method is for handling sample file processing"""
    print("Received Event: ", str(event))
    query_params = event.get('queryStringParameters', None)
    if query_params is not None and "job_id" in query_params:
        query_params = event['queryStringParameters']
        uuid = query_params['job_id']
        obj = {
            'uuid': uuid
           }
    else:
        str_ = "job_id not found"
        return response_body(400, "", body=str_)
    # Checking if top ten records are present in the DB
    status = CheckTopTenRecordsStatus(obj['uuid'])
    if status == "Yes":
        top_ten_rows = getTopTenRows(obj['uuid'])
        return response_body(200, "Retrieved Successfully", top_ten_rows)
    else:
        s3_object = getfilefromS3(uuid)
        if s3_object is None:
            return response_body(400, "", body="No Sample file found neither format not support")
        try:
            sample_df = readings3file(s3_object['body'], s3_object['file'], uuid)
            metadata_df = schemaInference(sample_df)
            meta_data_json = dataFrametoJson(metadata_df)
            obj['metadata'] = meta_data_json
            sampleTopDf = sample_df.head(10)
            sampleTopDf = sampleTopDf.fillna('')
            df_list = sampleTopDf.to_dict(orient='records')
            temp = updateMetadataRecord(obj, df_list)
            if temp is None:
                return response_body(200, "Retrieved successfully", df_list)
        except Exception as e:
            print(f"Filedata error: {e}")
            return response_body(400, "Invalid file data", str(e))


def getfilefromS3(uuid):
    """This method is for getting file from S3"""
    bucket_name = s3_bucket
    folder_path = 'sample-files/' + uuid + "/"
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        s3_object = ""
        obj = {}
        for obj in response['Contents']:
            if obj['Key'].endswith("csv"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                obj = {"body": s3_object['Body'], "file": 'csv'}
            elif obj['Key'].endswith("xlsx"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                obj = {"body": s3_object['Body'], "file": 'xlsx'}
        return obj
    except ClientError as e:
        error_message = e.response['Error']['Message']
        return error_message


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


def schemaInference(df):
    """This method is for creating schemaInference ex: mandatory etc..."""
    create_df = pd.DataFrame(columns=['column', 'mandatory', 'dtype', 'precision', 'range_list_value'])
    for index,columnName in enumerate(df.columns):
        mandatory = df[columnName].notnull().all()
        dataType =  df[columnName].dtype
        precision = ""
        range_list_value = ""
        if dataType == 'object':
            date_type = check_date_time(df, columnName)
            dataType = 'string' if date_type is None else "date"
            range_list_value = df[columnName].unique() if dataType == 'string' else ""
            precision = df[columnName].str.len().max() if dataType == 'string' else ""
        if dataType == 'int64':
            min = df[columnName].min()
            max = df[columnName].max()
            range_list_value = [min, max]
            precision = df[columnName].astype(str).apply(len).max()

        create_df.loc[index] = [columnName, mandatory, dataType, precision, range_list_value]

    create_df['dtype'] = create_df['dtype'].astype(str)
    return create_df


# creating dataframe from json
def dataFrametoJson(df):
    """This method is for converting dataFrame to Json"""
    return df.to_json(orient="records")


def getTopTenRows(job_id):
    try:
        get_top_ten_rows = """
                            SELECT sfc.top_ten_rows 
                            FROM source_file_config AS sfc 
                            JOIN jobs AS j ON j.id = sfc.job_id
                            WHERE j.uuid = %s;
                           """
        cursor.execute(get_top_ten_rows, (job_id,))
        data = cursor.fetchone()
        conn.commit()
        return data[0]
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response_body(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error: ", error)
        return response_body(500, str(error), None)


def CheckTopTenRecordsStatus(job_id):
    try:
        with conn.cursor() as cursor:
            selectSQl = "SELECT * FROM jobs WHERE uuid = %s"
            cursor.execute(selectSQl, (job_id,))
            rows = cursor.fetchall()

            if len(rows) > 0:
                row = rows[0]
                query = "SELECT top_ten_rows FROM source_file_config WHERE job_id = %s"
                cursor.execute(query, (row[0],))
                data = cursor.fetchone()

                if data[0] is not None:
                    top_ten_rows_there = "Yes"
                    return top_ten_rows_there
                else:
                    top_ten_rows_there = "No"
                    return top_ten_rows_there
            else:
                return response_body(400, "", body="job_record not found")

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response_body(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error: ", error)
        return response_body(500, str(error), None)
    finally:
        cursor.close()


def updateMetadataRecord(obj, df_list):
    """This method is for updating Metadata Record"""
    try:
        selectSQl = "SELECT * FROM jobs  WHERE uuid = %s"
        data = [
            obj['uuid']
        ]
        cursor.execute(selectSQl, data)
        rows = cursor.fetchall()
        conn.commit()
        if len(rows) > 0:
            row = rows[0]
            sql = """UPDATE source_file_config  
                     SET metadata = %s, top_ten_rows = %s
                     WHERE job_id = %s"""
            data_update = ((obj['metadata']), json.dumps(df_list), row[0])

            cursor.execute(sql, data_update)
            conn.commit()
            return None
        else:
            return response_body(400, "", body="job_record not found")
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response_body(401, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error: ", error)
        return response_body(500, str(error), None)


def response_body(statuscode, message, body):
    """This is a response method"""
    response = {
        "statusCode": statuscode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods":"*",
            "Access-Control-Allow-Headers":"*",
            },
            "body":json.dumps({"message": message, "data":body})
    }
    return response


def check_date_time(df,column_name):
    pattern1 = r'\d{2}-\d{2}-\d{4}'
    pattern2 = r'\d{4}-\d{2}-\d{2}'
    pattern3 = r'\d{2}/\d{2}/\d{4}'
    pattern4 = r'\d{4}/\d{2}/\d{2}'
    patterns = [pattern1,pattern2,pattern3,pattern4]
    data = [df[df[column_name].str.match(pattern).fillna(False)] for pattern in patterns]
    data = max(len(item) for item in data)
    print(data, column_name)
    return column_name if data > 0 else None