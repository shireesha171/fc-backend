"""This is medule is for validating the files by business users"""
import uuid
import json
import boto3
from dbconnection import cursor, conn
import psycopg2
import os
from botocore import client
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
def lambda_handler(event, context):
    """This method is for business user process"""
    print("Received Event: ", str(event))
    try:
        body = json.loads(event['body'])
        business_process_id = body['business_process_id']
        job_id = body['job_id']
        file_uploaded_url = body['file_uploaded_url']
        file_name = body['file_name']
        user_id = body['user_id']
        # getting userDetails from User-table
        user_intid = user_details(user_id)
        #  getting  business_process details from table
        business_process_id = business_process(business_process_id)
        #  getting job_id from job table based on uuid
        job_id = job_details(job_id)
        # Inserting data into the business_validate_process table
        try:
            qurey = """
                           INSERT INTO business_user_validate_process(uuid, business_process_id,file_name, file_location_path, created_by,job_id)
                           VALUES(%s, %s, %s, %s,%s,%s)
                           RETURNING *
                       """
            values = (str(uuid.uuid4()), business_process_id, file_name, file_uploaded_url, user_intid, job_id)
            cursor.execute(qurey, values)
            conn.commit()
            inserted_row = cursor.fetchone()
            business_process_validate_id = inserted_row[1]
            print(inserted_row, "This is a row inserted in to business_process table")

            #  creating presigned url
            return uploadS3(file_name, business_process_validate_id)
        except psycopg2.DatabaseError as e:
            conn.rollback()
            print(f"Database error: {e}")
            return response(401, str(e), None)

    except Exception as error:
        print(error)
        return response(401, str(error), None)


def uploadS3(s3_filename, business_process_uuid):
    """This method is for uploding the validating file to S3"""
    s3_file = "sample-files/" + business_process_uuid + "/" + s3_filename
    try:
        s3_client = boto3.client('s3', config=client.Config(signature_version='s3v4'))
        print("before url")
        # Generate the presigned URL
        pre_signed_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': s3_bucket, 'Key': s3_file},
            ExpiresIn=600  # URL expiration time in seconds (adjust as needed)
        )
        print(pre_signed_url)
        print("bp_id---",business_process_uuid)
        data = {
            "presignedURL": pre_signed_url,
            "business_process_validate_id": business_process_uuid
        }
        return response(200, "The presignedURL is ready", data)

    except Exception as error:
        print("check", error)
        return response(401, str(error), None)


# getting user_details from db
def user_details(user_id):
    """This method is to get the user details"""
    try:
        users_table_get_query = "SELECT id FROM users WHERE uuid = %s"
        cursor.execute(users_table_get_query, (user_id,))
        user_intid = cursor.fetchone()[0]
        return user_intid

    except psycopg2.Error as e:
        print("An error occurred:", e)
        return str(e)


def business_process(business_process_id):
    """This method is to get the business process details"""
    try:
        business_validation_process_table_get_query = "SELECT id FROM business_process WHERE uuid = %s"
        cursor.execute(business_validation_process_table_get_query, (business_process_id,))
        business_process_id = cursor.fetchone()
        return business_process_id

    except psycopg2.Error as e:
        print("An error occurred:", e)
        return str(e)


# getting job details from db
def job_details(job_id):
    """This method is for getting the job details"""
    try:
        job_query = "SELECT id FROM jobs WHERE uuid = %s"
        cursor.execute(job_query, (job_id,))
        job_id = cursor.fetchone()
        return job_id

    except psycopg2.Error as e:
        print("An error occurred:", e)
        return str(e)


def response(stautsCode, message, data):
    """This is response method"""
    print("sending response", stautsCode, message, data)
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











