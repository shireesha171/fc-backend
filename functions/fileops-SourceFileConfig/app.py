"""This module is for source file configuration"""
import json
import uuid
import psycopg2
import os
import boto3
from dbconnection import cursor, conn
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    """This handler is for creating and getting the source file configurations"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job/configuration" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                job_int_id, created_by = getJobDetails(body['job_id'])
                get_obj_remaining = getfilefromS31(body['job_id'])
                if len(get_obj_remaining) > 0:
                    delete_files_in_s3(get_obj_remaining)
                if check_file_config_job_id(job_int_id):
                    return response(400, str("file config details are already exists for this job"), None)
                return createSourceFileConfigurations(body,job_int_id, created_by)
            else:
                return response(400, 'please provide body', None)
        elif event['resource'] == "/job/configuration" and event['httpMethod'] == 'GET':
            query_params = event.get("queryStringParameters", None)
            if query_params is not None and query_params["job_id"] != "":
                return getSourceFileConfigurations(query_params)
            else:
                return response(400, "Missing or invalid query parameters", None)
        elif event['resource'] == "/job/configuration" and event['httpMethod'] == 'PUT':
            if 'body' in event:
                body = json.loads(event['body'])
                get_obj_remaining = getfilefromS31(body['job_id'])
                if len(get_obj_remaining) > 0:
                    delete_files_in_s3(get_obj_remaining)
                return updateSourceFileConfigurations(body)
            else:
                return response(400, 'please provide body', None)
        else:
            return response(400, 'resource or endpoint not found', None)
    except Exception as error:
        print("The error: ", str(error))
        return response(500, str(error), None)


def getJobDetails(job_uuid):
    """This method is for getting job details"""
    try:
        query = "SELECT id, created_by FROM jobs where uuid = %s"
        cursor.execute(query, (job_uuid,))
        data = cursor.fetchone()
        conn.commit()
        if data is not None:
            job_int_id, created_by = data
            return job_int_id, created_by
        else:
            job_int_id = None
            created_by = None
            return job_int_id, created_by
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The error: ", str(error))
        return response(500, str(error), None)


def getIntIds(table_name, uuid):
    """This method is for getting source int id and target int id"""
    try:
        if uuid != '':
            get_query = f"SELECT id from {table_name} WHERE uuid = %s"
            cursor.execute(get_query, (uuid,))
            int_id = cursor.fetchone()
            conn.commit()
            return int_id
        else:
            return None
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The error: ", str(error))
        return response(500, str(error), None)


def getUuid(table_name, id):
    """This method is for getting source_uuid and target_uuid"""
    try:
        if id != '':
            get_query = f"SELECT uuid from {table_name} WHERE id = %s"
            cursor.execute(get_query, (id,))
            uuid_ = cursor.fetchone()
            conn.commit()
            return uuid_
        else:
            return None
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print(error)
        return response(500, str(error), None)


def getSourceFileConfigurations(query_params):
    """This method is for getting the source file configurations"""
    try:
        job_uuid = query_params['job_id']
        job_int_id, created_by = getJobDetails(job_uuid)

        if job_int_id is not None:
            get_source_file_configuration_details = """
                                                        SELECT sfc.source_id, sfc.target_id, sfc.file_config_details, sfc.standard_validations, j.job_type, sfc.email_notification, j.delimiter 
                                                        FROM source_file_config sfc
                                                        JOIN jobs j ON sfc.job_id  = j.id                  
                                                        WHERE sfc.job_id = %s      
                                                    """
            cursor.execute(get_source_file_configuration_details, (job_int_id,))
            data = cursor.fetchone()
            conn.commit()

            if data is not None:
                source_uuid = getUuid('source_config', data[0])
                target_uuid = getUuid('target_config', data[1])
                response_data = {
                    "job_id": job_uuid,
                    "source_id": source_uuid[0] if source_uuid is not None else None,
                    "target_id": target_uuid[0] if target_uuid is not None else None,
                    "file_configurations": data[2],
                    "standard_validations": data[3],
                    "job_type": data[4],
                    "email_notification": data[5],
                    "delimiter": data[6]
                }
                return response(200, "The fetched data is ready", response_data)
            else:
                return response(404, "No configuration data found for the given job_id", None)
        else:
            return response(400, "Could not fetch job details", None)

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The error: ", str(error))
        return response(500, str(error), None)


def updateSourceFileConfigurations(body):
    """This method is for updating the source file configurations"""
    try:
        job_uuid = body['job_id']
        job_int_id, created_by = getJobDetails(job_uuid)
        if job_int_id is not None:
            insert_into_sfc = """
                                  UPDATE source_file_config
                                  SET file_config_details = %s,
                                      standard_validations = %s
                                  WHERE job_id = %s
                              """
            values = (
              json.dumps(body.get("file_configurations",{})),
              json.dumps(body.get("standard_validations", {})),
              job_int_id
            )
            cursor.execute(insert_into_sfc, values)
            conn.commit()
            return response(200, "The data updated successfully", body)
        else:
            return response(400, "Provide valid job_id", None)
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.commit()
        print("The Error:", error)
        return response(500, str(error), None)


def createSourceFileConfigurations(body,job_int_id, created_by):
    """This method is for creating the source file configurations"""
    try:
        job_uuid = body['job_id']
        # job_int_id, created_by = getJobDetails(job_uuid)
        source_int_id = getIntIds('source_config', body.get('source_id', ''))
        target_int_id = getIntIds('target_config', body.get('target_id', ''))

        if job_int_id is not None and created_by is not None:
            insert_into_sfc = """
                             INSERT INTO source_file_config(uuid, source_id, target_id, file_config_details, standard_validations, created_by, job_id, email_notification)
                             VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                             RETURNING *
                           """
            values = (
              str(uuid.uuid4()),
              source_int_id,
              target_int_id,
              json.dumps(body.get("file_configurations",{})),
              json.dumps(body.get("standard_validations", {})),
              created_by,
              job_int_id,
              body.get('email_notification') == "True"
            )
            cursor.execute(insert_into_sfc, values)
            conn.commit()

            update_jobs = """
                            UPDATE jobs 
                            SET job_type = %s
                            WHERE uuid = %s 
                          """
            job_type = "scheduled" if body["scheduled"] == "true" else "adhoc"
            cursor.execute(update_jobs, (job_type, job_uuid))
            conn.commit()
            return response(200, "The data inserted successfully", body)
        else:
            return response(400, "Provide valid job_id", None)
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error:", error)
        return response(500, str(error), None)


def check_file_config_job_id(job_id):
    try:
        query = f"SELECT uuid FROM source_file_config where job_id=%s"
        cursor.execute(query, (job_id,))
        uuid = cursor.fetchone()
        print(uuid)
        if uuid is not None:
            return True
        return False
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("Theerror: ", str(error))
        return response(500, str(error), None)


def response(status_code, message, data):
    """This is response method"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
      "body": json.dumps({"message": message, "data": data})
    }


def delete_files_in_s3(objs):
    try:
        # Create an S3 client
        s3 = boto3.client('s3')
        files_to_delete = [{'Key': item['Key']} for item in objs]
        # Delete the specified files from the S3 bucket
        s3_response = s3.delete_objects(
            Bucket=s3_bucket,
            Delete={
                'Objects': files_to_delete,
                'Quiet': False
            }
        )

        # Print the response
        print(s3_response)
    except psycopg2.DatabaseError as error:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(error), None)
    except Exception as error:
        conn.rollback()
        print(error)
        return response(500, str(error), None)


def getfilefromS31(uuid):
    """This method is getting file from S3"""
    bucket_name = s3_bucket
    folder_path = 'sample-files/' + uuid + "/"
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        objects = response.get('Contents', [])
        sorted_objects = sorted(objects, key=lambda x: x['LastModified'])
        print(sorted_objects)
        sorted_objects.pop()
        return sorted_objects
        # print(response)
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print(error)
        return response(500, str(error), None)
