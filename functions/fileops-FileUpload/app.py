"""This module is for creating business processes, jobs and uploading file to S3"""
import json
import uuid
import boto3
from dbconnection import cursor, conn
import psycopg2
import os
from botocore import client
env = os.environ.get('Environment')
# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    """This handler is for creating and editing businesses process and jobs"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return createBusinessProcess(body)
            else:
                return response(400, 'please provide body', None)

        elif event['resource'] == "/job" and event['httpMethod'] == 'PUT':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return edit_email_and_jira_list(body)
            else:
                return response(400, 'please provide body', None)

        elif event['resource'] == "/job/file_upload" and event['httpMethod'] == 'POST':
            body = json.loads(event['body'])
            # body = event['body']
            if body is not None:
                type = "job"
                s3_file = "sample-files/" + body['job_id'] + "/" + body['s3_filename']
                return getPreSignedURL(s3_file, type, body)
            else:
                return response(400, 'please provide body', None)

        elif event['resource'] == "/jira/upload" and event['httpMethod'] == 'GET':
            filepath = prepareS3Key(event)
            type = "jira"
            return getPreSignedURL(filepath, type, 'null')

        elif event['resource'] == "/job" and event['httpMethod'] == 'DELETE':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return DeleteJob(body)
            else:
                return response(400, 'please provide body', None)

        else:
            return response(400, 'resource or endpoint not found', None)
    except Exception as error:
        print(error)
        return response(500, str(error), None)


def createBusinessProcess(body):
    """This method is for creating the business process and jobs"""
    try:
        user_uuid = body["user_id"]
        business_process_id = body.get('business_process_id', None)
        # Calling getUserDetails method
        user_intid = getUserDetails(user_uuid)
        business_status = "Draft"
        if user_intid is None:
            return response(400, f"Invalid user id{user_uuid}", None)

        else:
            if business_process_id is None:
                if "group_id" in body and body.get("group_id") != "" and body.get("group_id") != "undefined":
                    group_id = body['group_id']
                else:
                    print("group_id is missing in request body")
                    return response(400, "group_id is missing in request body", None)
                # Confirming whether the Business Process name is unique
                business_process_name = business_process_name_unique(body)
                if business_process_name != "unique":
                    return response(409,
                                    "The business process already exists with the given name, please provide the unique name",
                                    None)
                else:
                    insert_to_business_process = """
                                                  INSERT INTO business_process(uuid, name, created_by, status, email_notification, no_of_files, jira_service_management)
                                                  VALUES(%s, %s, %s, %s, %s, %s, %s)
                                                  RETURNING *
                                                 """
                    values = (
                        str(uuid.uuid4()),
                        body["business_process_name"],
                        user_intid,
                        business_status,
                        body.get('email_notification', []),
                        body.get("no_of_files"),
                        json.dumps(body.get('jira_service_management', {}))
                    )
                    cursor.execute(insert_to_business_process, values)
                    conn.commit()
                    inserted_row = cursor.fetchone()
                    data = {
                        'user_id': user_intid,
                        'business_process_int_id': inserted_row[0],
                        "business_process_id": inserted_row[1]
                    }
                    # adding the new business process to the user group
                    bp_uuid = inserted_row[1]
                    new_business_process = json.dumps(
                        {bp_uuid: body["business_process_name"]})
                    update_group_query = "UPDATE groups SET business_process_list = business_process_list || %s WHERE uuid = %s"
                    cursor.execute(update_group_query,
                                   (new_business_process, group_id))
                    conn.commit()
                    # Calling createJob method
                    return createJob(body, data)
            else:
                # This block is for creating multiple jobs
                business_process_int_id = businessDetails(business_process_id)
                data = {
                    "business_process_int_id": business_process_int_id,
                    "user_id": user_intid,
                    "business_process_id": business_process_id
                }
                return createJob(body, data)

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, str(error), None)


def createJob(body, data):
    """This method is for creating the jobs"""
    status = "Draft"
    try:
        insert_into_jobs = """
                                INSERT INTO jobs(uuid, job_name, created_by, status, business_process_id)
                                VALUES(%s, %s, %s, %s, %s)
                                RETURNING *
                               """
        values = (
            str(uuid.uuid4()),
            body["job_name"],
            data['user_id'],
            status,
            data['business_process_int_id']
        )
        cursor.execute(insert_into_jobs, values)
        conn.commit()
        job_data = cursor.fetchone()
        response_data = {
            "business_process_id": data['business_process_id'],
            "job_id": job_data[1]
        }

        # Updating the no_of_files column of the business process table
        update_query = """
                        UPDATE business_process
                        SET no_of_files = %s
                        WHERE id = %s
                        """
        update_values = (body.get('no_of_files', ''), data['business_process_int_id'])
        cursor.execute(update_query, update_values)
        conn.commit()

        return response(200, "Created successfully", response_data)
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, error, None)


def edit_email_and_jira_list(body):
    """ This method is for updating emails list and jira status """
    try:
        bp_uuid = body.get('business_process_id', '')
        if bp_uuid != '':
            update_business_process_table = f"""
                                            UPDATE business_process 
                                            SET email_notification = %s,
                                                jira_service_management = %s
                                            WHERE uuid = '{bp_uuid}'
                                            """
            values = (body.get('email_notification', ''), json.dumps(body.get('jira_service_management', {})))
            cursor.execute(update_business_process_table, values)
            conn.commit()
            return response(200, "Updated successfully", None)
        else:
            return response(400, "please provide valid uuid", None)

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, str(error), None)


def getPreSignedURL(s3_file, type, body):
    """This method is for generating Pre-SignedURL"""
    try:
        s3_client = boto3.client('s3', config=client.Config(signature_version='s3v4'))
        # Generating the pre-signed URL
        pre_signed_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': s3_bucket, 'Key': s3_file},
            ExpiresIn=600
        )
        if type == "job":
            # Updating the file_name and size in jobs table
            update_query = """
                           UPDATE jobs
                           SET file_name = %s,
                               file_size = %s
                           WHERE uuid = %s
                        """
            update_values = (body.get('s3_filename', ''), body.get('file_size', ''), body['job_id'])
            cursor.execute(update_query, update_values)
            conn.commit()
            # Here calling a method to update the top_ten_rows column
            updateTop10RowsColumn(body['job_id'])

            data = {
                "presignedURL": pre_signed_url,
                "job_id": body['job_id'],
                "business_process_id": body['business_process_id']
            }
        else:
            data = {
                "presignedURL": pre_signed_url,
                "filepath": s3_file
            }
        return response(200, "The presignedURL is ready", data)
    except Exception as error:
        print("The Error", str(error))
        return response(400, str(error), None)


def getJobDetails(job_uuid):
    """This method is for getting the job details"""
    try:
        query = f"SELECT * FROM jobs WHERE uuid = '{job_uuid}'"
        cursor.execute(query)
        data = cursor.fetchone()
        conn.commit()
        if data is None:
            return None
        else:
            return data

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(503, str(e), None)
    except Exception as error:
        print("The Error", str(error))
        return response(500, str(error), None)


def updateTop10RowsColumn(job_uuid):
    """This method is for updating the top_ten_rows column in the source-file-config table to empty"""
    try:
        data = getJobDetails(job_uuid)
        job_int_id = data[0]
        update_query = f"""
                           UPDATE source_file_config
                           SET top_ten_rows = %s
                           WHERE job_id = '{job_int_id}'
                        """
        update_values = (None,)
        cursor.execute(update_query, update_values)
        conn.commit()

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, str(error), None)


def DeleteJob(body):
    """This method is for soft deleting the job"""
    try:
        job_id = body['job_id']

        # the below statement sets the status of the job id as deleted
        query = "UPDATE jobs SET status = %s WHERE uuid = %s"
        values = ('Deleted', job_id)
        cursor.execute(query, values)
        conn.commit()

        # the below statement updates the no_of_files of the business_process table
        update_no_of_files_query = """UPDATE business_process 
                    SET no_of_files = no_of_files-1
                    WHERE id = (SELECT
                    business_process_id FROM jobs 
                    WHERE uuid = %s) 
                    and no_of_files > 0"""
        cursor.execute(update_no_of_files_query, (job_id,))
        conn.commit()

        return response(200, "Job deleted successfully", None)

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(409, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, error, None)


def prepareS3Key(event):
    userId = "afac5bdf-700c-49fe-bcab-18c1059960ec"
    # or
    # event['requestContext']['authorizer']['claims']['sub']
    return f"pemfiles/{userId}/privatekey.pem"


def business_process_name_unique(body):
    """This method is for checking the given business process name is unique or not"""
    try:
        business_process_name = body["business_process_name"]
        business_process_name_query = "SELECT * FROM business_process WHERE status != 'Deleted' and name = %s"
        cursor.execute(business_process_name_query, (business_process_name,))
        data = cursor.fetchone()
        conn.commit()
        if data is None:
            result = "unique"
            return result
        else:
            result = "not unique"
            return result

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, str(error), None)


def businessDetails(business_process_id):
    """This method is for getting the business process details"""
    try:
        get_business_process_details = (
            "SELECT id FROM business_process WHERE uuid = %s"
        )
        cursor.execute(get_business_process_details, (business_process_id,))
        data = cursor.fetchone()
        conn.commit()
        if data is not None:
            business_process_intid = data[0]
            return business_process_intid
        else:
            business_process_intid = None
            return business_process_intid

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
        return response(500, str(error), None)


def getUserDetails(user_uuid):
    """This method is for getting user details"""
    try:
        get_int_user_id = "SELECT id FROM users WHERE uuid = %s"
        cursor.execute(get_int_user_id, (user_uuid,))
        user_intid = cursor.fetchone()
        conn.commit()
        if user_intid is not None:
            return user_intid[0]
        else:
            user_intid = None
            return user_intid

    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return response(500, str(e), None)
    except Exception as error:
        conn.rollback()
        print("The Error", str(error))
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
        "body": json.dumps({"message": message, "data": data}),
    }
