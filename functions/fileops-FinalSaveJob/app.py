import psycopg2
from dbconnection import cursor, conn
import json
import boto3
import os
from botocore.exceptions import ClientError
import pandas as pd

env = os.environ.get('Environment')
scheduler_sns_topic = os.environ.get('Scheduler_Sns_Topic')
email_notification_sns_topic = os.environ.get('Email_Notification_Sns_Topic')

#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'

def lambda_handler(event, handler):
    print("Received Event: ", str(event))
    query_params = event.get('queryStringParameters', None)
    if event['resource'] == "/job/final-save" and event['httpMethod'] == 'POST':
        if 'body' in event:
            body = json.loads(event['body'])
            return businessProcessStatusUpdate(body)
    elif event['resource'] == "/job/final-save" and event['httpMethod'] == 'GET':
        return sourceConfigDetails(query_params)


def businessProcessStatusUpdate(body):
    if body is not None and "business_process_id" in body:
        business_process_id = body['business_process_id']
        job_id = body['job_id']
        try:
            jobs_query = """
                UPDATE jobs
                SET
                status = 'configured'
                where uuid = %s
                """
            values = (job_id,)
            cursor.execute(jobs_query, values)
            conn.commit()
            qurey = """
                UPDATE business_process
                SET
                status = sub.status
                from(
                select CASE WHEN count(*) >= bp.no_of_files THEN 'Active' ELSE 'Draft' END status
                from jobs as j join business_process as bp on bp.id = j.business_process_id
                where bp.uuid = %s and j.status = 'configured' group by bp.id )
                sub where uuid = %s
                """
            values = (business_process_id, business_process_id)
            cursor.execute(qurey, values)
            conn.commit()
            jobs_query1 = """
                              SELECT j.schedule_json, j.job_name, sc.file_config_details,
                                   sc2.location_pattern
                                  FROM jobs as j
    				              JOIN source_file_config as sc on j.id = sc.job_id
    				              left join source_config sc2 on sc2.id = sc.source_id
                                  WHERE j.uuid = %s
                              """
            values = (job_id,)
            cursor.execute(jobs_query1, values)
            record = cursor.fetchone()
            print(record)
            if record is not None and record[0] is not None:
                file_name_pattern = record[2]['file_name_pattern']
                location_pattern = record[3]
                sns_data = {
                    "job_id": job_id,
                    "business_process_id": business_process_id,
                    "schedule_json": record[0],
                    "job_name": record[1],
                    "env": env,
                    "action": "create",
                    "file_name_pattern": file_name_pattern,
                    "location_pattern": location_pattern
                }

                snsnotification(scheduler_sns_topic,sns_data)

            bp_details_query = """SELECT j.job_name, j.job_type, j.file_name,j.file_size, sfc.dqrules,sfc.standard_validations,sfc.file_config_details,sfc.email_notification,
                                          sfc.jira_incident_to_be_raised_for_failures, 
                                          bp."name" as business_name, bp.email_notification as email_recipient_list, bp.jira_incident_to_be_raised_for_failures as jira_recipient_list,
                                          sc."name" as source_name,sc.absolute_file_path as source_absolute_file_path,
                                          tc."name" as target_name,tc.absolute_file_path as target_absolute_file_path
                                          FROM jobs AS j
                                          JOIN business_process as bp on bp.id = j.business_process_id
                                          JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                          left join target_config as tc on sfc.target_id = tc.id
                                          left join source_config as sc on sc.id = sfc.source_id
                                          WHERE bp.uuid =  %s 
                                      """
            cursor.execute(bp_details_query, (business_process_id,))
            bp_data = cursor.fetchall()
            if len(bp_data) > 0:
                bp_data_record = bp_data[-1]
                if bp_data_record[7] == True:
                    sns_record = {
                        "record_type": "JOB_ONBOARD_EMAIL_NOTIFICATION",
                        "job_name": bp_data_record[0],
                        "job_type": bp_data_record[1],
                        "file_name": bp_data_record[2],
                        "file_size": bp_data_record[3],
                        "column_validation": bp_data_record[4],
                        "standard_validations": bp_data_record[5],
                        "file_config_details": bp_data_record[6],
                        "email_notification_enablement": bp_data_record[7],
                        "jira_notification_enablement": bp_data_record[8],
                        "business_process_name": bp_data_record[9],
                        "email_recipient_list": bp_data_record[10],
                        "jira_recipient_list": bp_data_record[11],
                        "source_name": bp_data_record[12],
                        "source_absolute_file_path": bp_data_record[13],
                        "target_name": bp_data_record[14],
                        "target_absolute_file_path": bp_data_record[15]
                    }

                    print("sns_record :", sns_record)

                    snsnotification(email_notification_sns_topic, sns_record)
            return response_body(200, "Updated Successfully", "")
        except psycopg2.Error as error:
            # Handle the error
            conn.rollback()
            print("Error executing update query:", error)
            return response_body(400, 'error', str(error))
        except Exception as error:
            conn.rollback()
            print("Error executing update query:", error)
            return response_body(400, str(error), None)
    else:
        return response_body(400, "Business_process_id not found", "")


def sourceConfigDetails(body):
    try:
        if body is not None and "business_process_id" in body:
            business_process_id = body["business_process_id"]
            get_business_procces_details = """SELECT j.file_name,j.job_type,j.job_name,sfc.standard_validations,sfc.file_config_details,sfc.email_notification,
                                          sfc.file_format,sfc.jira_incident_to_be_raised_for_failures,j.id, 
                                          j.schedule_json as job_schedule,bp.no_of_files,bp."name" as business_name,
                                          tc."name" as target_name,tc.host as target_host,tc.user_name as target_user_name,
                                          tc.connectivity_type as target_connectivity_type,tc.location_pattern as target_location_pattern,
                                          sc."name" as source_name,tc.host as source_host,tc.user_name as source_user_name,
                                          sc.connectivity_type as source_connectivity_type,sc.location_pattern as source_location_pattern
                                          FROM jobs AS j
                                          JOIN business_process as bp on bp.id = j.business_process_id
                                          JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                          left join target_config as tc on sfc.target_id = tc.id
                                          left join source_config as sc on sc.id = sfc.source_id
                                          WHERE bp.uuid =  %s 
                                      """
            cursor.execute(get_business_procces_details, (business_process_id,))
            get_business_procces_details_data = cursor.fetchall()
            cols = list(map(lambda x: x[0], cursor.description))
            df = pd.DataFrame(get_business_procces_details_data, columns=cols)
            records = df.to_dict(orient="records")
            return response_body(200, "retrive succesfully", records)

        else:
            return response_body(400, "provide business_process_id", "")


    except psycopg2.Error as error:
        conn.rollback()
        print("Error occurred:", error)
        return response_body(400, str(error), None)
    except Exception as error:
        print(error)
        return response_body(500, str(error), None)


def response_body(statuscode, message, body):
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


def snsnotification(sns_topic, data):
    try:

        sns = boto3.client('sns')
        payload = {
            'message': 'Sending payload to SNS from final Save Job ',
            'data': data
        }
        response = sns.publish(
            TopicArn=sns_topic,
            Message=json.dumps(payload)
        )
        print(f"Message published successfully with message ID: {response['MessageId']}")
    except ClientError as e:
        # Handle SNS client errors
        if e.response['Error']['Code'] == 'InvalidParameter':
            print(f"Invalid parameter error: {e}")
        elif e.response['Error']['Code'] == 'AuthorizationError':
            print(f"Authorization error: {e}")
        else:
            print(f"An unexpected error occurred: {e}")

    except Exception as e:
        # Handle unexpected errors
        print(f"An unexpected error occurred: {e}")