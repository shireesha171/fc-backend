"""This module is for storing the error is database"""
import json
import os
import uuid
from datetime import datetime

import boto3
import psycopg2
from botocore.exceptions import ClientError

from dbconnection import cursor, conn

sns_topic = os.environ.get('Email_Notification_Sns_Topic')

def error_storing_db(errorList, buiness_process_valid, complete_error_list, business_validate_id, params):
  # print("buiness_process_valid", buiness_process_valid)
  """This method is for error_storing_db"""
  try:
    error_records = {
      'range_check_error_rows': complete_error_list.get('range_rows')['value'] if "range_rows" in complete_error_list and  "value" in complete_error_list.get('range_rows') else [],
      'list_value_error_rows': complete_error_list.get('list_value_rows')['value']  if "list_value_rows" in complete_error_list and  "value" in complete_error_list.get('list_value_rows') else [],
      "null_rows": complete_error_list.get('null_rows')["value"] if "null_rows" in complete_error_list and  "value" in complete_error_list.get('null_rows') else []
    }
    error_records = json.dumps(error_records)
    data = json.dumps(errorList)
    date = datetime.utcnow()
    qurey = """
      INSERT INTO job_runs(uuid, errors,job_id,business_user_validate_process_id,
      status,error_records,job_type, "created_at")
                   VALUES(%s, %s, %s,%s,%s,%s,%s,%s)
                   RETURNING *
               """
    values = (str(uuid.uuid4()), data, buiness_process_valid['job_id'],
              buiness_process_valid['business_validate_process_id'], "Completed",error_records, "Scheduled" if ('job_type' in params and params['job_type'] =='Scheduled')  else "Ad hoc" ,date)
    cursor.execute(qurey, values)
    inserted_row = cursor.fetchone()
    conn.commit()

    job_run_query = """SELECT jr.uuid , jr.status, jr.errors , buvp.business_process_id , buvp.job_id 
                          FROM business_user_validate_process as buvp
                          left JOIN job_runs as jr ON jr.business_user_validate_process_id = buvp.id
                          WHERE buvp.uuid= %s and buvp.job_id = %s """
    
    cursor.execute(job_run_query, (business_validate_id, buiness_process_valid['job_id']))
    job_run_details = cursor.fetchone()

    bp_details_query = """SELECT j.uuid, j.job_name, j.job_type, j.file_name,j.file_size, sfc.dqrules,sfc.standard_validations,sfc.file_config_details,sfc.email_notification,
                                          sfc.jira_incident_to_be_raised_for_failures, 
                                          bp."name" as business_name, bp.email_notification as email_recipient_list, bp.jira_incident_to_be_raised_for_failures as jira_recipient_list, 
                                          sc."name" as source_name,sc.absolute_file_path as source_absolute_file_path,
                                          tc."name" as target_name,tc.absolute_file_path as target_absolute_file_path
                                          FROM jobs AS j
                                          JOIN business_process as bp on bp.id = j.business_process_id
                                          JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                          left join target_config as tc on sfc.target_id = tc.id
                                          left join source_config as sc on sc.id = sfc.source_id
                                          WHERE bp.id =  %s 
                                      """
    cursor.execute(bp_details_query, [job_run_details[3]])
    bp_data = cursor.fetchall()
    if bp_data is not None:
      bp_data_record = bp_data[-1]
      if bp_data_record[8] == True:
        sns_data = {
            "record_type": "JOB_STATUS_EMAIL_NOTIFICATION",
            "job_id": bp_data_record[0],
            "job_name": bp_data_record[1],
            "job_type": bp_data_record[2],
            "file_name": bp_data_record[3],
            "file_size": bp_data_record[4],
            "column_validation": bp_data_record[5],
            "standard_validations": bp_data_record[6],
            "file_config_details": bp_data_record[7],
            "email_notification_enablement": bp_data_record[8],
            "jira_notification_enablement": bp_data_record[9],
            "business_process_name": bp_data_record[10],
            "email_recipient_list": bp_data_record[11],
            "jira_recipient_list": bp_data_record[12],
            "source_name": bp_data_record[13],
            "source_absolute_file_path": bp_data_record[14],
            "target_name": bp_data_record[15],
            "target_absolute_file_path": bp_data_record[16],
            "job_status": job_run_details[1],
            "file_errors": {
              "count": 0,
                "file_type_check": {
                  "value" : "",
                  "status": "PASS"
                },
                "file_size_check": {
                  "value" : "",
                  "status": "PASS"
                }
            },
            "column_header_errors": {
              "count": 0,
              "additional_columns": 0,
              "missing_columns": 0,
            },
            "data_errors": {
                "count": 0,
                "delimeter_errors": 0,
                "encoding_check": "PASS",
                "rang_check_error": "PASS",
                "null_values" : 0,
                "blanks" : 0,
                "list_of_value_errors" : 0,
            }
        }

        for item in job_run_details[2]:
          if item["name"] == "file_extension":
            sns_data["file_errors"]["file_type_check"]["value"] = item["value"][0]
            sns_data["file_errors"]["file_type_check"]["status"] = item["value"][1]
            if item["value"][1] == 'FAIL':
                sns_data["file_errors"]['count'] += 1
          elif item["name"] == "file_size_range":
            sns_data["file_errors"]["file_size_check"]["value"] = item["value"][2] + '-' + item["value"][3]
            sns_data["file_errors"]["file_size_check"]["status"] = item["value"][1]
            if item["value"][1] == 'FAIL':
                sns_data["file_errors"]['count'] += 1    
          elif item["name"] == "additional_columns":
            sns_data["column_header_errors"]["additional_columns"] = len(item["value"])
            sns_data["column_header_errors"]["count"] += len(item["value"])
          elif item["name"] == "missing_columns":
            sns_data["column_header_errors"]["missing_columns"] = len(item["value"])
            sns_data["column_header_errors"]["count"] += len(item["value"])
          elif item["name"] == "file_delimiter":
            sns_data["data_errors"]["delimeter_errors"] = len(item["value"][0])
            sns_data["data_errors"]["count"] += len(item["value"][0])
          elif item["name"] == "file_encoding":
            sns_data["data_errors"]["encoding_check"] = item["value"][1]
            if item["value"][1] == 'FAIL':
              sns_data["data_errors"]["count"] +=  1
          elif item["name"] == "total_null_count":
            sns_data["data_errors"]["blanks"] = item["value"]
            sns_data["data_errors"]["count"] +=  item["value"]
            
            sns_data["data_errors"]["null_values"] = item["value"]
            sns_data["data_errors"]["count"] +=  item["value"]
          elif item["name"] == "number_of_list_value_error":
            sns_data["data_errors"]["list_of_value_errors"] = len(item["value"])
            sns_data["data_errors"]["count"] +=  len(item["value"])
        
        print("sns_data :", sns_data)
        snsnotification(sns_data)

    return inserted_row
  except psycopg2.Error as e:
    print("An error occurred:", e)
    conn.rollback()
    return str(e)
  except Exception as error:
    print("An error occurred:", error)
    conn.rollback()
    return str(error)


def snsnotification(data):
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
            print(f"An unexpected error occurred : {e}")
    except Exception as e:
        # Handle unexpected errors
        print(f"An unexpected error occurred: {e}")