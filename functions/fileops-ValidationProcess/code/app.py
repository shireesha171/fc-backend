"""This module is for validating process"""
import json
import logging

import boto3
import pandas as pd

from .error_saving import error_storing_db
from .forming_errors import forming_error
from .logs import printlogs, error_logs
from .run_job_detail import run_job_detail_fun
from .file_validations import get_file_validations
from .schema_validations import getSchemaValidations
from .snspublisher import get_current_datatime

from dbconnection import cursor, conn

# JobRunLogger
from .utils.loggers import job_run_logger, update_job_metadata, update_job_run_timestamp
from .utils.store_logs import store_logs_db, store_logs_s3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')
from datetime import datetime


def replace_the_pattern_with_date(date_expression_string):
    date_pattern = r'\${(.*?)}'
    import re
    print("date_expression_string", date_expression_string)
    match = re.search(date_pattern, date_expression_string)
    if match:
        date = datetime.now().date()
        formatted_date = date.strftime(match.group(1))
        formatted_string = date_expression_string.replace(f"{match.group()}", formatted_date)
    else:
        print("No date format found in the input string.")
        formatted_string = date_expression_string
    return formatted_string


def lambda_handler(event, context):
    """This method is for handling validation  process"""

    update_job_run_time = update_job_run_timestamp()

    # JobRunLogger - Updating Start Time
    update_job_run_time.__next__()

    print("Received Event: ", str(event))
    try:
        query_params = event.get('queryStringParameters', None)
        if query_params is not None and "business_validate_id" in query_params:
            query_params = event['queryStringParameters']
            business_validate_id = query_params['business_validate_id']
            logger.info(
                "Business_process_validation  ========   " + business_validate_id + 'date_time----  ' + get_current_datatime())

            job_query = """
          SELECT sc.metadata,bvp.file_location_path, bvp.business_process_id, bvp.job_id, bvp.mailing_list, sc.dqrules,
          bvp.id as business_validate_process_id,j.uuid AS job_uuid, j.delimiter as delimiter, sc.file_config_details, 
          sc.standard_validations, tc."name" as target_name,tc.location_pattern as target_location_pattern,
          tc.connectivity_type as target_connectivity_type, tc.absolute_file_path as target_absolute_file_path,
          sc2."name" as source_name,sc2.location_pattern as source_location_pattern,
          sc2.absolute_file_path as source_absolute_file_path, sc2.connectivity_type as source_connectivity_type,
          bp.uuid as business_process_uuid
          FROM business_user_validate_process AS bvp join
          source_file_config as sc on bvp.job_id = sc.job_id
          join jobs AS j on j.id = bvp.job_id
          join business_process bp on bp.id = bvp.business_process_id
          left join target_config tc on tc.id = sc.target_id
          left join source_config sc2   on sc2.id = sc.source_id
          WHERE bvp.uuid = %s
           """
            cursor.execute(job_query, (business_validate_id,))
            data = cursor.fetchone()
            if data is None:
                return response_body(400, "record not found", "")
            cols = list(map(lambda x: x[0], cursor.description))
            df = pd.DataFrame([data], columns=cols)
            record = df.to_dict(orient="records")[0]
            job_uuid = record['job_uuid']
            job_int_id = record['job_id']

            # Job Run Logger - Business Process Id
            job_run_logger.business_process_id = record.get('business_process_uuid', None)

            printlogs(record, logger)

            if job_int_id is not None:
                if 'job_type' in query_params and query_params['job_type'] == "Scheduled":
                    query_params['location_pattern'] = replace_the_pattern_with_date(query_params['location_pattern'])
                    query_params['file_name_pattern'] = replace_the_pattern_with_date(query_params['file_name_pattern'])
                file_errors, file_validations_data = get_file_validations(record, logger,
                                                                          business_validate_id,
                                                                          query_params)
                on_error = record['standard_validations']['on_error'] if "on_error" in record[
                    'standard_validations'] else None
                delete_key = False
                if on_error == "terminated":
                    keys = list(file_validations_data.keys())
                    for key in keys:
                        value = file_validations_data[key]
                        if delete_key:
                            del file_validations_data[key]
                        if "status_type" in value and value['status_type'] == "Failure":
                            delete_key = True

                schema_validations_data = {}
                if "file_doesn't_exists" not in file_validations_data and delete_key == False:
                    schema_validations_data = getSchemaValidations(business_validate_id, record,
                                                                   logger, query_params)
                    delete_key = False
                    if on_error == "terminated":
                        keys = list(schema_validations_data.keys())
                        for key in keys:
                            value = schema_validations_data[key]
                            if delete_key:
                                del schema_validations_data[key]
                            if "status_type" in value and value['status_type'] == "Failure":
                                delete_key = True
                complete_error_list = {**schema_validations_data, **file_validations_data}
                logger.info("collecting error and success List " + get_current_datatime())
                error_list = forming_error(complete_error_list)
                job_run_record = error_storing_db(error_list, record, complete_error_list,
                                                               business_validate_id, query_params)
                # print(job_run_record,'job-run')
                logger.info("saving error and success  List " + get_current_datatime())
                error_logs(error_list, logger)
                if "range_rows" in schema_validations_data:
                    del schema_validations_data['range_rows']
                if "list_value_rows" in schema_validations_data:
                    del schema_validations_data['list_value_rows']
                get_run_job = run_job_detail_fun(job_run_record)

                # JobRunLogger - business_process_name, job_name, created_time, trigger_type, trigger_by, job_status
                update_job_metadata(get_run_job[0])

                # print(get_run_job,'get_run')
                # schema_validations_data.update({"file_errors":file_errors[0]})

                # JobRunLogger - Updating Completed Time
                update_job_run_time.__next__()

                # JobRunLogger - Job Id & Job Run Id
                job_run_id = job_run_record[1]
                job_run_logger.job_id = job_uuid
                job_run_logger.job_run_id = job_run_id

                # JobRunLogger - Store Logs
                job_run_logs_json = job_run_logger.to_json()

                store_logs_db(job_run_record[1], job_run_logs_json)
                store_logs_s3(job_uuid, job_run_id, job_run_logs_json)

                return response_body(200, "Retrive Successfully", get_run_job)
            else:
                return response_body(400, "Provide valid job_id", None)
        else:
            return response_body(400, "Business_validate_id not found", None)
    except Exception as error:
        conn.rollback()
        return response_body(400, str(error), None)


def response_body(statuscode, message, body):
    """This is response method"""
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


def trigger_logs_lambda(job_run_id):
    import os
    # split_character = "-"
    # split_list = os.environ['AWS_LAMBDA_FUNCTION_NAME'].split(split_character)
    # print("checking on ",split_list)
    lambda_client.invoke(
        FunctionName='fileops-Logs' + "-" + os.environ.get('Environment'),
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps({'function': os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                            "job_run_id": job_run_id,
                            "log_stream_name": os.environ['AWS_LAMBDA_LOG_STREAM_NAME']})
    )
