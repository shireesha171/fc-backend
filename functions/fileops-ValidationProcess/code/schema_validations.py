"""This module is for validating process"""
import json
from datetime import datetime
from functools import reduce
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd

from .data_validations import mandatoryComparision
from .snspublisher import get_current_datatime
from .utils.loggers import update_schema_validations

logger = ''

import os
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def getSchemaValidations(business_validate_id,record,logger1, query_params):
    """This method is for handling validation process"""
    global logger
    logger = logger1
    if record is not None and record['metadata'] is not None:
        metadata = record['metadata']
        metadata_df = pd.json_normalize(metadata)
        metadata_df.set_index('column', inplace=True)
    else:
        logger.error("metaData not found   ", + get_current_datatime())
        print("metaData not found")

    if 'job_type' in query_params and query_params['job_type'] == "Scheduled":
       print("inside schema validation")
       parsed_s3_path = urlparse(query_params['location_pattern'])
       bucket_name = parsed_s3_path.netloc
       object_key = parsed_s3_path.path.lstrip('/')
       folders = object_key
       folder_path = folders + '/' + query_params['file_name_pattern']
       date_prefix =folders.split("/")[1]
       if "{" in date_prefix and "}" in date_prefix:
            date_prefix = date_prefix.replace("{","").replace("}","")
       else:
           print("Date prefix error", date_prefix)
           
       print("folder_prefix", datetime.now().strftime(date_prefix))
        
       folder_path = f"patterns/"+datetime.now().strftime(date_prefix)+"/"+query_params['file_name_pattern']
    else:
       folder_path = 'sample-files/' + business_validate_id + "/"
       bucket_name = s3_bucket
    s3_object = getfilefromS3(folder_path, bucket_name)
    
    if s3_object is None:
        logger.error("No Sample file found neither format not support  " + get_current_datatime())
        return response_body(400, "", body="No Sample file found neither format not support")
  
    source_df = creatingDataframe(s3_object['file'], s3_object['Body'])
    data = actualData(source_df, metadata_df,record)
    data["mandatory_column_data"] = mandatoryComparision(source_df, metadata_df)
    # data["mandatory_columns"] = data_validations.checkingMandatoryColumns(source_df, metadata_df)

    update_schema_validations(data)

    return data


#  creating actual validation part
def actualData(source_df,metadata_df,record):
    """This method is for actual data"""
    dq_rules = record['dqrules']
    standard_validation = record['standard_validations']
    profile_column = source_df.columns
    metadata_column = metadata_df.index
    emptyvalues = source_df.isnull().sum()

    obj = {}
    obj['total_columns'] = forming_values(len(profile_column),None)
    logger.info("getting total column  " + get_current_datatime())
    #checking  additinal columns
    additional_columns = list(set(profile_column) - set(metadata_column))
    obj["additional_columns"] =  forming_values(additional_columns,"Failure"if len(additional_columns) > 0 else "Passed")
    # obj["additional_columns"] = list(set(profile_column) - set(metadata_column))
    logger.info("validating additional columns  " + get_current_datatime())
    # checking missing columns
    missing_columns = list(set(metadata_column) - set(profile_column))
    obj["missing_columns"] = forming_values(missing_columns,"Failure"if len(additional_columns) > 0 else "Passed")
    logger.info("validating missing columns  " + get_current_datatime())
    # checking missing content
    missing_content = len(list(filter(lambda x: x > 0, emptyvalues)))
    obj["missing_content"] = forming_values(missing_content,"Failure"if missing_content > 0 else "Passed")
    logger.info("validating missing_content  " + get_current_datatime())
    # checking total null count
    total_null_count = reduce(lambda x, y: x + y, emptyvalues)
    obj["total_null_count"] = forming_values(total_null_count,"Failure"if total_null_count > 0 else "Passed")
    logger.info("validating total_null_count  " + get_current_datatime())
    # checking duplicate_records_by_row
    duplicate_records_by_row = check_duplicate_records(source_df,standard_validation)
    obj["duplicate_records_by_row"] = forming_values(duplicate_records_by_row,"Failure"if duplicate_records_by_row > 0 else "Passed")
    logger.info("validating duplicate_records_by_row  " + get_current_datatime())
    data_check = data_type_checks(source_df,metadata_df)
    obj['data_check_passed'] = forming_values(data_check.get('Passed'),"Passed" if len(data_check.get('Passed')) > 0 else "Failure")
    logger.info("validating data_check_passed  " + get_current_datatime())
    obj['data_check_failure'] = forming_values(data_check.get('failed'),"Failure" if len(data_check.get('failed')) > 0 else "Passed")
    logger.info("validating data_check_failure  " + get_current_datatime())
    range_rows = range_check_main(source_df,dq_rules,metadata_df)
    obj['range_rows'] = forming_values(range_rows,"Failure"if len(range_rows) > 0 else "Passed")
    logger.info("validating range_rows  " + get_current_datatime())
    list_value_rows = list_value_check_main(source_df, dq_rules, metadata_df)
    obj['list_value_rows'] =  forming_values(list_value_rows,"Failure"if len(list_value_rows) > 0 else "Passed")
    logger.info("validating list_value_rows  " + get_current_datatime())
    Number_of_range_mismatch = [{"column_name": item['column_name'], "count": len(item['data'])} for item in obj["range_rows"]['value']]
    range_mismatch_status_type = (x for x in Number_of_range_mismatch if x['count'] > 0)
    range_count = next(range_mismatch_status_type, None)
    obj['Number_of_range_mismatch'] = forming_values(Number_of_range_mismatch,"Failure" if range_count is not None else "Passed")
    logger.info("validating Number_of_range_mismatch  " + get_current_datatime())
    number_of_list_value_error = [{"column_name": item['column_name'], "count": len(item['data'])} for item in obj["list_value_rows"]['value']]
    number_of_list_value_status_type = (x for x in number_of_list_value_error if x['count'] > 0)
    list_count = next(number_of_list_value_status_type, None)
    obj['number_of_list_value_error'] = forming_values(number_of_list_value_error,"Failure" if list_count is not None else "Passed")
    logger.info("validating number_of_list_value_error " + get_current_datatime())
    obj['number_of_range_mismatch_success'] = forming_values(len([item for item in obj['Number_of_range_mismatch']['value'] if item['count'] == 0]),None)
    logger.info("validating number_of_range_mismatch_success  " + get_current_datatime())
    obj['number_of_list_value_success'] = forming_values(len([item for item in obj['number_of_list_value_error']['value'] if item['count'] == 0]),None)
    logger.info("validating number_of_list_value_success  " + get_current_datatime())
    obj['range_mismatch_count'] = forming_values( sum(data_dict.get('count',0) for data_dict in obj['Number_of_range_mismatch']['value']),None)
    logger.info("validating range_mismatch_count  " + get_current_datatime())
    obj['list_value_count'] = forming_values(sum(data_dict.get('count',0) for data_dict in obj['number_of_list_value_error']['value']),None)
    logger.info("validating list_value_count  " + get_current_datatime())
    null_rows = getting_null_rows(source_df)
    obj['null_rows'] = forming_values(null_rows,"Failure" if len(null_rows) > 0 else "Passed")
    logger.info("validating null_rows  " + get_current_datatime())
    return obj

def creatingDataframe(key,body):
    """This method is creatingDataframe"""
    if key.endswith("csv"):
        return pd.read_csv(body)
    elif key.endswith("xlsx"):
        return pd.read_excel(body)
    elif key.endswith(".json"):
        return pd.read_json(body)
    else:
        return None


def getfilefromS3(folder_path, bucket_name):
    """This method is getting file from S3"""
    try:
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



def readings3file(body, file):
    """This method is for reading file from S3"""
    if file == 'csv':
        return pd.read_csv(body)
    elif file == 'xlsx':
        return pd.read_excel(body)
    elif file == 'json':
        return pd.read_json(body)
    return None


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


def data_type_checks(source_df, metadata_df):
  meta_columns = metadata_df.index
  condition = metadata_df['dtype'] == ("string")
  metadata_df.loc[condition, 'dtype'] = 'object'
  check_obj = {}
  check_obj['Passed'] = []
  check_obj['failed'] = []
  for item in meta_columns:
    if item in source_df.columns and (metadata_df.loc[item]['dtype'] == source_df[item].dtype):
      check_obj['Passed'].append(item)
    else:
      check_obj['failed'].append(item)
  return check_obj


def range_check_fun(source_df, range_check, column):
  return source_df[(source_df[column] < range_check[0]) | (source_df[column] > range_check[1])]


def list_value_check_fun(source_df, list_value, column):
  return source_df[~source_df[column].isin(list_value)]


def list_value_check_main(source_df, dq_rules, metadata_df):
    if dq_rules is not None:
      dqrules = dq_rules
      keys = list(dqrules.keys())
      column_mismatch = []
      for item in keys:
        if item in source_df.columns and item in metadata_df.index:
          if metadata_df.loc[item]['dtype'] == "object":
            data = list_value_check_fun(source_df, dq_rules.get(item)['list_of_value_check'], item)
            obj = {"column_name": item, "data": data.to_dict(orient='records')}
            column_mismatch.append(obj)
      return column_mismatch

    else:
      return []


def range_check_main(source_df, dq_rules, metadata_df):
    if dq_rules is not None:
      dqrules = dq_rules
      keys = list(dqrules.keys())
      column_mismatch = []
      for item in keys:
        if item in source_df.columns and item in metadata_df.index:
          if metadata_df.loc[item]['dtype'] == "int64":
            data = range_check_fun(source_df, dq_rules.get(item)['range_check'], item)
            obj = {"column_name": item, "data": data.to_dict(orient='records')}
            column_mismatch.append(obj)
      return column_mismatch
    else:
      return []


def check_duplicate_records(source_df,standard_validation):
      column_list = source_df.columns.tolist()
      if "ignore_columns" in standard_validation :
          column_list = set(column_list) - set(standard_validation['ignore_columns'])
      duplicate_rows = source_df[source_df.duplicated(subset=column_list, keep="first")]
      return len(duplicate_rows)

def getting_null_rows(source_df):
    null_columns_data = []
    null_rows = source_df.loc[:, source_df.isnull().any()]
    for item in null_rows.columns:
      df = source_df.loc[source_df[item].isnull(), :]
      df_filled = df.replace({np.nan: None})
      null_columns_data.append({"column_name": item, "data": df_filled.to_dict(orient='records')})
    return null_columns_data

def forming_values(value,status_type):
    if isinstance(value, list):
        value = tuple(value)
    return {"value": value, "status_type": status_type}