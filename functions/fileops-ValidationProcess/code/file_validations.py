import copy
import math
import os
import re
from urllib.parse import urlparse
import boto3
import chardet
import pandas as pd

env = os.environ.get('Environment')
# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
from datetime import datetime

file_validation_data = {}
logger = ""
from .snspublisher import get_current_datatime
# JobRunLogger
from .utils.loggers import update_file_validations, update_file_metadata

# JobRunLogger - FileValidation Class


def fetchFileContent(business_validate_id, query_params):
    s3 = boto3.client('s3')
    bucket_name = s3_bucket
    folder_prefix = 'sample-files/' + business_validate_id + "/"
    if 'job_type' in query_params and query_params['job_type'] == 'Scheduled':
        # folder_prefix = query_params['location_pattern'] + "/"

        folder_prefix = urlparse(query_params['location_pattern'], allow_fragments=False).path + "/"
        folder_prefix = folder_prefix.lstrip('/')
        folder_prefix = folder_prefix + query_params['file_name_pattern']
        date_prefix = folder_prefix.split("/")[1]

        if "{" in date_prefix and "}" in date_prefix:
            date_prefix = date_prefix.replace("{", "").replace("}", "")
        else:
            print("Date prefix error", date_prefix)

        print("folder_prefix", datetime.now().strftime(date_prefix))

        folder_prefix = f"patterns/" + datetime.now().strftime(date_prefix) + "/" + query_params['file_name_pattern']
        print("folder_prefix after", folder_prefix)

    # Use the list_objects_v2 method to retrieve object names
    print("Folder Prefix for listing the filename", folder_prefix)
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_prefix
    )

    # Check if there is exactly one object in the specified folder
    if 'Contents' in response and len(response['Contents']) == 1:
        # Extract the object name (key) of the single object
        object_name = response['Contents'][0]['Key']
        print("Single object name in the specified folder:", object_name)
    else:
        print("There is not exactly one object in the specified folder or no objects found.")
        return None

    # Use the `os.path.basename` function to extract the file name
    file_name = os.path.basename(object_name)

    # JobRunLogger - Update File name
    update_file_metadata(file_name=file_name, file_path=object_name, bucket_name=bucket_name)

    local_file_name = "/tmp/" + file_name
    s3.download_file(bucket_name, object_name, local_file_name)
    return local_file_name


def get_file_validations(record, logger1, business_validate_id, query_params):
    global logger
    logger = logger1
    file_errors = [0]

    try:
        localfile = fetchFileContent(business_validate_id, query_params)
        if localfile is None:
            return file_errors, {"file_doesn't_exists": ""}
        # # calling get file encodings of the file
        get_file_encodings(record, file_errors, localfile)
        # calling get_file_size method
        get_file_size(record, file_errors, localfile)
        # calling get delimiter of the file
        get_delimiter_of_file(record, file_errors, localfile)
        # # calling get file extension method
        get_file_extension(record, file_errors, localfile)

        # Job Run Logger - Update File Validations
        update_file_validations(file_validation_data)

        return file_errors, file_validation_data
    except Exception as e:
        print(f"Error: {e}")
        raise e


def get_file_size(record, file_errors, localfile):
    """This method is for checking the file size """
    try:
        file_size = os.path.getsize(localfile)
        file_size_max_str = record['file_config_details']['file_size_max']
        file_size_min_str = record['file_config_details']['file_size_min']
        if file_size_min_str != "" and file_size_max_str != "":
            max_value, unit_of_data = extract_numerical_and_non_numerical_parts(file_size_max_str)
            min_value, unit_of_data = extract_numerical_and_non_numerical_parts(file_size_min_str)

            if unit_of_data == "Bytes":
                if min_value <= file_size <= max_value:
                    message = f"The file size is in range: [{min_value}Bytes, {max_value}Bytes] i.e {file_size} Bytes"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)

                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size) + "Bytes", "PASS", str(min_value) + ' Bytes', str(max_value) + ' Bytes'],
                        "Passed")
                else:
                    message = f"This file size is not in the range: [{min_value}Bytes, {max_value}Bytes]"
                    print(message)
                    logger.info(message + "  " + get_current_datatime())
                    file_errors[0] += 1
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size) + "Bytes", "FAIL", str(min_value) + ' Bytes', str(max_value) + ' Bytes'],
                        "Failure")

            elif unit_of_data == "KB":
                if min_value <= file_size / 1024 <= max_value:
                    message = f"The file size is in range: [{min_value}KB, {max_value}KB] i.e {math.ceil(file_size / (1024) * 100) / 100} KB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // 1024) + "KB", "PASS", str(min_value) + ' KB', str(max_value) + ' KB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range: [{min_value}KB, {max_value}KB]"
                    print(message)
                    logger.info(message + "  " + get_current_datatime())
                    file_errors[0] += 1
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // 1024) + "KB", "FAIL", str(min_value) + ' KB', str(max_value) + ' KB'],
                        "Failure")

            elif unit_of_data == "MB":
                if min_value <= file_size / (1024 ** 2) <= max_value:
                    message = f"The file size is in range: [{min_value}MB, {max_value}MB] i.e {math.ceil(file_size / (1024 ** 2) * 100) / 100} MB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // (1024 ** 2)) + "MB", "PASS", str(min_value) + ' MB', str(max_value) + ' MB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range{min_value} and {max_value}MB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // (1024 ** 2)) + "MB", "FAIL", str(min_value) + ' MB', str(max_value) + ' MB'],
                        "Failure")
                    file_errors[0] += 1

            elif unit_of_data == "GB":
                if min_value <= file_size / (1024 ** 3) <= max_value:
                    message = f"The file size is in range: [{min_value}GB, {max_value}GB] i.e {math.ceil(file_size / (1024 ** 3) * 100) / 100} GB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 3)) + "GB", "PASS", str(min_value) + ' GB', str(max_value) + ' GB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range{min_value} and {max_value}GB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 3)) + "GB", "FAIL", str(min_value) + ' GB', str(max_value) + ' GB'],
                        "Failure")
                    file_errors[0] += 1

            elif unit_of_data == "TB":
                if min_value <= file_size / (1024 ** 4) <= max_value:
                    message = f"The file size is in range: [{min_value}GB, {max_value}GB] i.e {math.ceil(file_size / (1024 ** 4) * 100) / 100} TB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 4)) + "TB", "PASS", str(min_value) + ' TB', str(max_value) + ' TB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range{min_value} and {max_value}TB"
                    logger.info(message + "  " + get_current_datatime())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 4)) + "TB", "FAIL", str(min_value) + ' TB', str(max_value) + ' TB'],
                        "Failure")
                    file_errors[0] += 1
    except Exception as e:
        logger.error(str(e) + "  " + get_current_datatime())
        print("Exception Found", e)
        raise e


def no_of_columns(localfile):
    """This method is to get the no of columns in a file"""
    try:
        df = pd.read_csv(localfile)

        header_row = df.columns.to_list()
        list_delims = [',', '|', ';', ':']

        new_cols = [column.split(delim) for column in header_row for delim in list_delims if
                    len(column.split(delim)) > 1]
        header_row = [column for column in header_row if all(delim not in column for delim in list_delims)]
        no_of_cols = len(list(set(header_row + [item for sublist in new_cols for item in sublist])))

        return no_of_cols

    except pd.errors.ParserError as error:
        # using pass here for ignoring the Error: like tokenizing data. C error: Expected 8 fields in line 3, saw 9
        pass
    except Exception as error:
        return error


def get_the_separators(line, separator_in_file, delimiter):
    """This method is for getting the possible separators of a given row"""
    try:
        possible_separators = [',', '|', ';', ':']
        possible_separators.remove(delimiter)

        for separator in possible_separators:
            if len(line.split(separator)) > 1:
                separator_in_file.append(separator)
        return separator_in_file

    except Exception as error:
        logger.error(str(error) + ' ' + get_current_datatime())
        return error


def check_delimiter_consistency(localfile, delimiter):
    """This method is for getting the list of actual delimiters present in the file and validating against the given delimiter"""
    try:
        df = pd.read_csv(localfile)

        with open(localfile, 'r') as file:
            response_list = []
            check = True
            first_line = file.readline()
            expected_fields = no_of_columns(localfile)

            # This is to check the file has the same delimiter(given by DE) or different one.
            if len(first_line.split(delimiter)) != 1:

                # Handling if header contains different delimiters(given and actual delimiter of the file is same).
                if 1 < len(first_line.split(delimiter)) < expected_fields:
                    copy_of_response_list = copy.deepcopy(response_list)
                    delimiter_in_header = get_the_separators(first_line, copy_of_response_list, delimiter)
                    response_list.insert(0, delimiter_in_header)
                    response_list.append(False)
                    check = False

                for line in file:
                    splitted_line = line.split(delimiter)

                    # Thia is to check all the rows has the same given delimiter or not.
                    if len(splitted_line) != expected_fields:
                        response_list_copy = copy.deepcopy(response_list)
                        actual_delimiters = get_the_separators(line, response_list_copy[0], delimiter) if len(
                            response_list_copy) > 0 else get_the_separators(line, response_list_copy, delimiter)

                        if len(response_list) > 0:
                            response_list[0] = actual_delimiters
                        else:
                            response_list.insert(0, actual_delimiters)

                        response_list.append(False) if False not in response_list else None
                        check = False
                if check:
                    response_list.append(True)
                return response_list

            # Here handling if file contains different dilimiter.
            else:
                response_list_copy = copy.deepcopy(response_list)
                actual_delimiter = get_the_separators(first_line, response_list_copy, delimiter)
                response_list.insert(0, actual_delimiter)
                response_list.append(False)
                return response_list

    except Exception as error:
        logger.error(str(error) + ' ' + get_current_datatime())
        return error


def get_delimiter_of_file(record, file_errors, localfile):
    """This method is for checking the file delimiter """
    try:
        if record['standard_validations'] and record['standard_validations']['delimiter'] == "Yes":
            delimiter_to_check = record['delimiter']
            delimiter_list = check_delimiter_consistency(localfile, delimiter_to_check)
            if delimiter_list[-1]:
                print(f"This file has a valid delimiter: {delimiter_to_check}")
                logger.info(f"This file has a valid delimiter: {delimiter_to_check}" + "  " + get_current_datatime())
                file_validation_data['file_delimiter'] = forming_values([[delimiter_to_check], "PASS"], "Passed")
            else:
                actual_delimiter_list = list(set(delimiter_list[0]))
                print("This file doesn't have a valid delimiter")
                logger.info("This file doesn't have a valid delimiter" + "  " + get_current_datatime())
                file_validation_data['file_delimiter'] = forming_values([actual_delimiter_list, "FAIL"], "Failure")
                file_errors[0] += 1
    except Exception as e:
        print(f"Error retrieving file delimiter from S3: {e}")
        logger.error(f"Error retrieving file delimiter from S3: {e}" + '   ' + get_current_datatime())
        raise e


def get_file_encodings(record, file_errors, localfile):
    """This method is for checking the file encoding """
    try:
        file_content_data = open(localfile, 'rb').read()
        result = chardet.detect(file_content_data)
        file_encoding = result['encoding']
        file_encoding = file_encoding.upper()
        print("file_encoding: ", file_encoding)
        if record['standard_validations'] and record['standard_validations']['encoding'] is not None and len(
                record['standard_validations']['encoding']) > 0:
            if record['standard_validations']['encoding'] == file_encoding:
                logger.info(f"This file has a valid file encoding: {file_encoding}")
                print(f"This file has a valid file encoding: {file_encoding}")
                file_validation_data['file_encoding'] = forming_values(
                    [file_encoding, "PASS", record['standard_validations']['encoding']], "Passed")
            else:
                print("This file doesn't have a valid file encoding")
                logger.info("This file doesn't have a valid file encoding")
                file_validation_data['file_encoding'] = forming_values(
                    [file_encoding, "FAIL", record['standard_validations']['encoding']], "Failure")
                file_errors[0] += 1
    except Exception as e:
        logger.error(f"Error in getting file encoding file from S3: {e}")
        print(f"Error in getting file encoding file from S3: {e}")
        raise e


def get_file_extension(record, file_errors, localfile):
    """This method is for checking the file file extension """
    try:
        split_tup = os.path.splitext(localfile)
        file_extension = split_tup[1].replace(".", "")
        file_extension = file_extension.upper()
        print("file_extension: ", file_extension)
        if record['file_config_details']['file_type'] == file_extension:
            logger.info(f"This file has a valid file extension: {file_extension}")
            print(f"This file has a valid file extension: {file_extension}")
            file_validation_data['file_extension'] = forming_values([file_extension, "PASS"], "Passed")
        else:
            print("This file doesn't have a valid file extension")
            logger.info("This file doesn't have a valid file extension")
            file_validation_data['file_extension'] = forming_values([file_extension, "FAIL"], "Failure")
            file_errors[0] += 1
    except Exception as e:
        logger.error(f"Error getting file extension from S3: {e}")
        print(f"Error getting file extension from S3: {e}")
        raise e


def extract_numerical_and_non_numerical_parts(word):
    """This method is for extracting the numerical and non-numerical part"""
    parts = re.split(r'(\d+)', word)
    size_value = parts[1] if len(parts) > 1 else None
    unit_of_data = parts[2] if len(parts) > 0 else None
    return int(size_value), unit_of_data


def forming_values(value, status_type):
    if isinstance(value, list):
        value = tuple(value)

    return {"value": value, "status_type": status_type}
