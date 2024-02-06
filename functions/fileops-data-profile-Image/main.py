import csv
import os
import urllib.parse as parse
from io import BytesIO

import boto3
import chardet
import pandas as pd
from ydata_profiling import ProfileReport

s3 = boto3.client('s3')


def decodeContent(object_content_bytes):
    """This method is for decoding the s3 file content with appropriate file encoding"""

    try:
        detected_encoding = chardet.detect(object_content_bytes)['encoding']
        decoded_file_content = object_content_bytes.decode(detected_encoding)
    except UnicodeDecodeError:
        fallback_encoding = 'utf-8'
        decoded_file_content = object_content_bytes.decode(fallback_encoding)
    return decoded_file_content


def getDelimiter(object_content_bytes):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = decodeContent(object_content_bytes)

        sniffer = csv.Sniffer()
        sample = file_content[:4096]
        file_delimiter = sniffer.sniff(sample, delimiters=[',', ';', ':', '|']).delimiter
        return file_delimiter
    except csv.Error:
        return ','


try:
    # Define the output file names
    html_file_name = 'y_data_profile.html'
    json_file_name = 'y_data_profile.json'

    if not os.environ['file_path']:
        raise FileNotFoundError("unable to get the path from airflow")

    filepath = os.environ['file_path']
    bucket_name = filepath.split('__init__')[0]
    filepath = filepath.split('__init__')[1]

    print("Bucket name: ", bucket_name)
    print("filepath: ", filepath)

    job_id = str()
    if filepath is not None:
        filepath = parse.unquote(filepath)
        job_id = filepath.split('/')[1]
    # Read the CSV file from S3
    response = s3.get_object(Bucket=bucket_name, Key=filepath)

    # Object Content is of type bytes
    object_content = response['Body'].read()

    # calling getDelimiter method to get the delimiter
    delimiter = getDelimiter(object_content)
    print("delimiter: ", delimiter)

    df = pd.read_csv(BytesIO(object_content), delimiter=delimiter)

    profile = ProfileReport(df, title="Profiling_Report")

    # Generate HTML report and save to S3
    html_output = profile.to_html()
    s3.put_object(Body=html_output, Bucket=bucket_name, Key=f"profiling/{job_id}/{html_file_name}",
                  ContentType='text/html')

    # Generate JSON report and save to S3
    json_output = profile.to_json()
    s3.put_object(Body=json_output, Bucket=bucket_name, Key=f"profiling/{job_id}/{json_file_name}",
                  ContentType='application/json')

except Exception as error:
    print("Error occurred", error)
