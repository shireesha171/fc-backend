# # import ftplib
from ftplib import FTP, error_perm
# import uuid
# import boto3
#
import ftplib
import boto3
import io
import uuid
import json

import os
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
def lambda_handler(event, context):
    # Replace these with your S3 and FTP details
    print("Received Event: ", str(event))
    body = json.loads(event['body'])
    if body is None:
        return response(400, "body is empty", "")
    else:
        try:
            ftp_host = body.get('ip_host')
            ftp_user = body.get('user_name')
            ftp_pass = body.get('password')
            directory = body.get('location_path')
            job_id = body.get('job_id')
            actualpath = directory.split("/")
            s3_file_data = getfilefromS3(job_id)
            # Connect to the FTP server
            ftp = FTP_config(ftp_host=ftp_host, ftp_user=ftp_user, ftp_pass=ftp_pass)
            file_path = actualpath[-1]
            actualpath = actualpath[:-1]
            directory_creation(actualpath, ftp)
            print(ftp.pwd(), 'pwd')
            # # Upload the file to the FTP server
            with io.BytesIO(s3_file_data) as ftp_file_data:
              ftp.storbinary('STOR ' + file_path, ftp_file_data)
            # Close the FTP connection
            ftp.quit()
            return response(200, "Congratulations! Your file has been securely uploaded to the server.", "")
        except error_perm as e:
          # Handle specific FTP error exceptions
          print(f"FTP error: {str(e)}")
          return response(400, str(e), "")
        except Exception as e:
          # Handle other exceptions
          print(f"An error occurred: {str(e)}")
          return response(400, str(e), "")


def getfilefromS3(db_uuid):
  """This method is getting file from S3"""
  try:
    bucket_name = s3_bucket
    folder_path = 'sample-files/' + db_uuid + "/"
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    for obj in response['Contents']:
      if obj['Key'].endswith("csv"):
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
        return s3_object['Body'].read()
    return None
  except Exception as error:
    print("The Error:", str(error))
    return str(error)


def directory_creation(result_list, ftp):
  for index, item in enumerate(result_list):
    if not path_exists(ftp, item):
        ftp.mkd(item)
        ftp.cwd(item)


def path_exists(ftp, path):
  try:
    ftp.cwd(path)
    return True
  except ftplib.error_perm as e:
    return False


def FTP_config(ftp_host, ftp_user, ftp_pass):
  ftp = ftplib.FTP()
  ftp.connect(host=ftp_host)
  print("connected")
  # ftp.set_pasv(True)
  ftp.login(user=ftp_user, passwd=ftp_pass)
  print("login successfull")
  print(ftp.getwelcome())
  return ftp


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
