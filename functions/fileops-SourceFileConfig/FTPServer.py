import boto3
import uuid
from ftplib import FTP, error_perm
from dbconnection import cursor, conn
def get_file_ftp_server(body,job_id):
    try:
        source_file_details = body.get("source_files", {})
        ftp_host = source_file_details.get('ip_host')
        ftp_user = source_file_details.get('user_name')
        ftp_pass = source_file_details.get('password')
        remote_file_path = source_file_details.get('location_path')
        last_substring = remote_file_path.rsplit("/", 1)[-1]
        # Connect to the FTP server
        ftp = FTP(ftp_host)
        # ftp.set_pasv(False)
        ftp.login(user=ftp_user, passwd=ftp_pass)
        folder = "sample-files/" + str(job_id) + "/" + last_substring
        ftp_file_data = bytearray()
        ftp.retrbinary('RETR ' + remote_file_path, ftp_file_data.extend)
        create_s3_folder("file-ops-tool-dev", folder, bytes(ftp_file_data))
        print('File downloaded successfully.')
        # Close the FTP connection
        ftp.quit()

    except error_perm as e:
        # Handle specific FTP error exceptions
        print(f"FTP error: {str(e)}")
        raise error_perm(e)
    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred: {str(e)}")
        raise Exception(e)


def create_s3_folder(bucket_name, folder_name, file):
    # Create an S3 client
    s3_client = boto3.client('s3')

    # Specify the folder name as the object key
    folder_key = f'{folder_name}'

    # Create an empty object with the folder key
    s3_client.put_object(Body=file, Bucket=bucket_name, Key=folder_key)

    print(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")



