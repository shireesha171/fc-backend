import boto3
import os
import csv
import psycopg2
from dbconnection import cursor, conn
import chardet
from urllib import parse

env = os.environ.get('Environment')
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'

# Initialize the ECS client
ecs = boto3.client('ecs', region_name='us-east-1')

cluster_name = os.environ.get('cluster_name')
account = os.environ.get('Account')

task_definition_arn = f'arn:aws:ecs:us-east-1:{account}:task-definition/FileOps-data-profilling:5'

# Define the container name if you want to override environment variables
container_name = os.environ.get('container_name') or 'Task-data-profile'


def lambda_handler(event, ctx):
    print("The Event", event)
    value = parse.unquote_plus(event['Records'][0]["s3"]["object"]["key"])
    bucket_name = event['Records'][0]["s3"]["bucket"]["name"]
    s3_filepath = bucket_name + '__init__' + value
    job_id = value.split('/')[1]
    print("job_id: ", job_id)

    # Here calling this method for saving the delimiter file.
    SaveDelimiter(value, job_id)

    environment_variables = [
        {
            'name': 'file_path',
            'value': s3_filepath
        },

    ]
    task_overrides = {
        'containerOverrides': [
            {
                'name': container_name,
                "command": ["python", "main.py"],
                'environment': environment_variables
            }
        ]
    }

    network_config = {
        "awsvpcConfiguration": {
            "subnets": ['subnet-090893965aa1e157c'],
            "securityGroups": ['sg-08551f70d8d914843'],
            'assignPublicIp': 'ENABLED'
        },
    }
    response = ecs.run_task(
        cluster=cluster_name,
        taskDefinition=task_definition_arn,
        overrides=task_overrides,
        networkConfiguration=network_config,
        launchType='FARGATE',
        count=1  # Specify the number of tasks to run
    )
    print("ECS task started successfully:", response)


def getDelimiter(file_key):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = getFileFromS3(file_key)

        sniffer = csv.Sniffer()
        sample = file_content[1:4096]
        delimiter = sniffer.sniff(sample, delimiters=[',', ';', ':', '|']).delimiter
        return delimiter
    except csv.Error:
        return ','


def getFileFromS3(file_key):
    """This method is for getting the file from s3"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(
            Bucket=s3_bucket,
            Key=file_key,
        )
        object_content = response['Body'].read()
        try:
            detected_encoding = chardet.detect(object_content)['encoding']
            decoded_file_content = object_content.decode(detected_encoding)
        except UnicodeDecodeError:
            fallback_encoding = 'utf-8'
            decoded_file_content = object_content.decode(fallback_encoding)

        return decoded_file_content
    except Exception as error:
        print("The error at GetFileFromS3 method", error)
        raise error


def SaveDelimiter(file_key, job_id):
    """This method is for saving the delimiter in database"""
    try:
        delimiter = getDelimiter(file_key)
        print('delimiter: ', delimiter)
        update_query = f"""
                            UPDATE jobs
                            SET delimiter = %s
                            WHERE uuid = %s
                          """
        cursor.execute(update_query, (delimiter, job_id))
        conn.commit()
    except psycopg2.DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise e
    except Exception as error:
        conn.rollback()
        print("The Error at SaveDelimiter method: ", error)
        raise error
