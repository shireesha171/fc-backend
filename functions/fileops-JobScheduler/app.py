import json
import boto3
from datetime import datetime, timedelta
import os
from dbconnection import cursor, conn
import uuid

scheduler_client = boto3.client('scheduler')
from urllib.parse import urlparse

# import psycopg2

# these values come from the EventBridge schedule itself

account = os.environ.get('Account')
env = os.environ.get('Environment')
region = os.environ.get('Region')

target_arn = f'arn:aws:lambda:{region}:{account}:function:fileops-ValidationProcess-{env}'
role_arn = f'arn:aws:iam::{account}:role/FileOps_Role-{env}'


def prepare_cron_expression(payload):
    schedule_json = payload.get('schedule_json')
    print(schedule_json)
    if schedule_json.get('is_cron_job') == 'false' or schedule_json.get('is_cron_job') == 'False':

        if schedule_json.get('job_schedule') == 'Hourly' or schedule_json.get('job_schedule') == 'hourly':

            hours = int(schedule_json.get('run_every'))
            minuts = int(schedule_json.get('start_at'))

            if minuts != 0 and hours != 0:
                cron_expression = f'cron({minuts} */{hours} * * ? *)'
            elif hours != 0:
                cron_expression = f'cron(* */{hours} * * ? *)'
            elif minuts != 0:
                cron_expression = f'cron(*/{minuts} * * * ? *)'
            else:
                cron_expression = f'cron(0 0 * * ? *)'

            # date_for_s3_path = datetime.now().strftime('%d-%m-%Y')

        elif schedule_json.get('job_schedule') == 'daily':
            hours = schedule_json.get('daily_time')
            target_time = datetime.strptime(hours, "%H:%M:%S")

            cron_expression = f"cron({target_time.minute} {target_time.hour} * * ? *)"


        elif schedule_json.get('job_schedule') == 'weekly':

            week_days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
            days = schedule_json.get('days')
            hours = schedule_json.get('weekly_time')
            # days_of_week = [week_days.index(day) for day in days if day in week_days]

            specific_time = datetime.strptime(hours, "%H:%M:%S")

            cron_expression = f"cron({specific_time.minute} {specific_time.hour} ? *"
            cron_expression += " " + ",".join(map(str, days)) + " *)"

        elif schedule_json.get('job_schedule') == 'monthly':

            hours = schedule_json.get('monthly_time')
            day_of_month = int(schedule_json.get('monthly_day'))
            day = day_of_month - 1
            specific_time = datetime.strptime(hours, "%H:%M:%S")
            cron_expression = f"cron({specific_time.minute} {specific_time.hour} {day_of_month} * ? *)"

    else:
        cron = schedule_json.get('cron_expression')
        cron_expression = f"cron({cron})"
    return cron_expression


def insert_into_business_user_process(payload, conn, cursor):
    file_name = payload['file_name_pattern']
    file_uploaded_url = payload['location_pattern']

    query_job_id = "select id from jobs where uuid = %s"
    cursor.execute(query_job_id, (payload['job_id'],))
    job_details = cursor.fetchone()

    if job_details is not None:
        job_id = job_details[0]
    else:
        job_id = 10

    query_job_id = "select id from business_process where uuid = %s"
    cursor.execute(query_job_id, (payload['business_process_id'],))
    business_process_fetch = cursor.fetchone()

    if business_process_fetch is not None:
        business_process_id = business_process_fetch[0]
    else:
        business_process_id = 10

    qurey = """
               INSERT INTO business_user_validate_process(uuid, business_process_id,file_name, file_location_path,job_id)
               VALUES(%s, %s, %s, %s,%s)
               RETURNING *
            """
    values = (str(uuid.uuid4()), business_process_id, file_name, file_uploaded_url, job_id)
    cursor.execute(qurey, values)
    conn.commit()
    inserted_row = cursor.fetchone()
    business_process_validate_id = inserted_row[1]
    return business_process_validate_id


def lambda_handler(event, context):
    print(event)

    try:

        sns_message = json.loads(event['Records'][0]['Sns']['Message'])

        print(sns_message)

        payload = sns_message['data']

        print(payload)

        cron_expression = prepare_cron_expression(payload)

        pattern = payload['location_pattern']

        s3_bucket = urlparse(pattern, allow_fragments=False).netloc

        # env = s3_bucket.split("-")[2]
        # conn, cursor = getConnection(env)
        business_process_validate_id = insert_into_business_user_process(payload, conn, cursor)

        if not cron_expression:
            raise Exception("please provide cron expression")

        lambda_event = {
            'queryStringParameters': {
                'business_validate_id': business_process_validate_id,
                'job_type': 'Scheduled',
                'location_pattern': payload['location_pattern'],
                'file_name_pattern': payload['file_name_pattern']
            }
        }
        response = scheduler_client.create_schedule(
            Description='test',
            FlexibleTimeWindow={

                'Mode': 'OFF'
            },
            Name=payload['job_id'],
            ScheduleExpression=f'{cron_expression}',
            ScheduleExpressionTimezone='Asia/Calcutta',
            State='ENABLED',
            Target={
                'Arn': f'arn:aws:lambda:{region}:{account}:function:fileops-ValidationProcess-{env}',

                'Input': json.dumps(lambda_event),

                'RetryPolicy': {
                    'MaximumEventAgeInSeconds': 123,
                    'MaximumRetryAttempts': 123
                },
                'RoleArn': role_arn,

            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Working!')
        }
    except Exception as error:
        print(error)
        return {
            'statusCode': 200,
            'body': json.dumps('not working!')
        }
