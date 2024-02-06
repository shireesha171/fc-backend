"""This module is for scheduling the jobs """
import json
from dbconnection import cursor, conn


def lambda_handler(event, context):
    """This handler is for creating and getting the source file configurations"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job/schedule" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                return createJobSchedule(body)
            else:
                return response_body(401, 'please provide body', None)
        elif event['resource'] == "/job/schedule" and event['httpMethod'] == 'GET':
            return getJobSchedule(event)
        else:
            return response_body(401, 'resource or endpoint not found', None)
    except Exception as error:
        print(error)
        return response_body(401, str(error), None)



def getJobSchedule(event):
    """This method is to get the job Schedule details"""
    try:
        query_params = event.get("queryStringParameters", None)
        if query_params is not None and query_params["job_id"] != "":
            get_job_schedule = "SELECT schedule_json FROM jobs where uuid = %s"
            cursor.execute(get_job_schedule, (query_params['job_id'],))
            data = cursor.fetchone()
            return response_body(200, "The Fetched data is ready", data)
        else:
            return response_body(404, "queryStringParameters not found", None)
    except Exception as error:
        print(error)
        return response_body(401, str(error), None)



def createJobSchedule(body):
    """This method is for handling the job scheduling"""
    try:
        schedule = {
          "job_id": body.get('job_id', None),
          "is_cron_job": body.get('is_cron_job', None),
          "job_schedule": body.get('job_schedule', None),
          "cron_expression": body.get('cron_expression', None),
          "run_every": body.get('run_every', None),
          "start_at": body.get('start_at', None),
          "daily_time": body.get('daily_time', None),
          "days": body.get('days', None),
          "monthly_day": body.get('monthly_day', None),
          "monthly_time": body.get('monthly_time', None),
          "weekly_time":body.get('weekly_time', None),
          "load_frequency": body.get('load_frequency', None)
        }
        schdule_json = json.dumps(schedule)
        sql = "UPDATE jobs  SET schedule_json = %s WHERE uuid = %s"
        data = (
          schdule_json,
          schedule['job_id'],
        )
        cursor.execute(sql, data)
        conn.commit()
        return response_body(200, message="Inserted Successfully", body=schedule)
    except Exception as error:
        print(error)
        return response_body(401, str(error), None)


def response_body(statuscode, message, body):
    """This is a response method"""
    response = {
        "statusCode": statuscode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods":"*",
            "Access-Control-Allow-Headers":"*",
            },
            "body":json.dumps({"message": message, "data":body})
    }
    return response
