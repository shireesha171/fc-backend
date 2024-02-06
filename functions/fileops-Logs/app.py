import boto3

# Initialize AWS SDK
client = boto3.client('logs')
from dbconnection import conn, cursor
import psycopg2
import json

# Get log streams
def lambda_handler(event,context):

    print("Received Event: ", str(event))
    if 'queryStringParameters' in event and 'job_run_id' in event['queryStringParameters']:
        query_string_params = event['queryStringParameters']
        job_run_id = query_string_params['job_run_id']
        return get_logs(job_run_id)

    else:
      storing_logs(event)





def storing_logs(event):
  log_group_name = '/aws/lambda/' + event['function']
  job_run_id = event['job_run_id']
  log_stream_name = event['log_stream_name']
  print("job-run-id----------->",job_run_id)
  print("log_group_name----------->", log_group_name)
  print("log_stream_name----------->", log_stream_name)
  # response = client.describe_log_streams(
  #   logGroupName=log_group_name,
  #   orderBy='LastEventTime',
  #   descending=True,
  #   limit=1
  # ),

  # Get the latest log stream
  log_stream_name = log_stream_name

  # Get log events
  response = client.get_log_events(
    logGroupName=log_group_name,
    logStreamName=log_stream_name
  )
  print(type(response['events']))
  try:
    update_query = """
        UPDATE job_runs
        SET logs = %s
        WHERE uuid = %s
    """
    values = (json.dumps(response['events']), job_run_id)
    cursor.execute(update_query, values)
    conn.commit()
  except psycopg2.Error as e:
    print("An error occurred:", e)
    conn.rollback()
    return str(e)


def get_logs(job_run_id):
  try:
      job_query = """
        select j.logs from job_runs as j where uuid = %s
         """
      values = (job_run_id,)
      cursor.execute(job_query,values)
      data = cursor.fetchone()
      return  response_body(200, "retrieve Successfully", data)
  except Exception as error:
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

