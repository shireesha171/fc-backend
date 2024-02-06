import boto3
#access_key = "AKIAW6XNZ6QKFG23D3C7"
#secret_key = "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"
import json
import datetime
import os
account = os.environ.get('Account')
region = os.environ.get('Region')
def snsnotification(job):
    sns = boto3.client('sns', region_name=region)
    payload = {
        'message': 'Sending payload to SNS from Validation ',
        'data': {
            "job_id": job['job_id'],
            "job_run_id": job['job_run_id'],
            "log_level": job['log_level'],
            "error_code_id": job['error_code_id']
        }
    }
    response = sns.publish(
        TopicArn=f'arn:aws:sns:{region}:{account}:fileops-jira-client',
        Message=json.dumps(payload)
    )
    print(response)

def get_current_datatime():
  message = "Current datetime: " + str(datetime.datetime.now())
  return message
