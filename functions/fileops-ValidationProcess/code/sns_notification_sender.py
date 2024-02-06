import boto3
import json
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
    print(response,'reponse')

def sns_notification_fun(errorList, record, job_id, mailing_list):
      """This method is for sns_notification_fun"""
      if len(errorList) > 0:
        data = {
          "job_id": job_id,
          "job_run_id": record[1],
          "log_level": "warn",
          "error_code_id": errorList,
          "mailing_list": mailing_list
        }
        snsnotification(data)
