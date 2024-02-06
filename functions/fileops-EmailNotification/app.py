
import boto3
import json
from jinja2 import Environment, FileSystemLoader
import os

region = os.environ.get('Region')

def lambda_handler(event,context):
    print("Received Event: ", str(event))
    message = event['Records'][0]['Sns']['Message']
    message = json.loads(message)
    sns_data = message['data']
    sender_email = os.environ.get('Sender_id')
    sender_cc_email = os.environ.get('Sender_cc_id')
    

    if sns_data['record_type'] == 'JOB_ONBOARD_EMAIL_NOTIFICATION':
        print("Job onboard email notification")
        email_body = prepare_templete(sns_data, 'job_onboard.html')
        #sender_email = "kantesh.dandi@blumetra.com"
        recipient_email = sns_data['email_recipient_list']
        subject = "Job onboarding success !"
        send_email_with_custom_template(sender_email, recipient_email, subject, email_body)
    elif sns_data['record_type'] == 'JOB_STATUS_EMAIL_NOTIFICATION':
        email_body = prepare_templete(sns_data, 'job_status.html')
        print("Job status email notification")
        #sender_email = "kantesh.dandi@blumetra.com"
        recipient_email = sns_data['email_recipient_list']
        subject = "Job status has been changed !"
        send_email_with_custom_template(sender_email, recipient_email, subject, email_body)
    elif sns_data['record_type'] == 'JIRA_NOTIFICATION':
        print("Jira notification")
        data = sns_data
    else:    
        data = {
            "name": "file_ops",
            "job_id": sns_data["job_id"],
            "job_run_id": sns_data['job_run_id'],
            "log_level": "warn",
            "error_code_id": json.dumps(sns_data['error_code_id'])
        }
        if sns_data['mailing_list'] is not None:
            data = json.dumps(data)
            send_email(source=sender_cc_email, recipients=sns_data['mailing_list'],
                                                template_name="file_ops_email_template", template_data=data)




def send_email(source,recipients, template_name, template_data):
    # Create a new SES resource
    client = boto3.client('ses', region_name=region)

    response = client.send_templated_email(
        Source=source,
        Destination={
            'ToAddresses': recipients,
        },
        Template=template_name,
        TemplateData=template_data
    )
    print("email message:", response['MessageId'])


def send_email_with_custom_template (sender_email, recipient_email, subject, email_body):
    ses = boto3.client('ses', region_name=region)
    try:
        response = ses.send_email(
            Source = sender_email,
            Destination ={'ToAddresses': recipient_email},
            Message = {
                'Subject': {'Data': subject},
                'Body': {'Html': {'Data': email_body}}
            }
        )
        print("Email sent! Message ID: {}".format(response['MessageId']))
    except Exception as e:
        print("Exception Occured :", e)

def prepare_templete(sns_data, html_template):
    # Create a Jinja2 environment
    env = Environment(loader=FileSystemLoader('./'))
    template = env.get_template(html_template)

    # Render the template with user data
    rendered_template = template.render(sns_data=sns_data)
    return rendered_template