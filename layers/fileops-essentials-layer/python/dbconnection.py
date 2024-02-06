import boto3
import psycopg2
import json
import os
region = os.environ.get('Region')
def get_secret():
    env = os.environ.get('Environment')
    
    #env = "dev"

    print("this is the env form template",env)
    secret_name = f"fileops-postgres-{env}"
    region_name = region
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    return secret_string

secret_string = get_secret()
secret_dict = json.loads(secret_string)

conn = psycopg2.connect(
     host = secret_dict['host'],
     database = secret_dict['dbname'],
     user = secret_dict['username'],
     password = secret_dict['password']
 )
cursor = conn.cursor()

# import psycopg2
#
# conn = psycopg2.connect(
#
#     host="fileopstool-dev.cizpyhadeg5w.us-east-2.rds.amazonaws.com",
#
#     database="fileops_dev",
#
#     user="postgres_dev",
#
#     password="Blumetra123"
#
# )
#
# cursor = conn.cursor()