"""This module is for analysing the schema"""
import json
from dbconnection import cursor, conn


def lambda_handler(event, context):
    """This method is for handling the analyse schema functionality"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job/analyze-schema" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return UpdateSchema(body)
            else:
                return response(401, 'please provide body', None)
        elif event['resource'] == "/job/analyze-schema" and event['httpMethod'] == 'GET':
            job_id = event["queryStringParameters"]["jobId"]
            return EditSchema(job_id)
        else:
            return response(401, 'resource or endpoint not found', None)
    except Exception as error:
        print(error)
        return response(401, str(error), None)


def response(stautsCode, message, data):
    """This is response handling method"""
    return {
        "statusCode": stautsCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods":"*",
            "Access-Control-Allow-Headers":"*",
            },
            "body":json.dumps({"message": message, "data":data})
    }


def UpdateSchema(payload):
    """This is updateSchema method"""
    try:
        schema = payload['schema']
        job_id = payload.get('job_id', None)
        if job_id is not None:
            query = """
                        UPDATE source_file_config  
                        SET metadata = %s 
                        FROM jobs as j
                        WHERE j.id  = job_id AND j.uuid = %s
                    """
            data = [(json.dumps(schema)), job_id]
            cursor.execute(query, data)
            conn.commit()
            return response(200, "Updated successfully", None)
        else:
            return response(400, "Provide valid job_id", None)
    except Exception as error:
        print(error)
        return response(401, str(error), None)


def EditSchema(job_id):
    """This is editSchema method"""
    data = {}
    try:
        query_job_id = "select id from jobs where uuid = %s"
        cursor.execute(query_job_id, (job_id,))
        job_details = cursor.fetchone()
        print(job_details)
        if job_details and len(job_details) > 0:
            query = "select job_id, metadata, uuid from source_file_config where job_id = %s"
            cursor.execute(query, (job_details[0],))
            rows = cursor.fetchone()
            if rows is not None:
                data['schema'] = rows[1]
                return response(200, 'Successfully fetched', (data))
            else:
                return response(200, f'file schema not found for job_id {job_id}', None)
        else:
            return response(200, f'unable to find job details with job_id {job_id}', (data))
    except Exception as err:
        print(err)
        return response(401, str(err), None)
