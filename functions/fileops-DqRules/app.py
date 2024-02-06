from dbconnection import cursor, conn
import json
import uuid
import psycopg2
from psycopg2 import IntegrityError


def response(stautsCode, message, data):
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


def lambda_handler(event, context):
    print("Received Event: ", str(event))
    if event['resource'] == "/job/dq-rules" and event['httpMethod'] == 'POST':
        return post_DQRules(event)

    elif event['resource'] == "/job/dq-rules" and event['httpMethod'] == 'GET':
        return get_DQRules(event)

    else:
        return response(401, 'resource or endpoint not found', None)


# This method is get the DQRules from Source_file_config table @ 'DQRules' column
def get_DQRules(event):

    print(event)
    query_params = event.get('queryStringParameters', None)
    if query_params is not None and "job_id" in query_params and query_params.get('job_id') !="":
        query_params = event['queryStringParameters']
        job_uuid = query_params['job_id']
        print("it is inside the if clause")

    else:
        return response(400, "job_id or job_id value is not found", None)

    try:

        select_query = """
                        SELECT sf.DQRules
                        FROM source_file_config sf
                        JOIN jobs j ON sf.job_id = j.id
                        WHERE j.uuid = %s
                       """

        cursor.execute(select_query, (job_uuid,))
        dqrules_list = cursor.fetchone()

        if(len(dqrules_list) > 0 and isinstance(dqrules_list[0],dict)):
            dqrules = dqrules_list[0]
            dqrules_response = []
            for key in dqrules.keys():
                value = dqrules[key]
                value['column_name'] = key
                dqrules_response.append(value)

            return response(200, 'Data retrieved successfully', dqrules_response)
        else:
            return response(200, f'No DQRules for this Job & Job_id: {job_uuid}', [])



    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(502, str(e), None)

    except Exception as error:
        print(error)
        return response(401, str(error), None)
    cursor.close()

# This method is to post the DQRules in Source_file_config table @ 'DQRules' column
def post_DQRules(event):
    body = json.loads(event['body'])
    dq_rules = body['dq_rules']
    column_name = body['column_name']
    job_uuid = body['job_id']
    dq_R_FDB = get_dq_rules(job_uuid)
    dq_rules_json = {}
    if dq_R_FDB is not None and dq_R_FDB[0] is not None:
        dq_rules_json = dq_R_FDB[0]
    dq_rules_json[column_name] = dq_rules
    dq_rules_json = json.dumps(dq_rules_json)
    try:
        query = "SELECT id FROM jobs WHERE uuid = %s"
        cursor.execute(query, (job_uuid,))
        job_integer_id = cursor.fetchone()[0]
        update_query = f"""
                UPDATE source_file_config
                SET dqrules = %s
                WHERE job_id = %s
                """
        values = (dq_rules_json,job_integer_id)
        cursor.execute(update_query,values)
        conn.commit()
        return response(200, 'Data updated successfully.', None)


    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(401, str(e), None)
        conn.rollback()

    except Exception as error:
        print(error)
        return response(401, str(error), None)
    cursor.close()

def get_dq_rules(job_uuid):
    try:
      select_query = """
                        SELECT sf.dqrules
                        FROM source_file_config sf
                        JOIN jobs j ON sf.job_id = j.id
                        WHERE j.uuid = %s
                       """

      cursor.execute(select_query, (job_uuid,))
      dqrules_list = cursor.fetchone()
      return dqrules_list

    except psycopg2.DatabaseError as e:
      print(f"Database error: {e}")
      return response(502, str(e), None)

    except Exception as error:
      print(error)
      return response(401, str(error), None)
    cursor.close()
