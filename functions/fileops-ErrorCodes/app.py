"""This module is for Error Codes"""
import json
from dbconnection import cursor


def lambda_handler(event, context):
    print("Received Event: ", str(event))
    """This method is for handling the error codes"""
    print(event)

    if event['resource'] == "/error-codes" and event['httpMethod'] == 'GET':
        return getAllErrorCodes()
    else:
        return response(401, 'resource or endpoint not found', None)


def response(stautsCode, message, data):
    """This is response method"""
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

def getAllErrorCodes():
    """This method is for getting the error codes"""
    query = "select uuid, errorcode, error_desc from status_codes"
    cursor.execute(query)
    rows = cursor.fetchall()
    error_code_list = []
    for row in rows:
        data = {}
        data['uuid']  = row[0]
        data['errorcode']  = row[1]
        data['error_desc']  = row[2]
        data['severity']  = 1
        data['impact']  = 1
        data['priority']  = 1
        error_code_list.append(data)
    return response(200,'Successfully fetched', error_code_list)


