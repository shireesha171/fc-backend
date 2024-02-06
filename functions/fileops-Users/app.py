"""This module is for users module"""
import json
from dbconnection import cursor, conn
import psycopg2
import boto3
import  requests
import os
env = os.environ.get('Environment') or 'dev'
import botocore.exceptions

def lambda_handler(event, context):
    """This method is for handling users"""
    print("Incoming Event", event)
    try:
        if event['resource'] == "/users" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return updateUser(body)
            else:
                return response(400, "INVALID_PAYLOAD", None)

        elif event['resource'] == "/users" and event['httpMethod'] == 'GET':
            query_params = event.get('queryStringParameters', None)
            if query_params is not None and query_params.get('offset', None) is not None:
                return getUserList(query_params)
            elif query_params is not None and query_params.get('user_id', None) is not None:
                return getUserRole(query_params)
            else:
                print("QUERY_PARAMS_NOT_FOUND", query_params)
                return response(400, "QUERY_PARAMS_NOT_FOUND", None)

        else:
            print("INVALID_METHOD")
            return response(404, "INVALID_METHOD", None)
    except Exception as error:
        print("Users API failed", str(error))
        return response(500, "Users API failed", None)


def getUserList(params):
    """This method is for the list of users"""
    try:
        recordsperpage = params['recordsperpage']
        offset = params['offset']
        pagination_query = f"offset {offset} ROWS FETCH next {recordsperpage} ROWS ONLY"
        get_users_list = f"""
                            SELECT uuid, email, first_name, last_name, assigned_groups, role_id, role_status, created_from, count(*) OVER() AS full_count
                            FROM users
                            ORDER BY id DESC {pagination_query}
        """
        cursor.execute(get_users_list)
        conn.commit()
        data = cursor.fetchall()
        list_of_users = []
        for row in data:
            user_dict = {}
            user_dict = {
                'user_id': row[0],
                'email': row[1],
                'first_name': row[2],
                'last_name': str(row[3]),
                'assigned_groups': row[4],
                'role_id': row[5],
                'role_status': row[6],
                'created_from': row[7],
                'full_count': row[8]
            }
            list_of_users.append(user_dict)
        print("GET_USERS_LIST_SUCCESS", list_of_users)
        return response(200, "GET_USERS_LIST_SUCCESS", list_of_users)

    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("GET_USERS_LIST_FAILED", str(error))
        return response(500, "GET_USERS_LIST_FAILED", str(error))
    except Exception as error:
        conn.rollback()
        print("GET_USERS_LIST_FAILED", str(error))
        return response(500, "GET_USERS_LIST_FAILED", str(error))
    

def updateUser(payload):
    """This method is for updating the user role/group"""
    try:
        update_user = """
                        UPDATE users 
                        SET role_id = %s, role_status = %s,
                            assigned_groups = %s
                        WHERE uuid = %s
        """
        values = (
            payload.get('role_id'),
            payload.get('role_status'),
            json.dumps(payload.get('groups_list', {})),
            payload.get('user_id')
        )

        if(payload.get('email')):
            cursor.execute(update_user, values)
            affected_rows = cursor.rowcount  # Get the number of affected rows

            if affected_rows == 0:
                raise ValueError("No records found to update")
            conn.commit()

            role_id_mapping = {
                "0": "Admin",
                "1": "Data_Engineer",
                "2": "Business_User",
                0: "Admin",
                1: "Data_Engineer",
                2: "Business_User"
            }

            role_name = role_id_mapping.get(payload.get('role_id'))
            user_email = payload.get('email')

            try:
                client = boto3.client('cognito-idp', region_name=os.environ.get('Region'))
                user_pool_id = os.environ.get('user_pool_id')

                # Specify the updated attributes
                updated_attributes = [{
                    'Name': 'custom:userType',
                    'Value': role_name
                }]

                # Update user attributes in cognito pool
                client.admin_update_user_attributes(
                    UserPoolId=user_pool_id,
                    Username=user_email,
                    UserAttributes=updated_attributes
                )
                # Perform a global sign-out for the user
                client.admin_user_global_sign_out(
                    UserPoolId=user_pool_id,
                    Username=user_email
                    )
                
                print(f"User attributes updated successfully to cognito: {updated_attributes}")

            except botocore.exceptions.ClientError as e:
                # Check if the exception is a UserNotFoundException
                if e.response['Error']['Code'] == 'UserNotFoundException':
                    try:
                        okta_email = 'okta_' + user_email
                        get_response = client.admin_get_user(
                                UserPoolId=user_pool_id,
                                Username=okta_email)
                        if(get_response['UserAttributes']):

                            token = get_secret(env)
                            token_dict = json.loads(token)
                            domain = os.environ.get('okta_domain')
                            
                            headers = {
                                "Accept": "application/json",
                                "Content-Type": "application/json",
                                "Authorization": f"SSWS {token_dict['api_token']}"
                            }
                            body =  {
                                "profile": {
                                    "userType": role_name
                                }
                            }
                            url = f"https://{domain}/api/v1/users/{user_email}"
                            okta_res = requests.post(url=url, headers=headers, data=json.dumps(body))

                            # Perform a global sign-out for the user
                            if(okta_res.status_code == 200):
                                client.admin_user_global_sign_out(
                                    UserPoolId=user_pool_id,
                                    Username=okta_email
                                    )
                                
                                print(f"User attributes updated successfully to okta: {body}")

                    except botocore.exceptions.ClientError as e:
                        # Check if the exception is a UserNotFoundException
                        if e.response['Error']['Code'] == 'UserNotFoundException':
                            try:
                                okta_email = 'okta-provider_' + user_email
                                get_response = client.admin_get_user(
                                        UserPoolId=user_pool_id,
                                        Username=okta_email)
                                if(get_response['UserAttributes']):

                                    token = get_secret(env)
                                    token_dict = json.loads(token)
                                    domain = os.environ.get('okta_domain')
                                    
                                    headers = {
                                        "Accept": "application/json",
                                        "Content-Type": "application/json",
                                        "Authorization": f"SSWS {token_dict['api_token']}"
                                    }
                                    body =  {
                                        "profile": {
                                            "userType": role_name
                                        }
                                    }
                                    url = f"https://{domain}/api/v1/users/{user_email}"
                                    okta_res = requests.post(url=url, headers=headers, data=json.dumps(body))

                                    # Perform a global sign-out for the user
                                    if(okta_res.status_code == 200):
                                        client.admin_user_global_sign_out(
                                            UserPoolId=user_pool_id,
                                            Username=okta_email
                                            )
                                        
                                        print(f"User attributes updated successfully to okta: {body}")
                            except Exception as error:
                                print("UPDATE_USER_FAILED", str(error))
                                return response(500, "UPDATE_USER_FAILED", str(error))
                            
                        else:
                            print("UPDATE_USER_FAILED", str(e))
                            return response(500, "UPDATE_USER_FAILED", str(e))

                else:
                    print("UPDATE_USER_FAILED", str(e))
                    return response(500, "UPDATE_USER_FAILED", str(e))

            print("User details updated successfully")
            return response(200, "User details updated successfully", None)
        else:
            return response(400, "email is missing in payload", None)


    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("UPDATE_USER_FAILED", str(error))
        return response(500, str(error), None)
    except Exception as error:
        print("UPDATE_USER_FAILED", str(error))
        return response(500, str(error), None)


def get_secret(env):

    print("this is the env form template",env)
    secret_name = f"filecentral-okta-{env}"
    region_name = os.environ.get('Region')
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    return secret_string

def getUserRole(params):
    """This method is to get the user role"""
    try:
        user_id = params['user_id']
        get_user_role = "SELECT role_id, role_status, assigned_groups FROM users WHERE uuid = %s"
        cursor.execute(get_user_role, (user_id,))
        conn.commit()
        user_data = cursor.fetchone()
        if user_data is None:
            response(404, "provide valid user_id", None)
        else:
            response_data = {
                "user_id": user_id,
                "role_id": user_data[0],
                "role_status": user_data[1],
                "assigned_groups": user_data[2]
            }

        print("GET_USER_ROLE_SUCCESS", response_data)
        return response(200, "GET_USER_ROLE_SUCCESS", response_data)

    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("GET_USER_ROLE_FAILED", str(error))
        return response(500, "GET_USER_ROLE_FAILED", str(error))
    except Exception as error:
        conn.rollback()
        print("GET_USER_ROLE_FAILED", str(error))
        return response(500, "GET_USER_ROLE_FAILED", str(error))

def response(status_code, message, data):
    """This is response structure method"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": data})
    }


# lambda_handler(event, "")