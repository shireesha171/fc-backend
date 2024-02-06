"""This module is for authenting the user from the database"""
import json
from dbconnection import cursor



def lambda_handler(event, context):
    """This method is for handling the authentication a user"""
    print("Received Event :", str(event))
    params_dict = json.loads(event['body'])
    email = params_dict['email']
    password = params_dict['password']
    try:
        get_query = """
                        SELECT users.first_name, users.uuid, users.last_login, roles.id, roles.role_name
                        FROM users as users
                        INNER JOIN roles as roles ON users.role_id = roles.id
                        WHERE users.email =  %s AND users.password =  %s;
                    """
        cursor.execute(get_query, (email, password))
        user_data = cursor.fetchone()
        if user_data is None:
            return response(200,'Invalid email or password', None)
        else:
            user_dict = {
            'first_name': user_data[0],
            'uuid': user_data[1],
            'last_login': user_data[2].strftime("%m/%d/%Y, %H:%M:%S"),
            'role_id': user_data[3],
            "role_name": user_data[4]
            }
            return response(200,'LoggedIn Successfully', (user_dict))
    except Exception as error:
        print(error)
        return response(401, str(error), None)


def response(stautsCode, message, data):
    """This is a response method"""
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
