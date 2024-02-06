import psycopg2

from dbconnection import cursor, conn
import os
import boto3

def lambda_handler(event, context):
    print("Received Event: ", str(event))
    if (
            (
                    event['triggerSource'] == 'PostConfirmation_ConfirmSignUp' and
                    event['request']['userAttributes']['cognito:user_status'] == 'EXTERNAL_PROVIDER'
            # user who comes from okta
            )
            or
            (
                    event['triggerSource'] == 'PostConfirmation_ConfirmSignUp' and
                    event['request']['userAttributes']['cognito:user_status'] == 'CONFIRMED'
            # user who signed up them self using cognito
            )
            or
            (
                    event['triggerSource'] == 'PostAuthentication_Authentication' and
                    event['request']['userAttributes']['cognito:user_status'] == 'FORCE_CHANGE_PASSWORD'
            # user who is created by admin
            )
    ):

        if 'email' not in event['request']['userAttributes']:
            print('email')
            raise Exception("email is required")
        if 'family_name' not in event['request']['userAttributes']:
            print('family_name')
            raise Exception("lastName is required")
        if 'given_name' not in event['request']['userAttributes']:
            print('given_name')
            raise Exception("firstName is required")
        if 'custom:userType' not in event['request']['userAttributes']:
            event['request']['userAttributes']['custom:userType'] = 'Data_Engineer'
            print('userType')
        # if 'phone_number' not in event['request']['userAttributes']:
        #     print('phone_number')
        #     raise Exception("mobilePhone is required")
        #     raise Exception("userType is required")
        # if 'name' not in event['request']['userAttributes']:
        #     raise Exception("displayName is required")

        email = event['request']['userAttributes']['email']
        family_name = event['request']['userAttributes']['family_name']
        given_name = event['request']['userAttributes']['given_name']
        user_type = event['request']['userAttributes']['custom:userType']
        sub = event['request']['userAttributes']['sub']
        if user_type == 'Admin': role_id = 0
        elif user_type == 'Data_Engineer': role_id = 1
        else: role_id = 2
        created_from = 'okta' if (
                    event['triggerSource'] == 'PostConfirmation_ConfirmSignUp' and
                    event['request']['userAttributes']['cognito:user_status'] == 'EXTERNAL_PROVIDER'
            # user who comes from okta
            ) else 'cognito'
        
        role_status = None
        if(role_id == 0):
            role_status='active'
        query = "INSERT INTO users (uuid, first_name, last_name, email, role_id, created_from, role_status) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        values = (
            sub,
            given_name,
            family_name,
            email,
            role_id,
            created_from,
            role_status
        )
        cursor.execute(query, values)
        conn.commit()
        print(f"New user {email} inserted into users table")

        if(created_from == 'okta'):
            email = 'okta_' + email
        region_name = os.environ.get('Region')
        client = boto3.client('cognito-idp', region_name=region_name)
        user_pool_id = os.environ.get('user_pool_id')

        # Specify the updated attributes
        updated_attributes = [{
            'Name': 'custom:userType',
            'Value': user_type
        }]

        # Update user attributes in cognito pool
        client.admin_update_user_attributes(
            UserPoolId=user_pool_id,
            Username=email,
            UserAttributes=updated_attributes
        )
    return event