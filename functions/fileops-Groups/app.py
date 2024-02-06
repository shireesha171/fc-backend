"""This module is for analysing the schema"""
import json
from dbconnection import cursor, conn
import uuid
import psycopg2


def lambda_handler(event, context):
    """This method is for handling groups"""
    print("Incoming Event", event)
    try:
        if event['resource'] == "/groups" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return createGroups(body)
            else:
                return response(400, "INVALID_PAYLOAD", None)

        elif event['resource'] == "/groups" and event['httpMethod'] == 'GET':
            query_params = event.get('queryStringParameters', None)
            if query_params is not None and query_params.get('group_id', None) is not None:
                return getGroupDetails(query_params)
            elif query_params is not None and query_params.get('offset', None) is not None:
                return getGroupList(query_params)
            else:
                return response(400, "QUERY_PARAMS_NOT_FOUND", None)

        elif event['resource'] == "/groups" and event['httpMethod'] == 'PUT':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                if(body.get('group_id') is not None):
                    return updateGroups(body)
                else:
                    return response(400, "group_id is missing", None)

            else:
                print("INVALID_PAYLOAD")
                return response(400, "INVALID_PAYLOAD", None)
        else:
            print("INVALID_METHOD")
            return response(404, "INVALID_METHOD", None)
    except Exception as error:
        print("Groups API failed", str(error))
        return response(500, "Groups API failed", None)


def createGroups(body):
    """This method is for creating the groups"""
    try:
        user_id = body.get('user_id', None)
        UUID = str(uuid.uuid4())
        if user_id is None:
            return response(400, "user_id is missing", None)
                # Confirming whether the Business Process name is unique
        business_process_name = is_group_name_unique(body)
        if business_process_name != "unique":
            print("Group name is not unique", body['group_name'])
            return response(409, "The group is already exits with given name, please provide the unique name", None)
        else:
            user_details = getUserDetials(user_id)
            insert_into_groups = """
                                INSERT INTO groups (uuid, group_name, business_process_list, created_by)
                                VALUES(%s, %s, %s, %s)
            """
            values = (
                UUID,
                body.get('group_name', ''),
                json.dumps(body.get('business_process_list', {})),
                user_details['username']
            )
            cursor.execute(insert_into_groups, values)
            conn.commit()

        print("CREATE_GROUP_SUCCESS")
        return response(200, "CREATE_GROUP_SUCCESS", {"group_id": UUID})

    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("CREATE_GROUP_FAILED", str(error))
        return response(500, str(error), None)
    except Exception as error:
        print("CREATE_GROUP_FAILED", str(error))
        return response(500, str(error), None)


def updateGroups(body):
    """This method is for updating the group details"""
    try:
        update_groups = """
                        UPDATE groups 
                        SET group_name = %s,
                            business_process_list = %s
                        WHERE uuid = %s
        """
        values = (
            body.get('group_name'),
            json.dumps(body.get('business_process_list', {})),
            body.get('group_id')
        )
        cursor.execute(update_groups, values)
        affected_rows = cursor.rowcount  # Get the number of affected rows

        if affected_rows == 0:
            raise ValueError("No records found to update")
        conn.commit()

        print("UPDATE_GROUP_SUCCESS")
        return response(200, "UPDATE_GROUP_SUCCESS", None)

    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("UPDATE_GROUP_FAILED", str(error))
        return response(500, str(error), None)
    except Exception as error:
        print("UPDATE_GROUP_FAILED", str(error))
        return response(500, str(error), None)


def getGroupDetails(query_params):
    """This method is for getting the details of the group"""
    try:
        group_id = query_params.get('group_id', None)
        if group_id is None:
            return response(400, "provide valid queryStringParameters", None)
        else:
            get_group_details = "SELECT group_name, business_process_list FROM groups WHERE uuid = %s"
            cursor.execute(get_group_details, (group_id,))
            conn.commit()
            group_data = cursor.fetchone()
            if group_data is None:
                response(404, "provide valid group_id", None)
            else:
                response_data = {
                    "group_id": group_id,
                    "group_name": group_data[0],
                    "business_process_list": group_data[1]
                }

            print("GET_GROUP_DATA_SUCCESS", response_data)
            return response(200, "GET_GROUP_DATA_SUCCESS", response_data)

    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("GET_GROUP_DATA_FAILED", str(error))
        return response(500, "GET_GROUP_DATA_FAILED", str(error))
    except Exception as error:
        print("GET_GROUP_DATA_FAILED", str(error))
        return response(500, "GET_GROUP_DATA_FAILED", str(error))


def getGroupList(query_params):
    """This method is to the list of groups"""
    try:
        recordsperpage = query_params['recordsperpage']
        offset = query_params['offset']
        pagination_query = f"offset {offset} ROWS FETCH next {recordsperpage} ROWS ONLY"
        get_groups_list = f"""
                            SELECT uuid, group_name, business_process_list, created_at, created_by, count(*) OVER() AS full_count
                            FROM groups
                            ORDER BY id DESC {pagination_query}
        """
        cursor.execute(get_groups_list)
        conn.commit()
        data = cursor.fetchall()
        list_of_groups = []
        for row in data:
            group_dict = {}
            group_dict = {
                'group_id': row[0],
                'group_name': row[1],
                'business_process_list': row[2],
                'created_at': str(row[3]),
                'created_by': row[4],
                'full_count': row[5]
            }
            list_of_groups.append(group_dict)
        print("GET_GROUPS_LIST_SUCCESS", list_of_groups)
        return response(200, "GET_GROUPS_LIST_SUCCESS", list_of_groups)

    except psycopg2.DatabaseError as error:
        conn.rollback()
        print("GET_GROUPS_LIST_FAILED", str(error))
        return response(500, "GET_GROUPS_LIST_FAILED", str(error))
    except Exception as error:
        print("GET_GROUPS_LIST_FAILED", str(error))
        return response(500, "GET_GROUPS_LIST_FAILED", str(error))


# see how this works, if it does not work change this.
class InvalidUserIdError(Exception):
    def __init__(self, message="Invalid user ID"):
        self.message = message
        super().__init__(self.message)

def is_group_name_unique(body):
    """This method is for checking the given group name is unique or not"""
    group_name = body["group_name"]
    group_name_query = "SELECT * FROM groups WHERE group_name = %s"
    cursor.execute(group_name_query, (group_name,))
    data = cursor.fetchone()
    conn.commit()
    if data is None:
        result = "unique"
        return result
    else:
        result = "not unique"
        return result

def getUserDetials(user_id):
    """This method is for getting the user details"""
    print("inside the getuser method")
    get_user_details = "SELECT id, first_name FROM users WHERE uuid = %s"
    cursor.execute(get_user_details, (user_id,))
    conn.commit()
    data = cursor.fetchone()
    if data is None:
        raise InvalidUserIdError()
    else:
        user_details = {
            'user_int_id': data[0],
            'username': data[1]
        }
        return user_details


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


