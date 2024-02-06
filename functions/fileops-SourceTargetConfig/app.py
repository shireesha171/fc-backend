"""This module is for source file configuration"""
import json
import uuid
from dbconnection import cursor, conn
import pandas as pd
import psycopg2


def insert_data(table_name, body, created_by):
    print("checking")

    insert_into_sc = f"""
                     INSERT INTO {table_name}(uuid, name, host, user_name, password, location_pattern, absolute_file_path, connectivity_type,status, created_by)
                     VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                     RETURNING *
                 """
    values = (
        str(uuid.uuid4()),
        body["name"] if body["name"] else '',
        body["host"] if body["host"] else '',
        body["user_name"] if body["user_name"] else '',
        body["password"] if body["password"] else '',
        body["location_pattern"] if body["location_pattern"] else '',
        body["absolute_file_path"] if body["absolute_file_path"] else '',
        body["connectivity_type"] if body["connectivity_type"] else '',
        'draft',
        created_by,
    )
    cursor.execute(insert_into_sc, values)
    data = cursor.fetchone()
    cols = list(map(lambda x: x[0], cursor.description))
    df = pd.DataFrame([data], columns=cols)
    record = df.to_dict(orient="records")[0]
    record['created_on'] = str(record['created_on'])
    print(record, "------------")
    conn.commit()
    return record


def check_source_or_target_exits(table_name, data):
    print(data)

    config_name = data['name']
    print("adsfasdf", config_name)
    query = f"SELECT uuid FROM {table_name} where name=%s and status !=%s"
    # print(data.get(name))
    cursor.execute(query, (config_name,"deleted"))
    uuid = cursor.fetchone()
    print(uuid)
    if uuid is not None:
        return True
    return False


def lambda_handler(event, context):
    """This method is for handling the source file configurations"""
    print("Received Event: ", str(event))
    if event['requestContext'] and not event['requestContext']['authorizer']['claims']:
        return response(400, "user is not authenticated", None)
    user_id = event['requestContext']['authorizer']['claims']['sub']
    try:
        if event['resource'] == "/source-target-config" and event['httpMethod'] == 'POST':
            if 'body' not in event:
                return response(400, 'please provide payload', None)
            body = json.loads(event['body'])
            return insert(user_id,body)
        elif event['resource'] == "/source-target-config" and event['httpMethod'] == 'GET':
            return editConfiguration(user_id,event)
        elif event['resource'] == "/source-target-config" and event['httpMethod'] == 'PUT':
            print("inside event")
            if 'body' not in event:
                return response(400, 'please provide update payload', None)
            body = json.loads(event['body'])
            print("body")
            return updateConfiguration(user_id, body)
        elif event['resource'] == "/source-target-config" and event['httpMethod'] == 'DELETE':
            return deleteConfiguration(user_id, event)
        elif event['resource'] == "/source-target-config/list" and event['httpMethod'] == 'GET':
            return ListOfConfigurations(user_id,event)
        else:
            return response(400, 'resource or endpoint not found', None)
    except Exception as error:
        print(error)
        conn.rollback()
        return response(400, str(error), None)

def insert(user_id, body):
    try:
        query = "SELECT id FROM users where uuid = %s"
        cursor.execute(query, (user_id,))
        created_by = cursor.fetchone()

        if body['config_type'] == 'source':
            source_table = 'source_config'
            # name = 'source_name'
            if check_source_or_target_exits(source_table, body):
                return response(400, str("source config details are already exists"), None)
            body = insert_data(source_table, body, created_by)
        elif body['config_type'] == 'target':
            target_table = 'target_config'
            # name = 'target_name'
            if check_source_or_target_exits(target_table, body):
                return response(400, str("target config details are already exists"), None)
            body = insert_data(target_table, body, created_by)
        else:
            return response(400, str("please provide config type"), None)
        return response(200, "The data inserted successfully", body)

    except Exception as error:
        print(error)
        return response(400, str(error), None)


def editConfiguration(user_id, event):
    print(event['queryStringParameters'])
    if 'queryStringParameters' not in event:
        return response(400, "Please provide Queryparameters", None)
    params = event['queryStringParameters']
    if 'source_id' in params:
        id = params['source_id']
        query = f"""select * 
                    from source_config where uuid = %s 
                    ORDER BY source_config.id DESC
                """
    elif 'target_id' in params:
        id = params['target_id']
        query = f"""select * 
                   from target_config where uuid = %s 
                   ORDER BY target_config.id DESC
                """
    else:
        return response(400, "Please provide source_id or target_id", None)
    cursor.execute(query, (id,))
    column_names = [desc[0] for desc in cursor.description]
    column_names.append('full_count')
    res = cursor.fetchone()
    obj = {}
    for index,item in enumerate(res):
        obj[column_names[index]] = res[index]
    print(obj)
    del obj['created_on']
    return response(200, "The data fetched successfully", obj)

def deleteConfiguration(user_id, event):
    print(event['queryStringParameters'])

    if 'queryStringParameters' not in event:
        return response(400, "Please provide Queryparameters", None)
    params = event['queryStringParameters']
    if 'source_id' in params:
        id = params['source_id']
        query = "update source_config set status=%s where uuid = %s"
    elif 'target_id' in params:
        id = params['target_id']
        query = "update target_config set status=%s where uuid = %s"
    else:
        return response(400, "Please provide source_id or target_id", None)
    print(query, id)
    data = cursor.execute(query, ('deleted',id,))
    conn.commit()
    return response(200, "deleted successfully", id)

def updateConfiguration(user_id, body):
    set_clause = ', '.join(f"{column} = %s" for column in body["update_values"].keys())
    if 'source_id' in body or body["config_type"] == 'source':
        id = body['source_id']
        query = f"update source_config set {set_clause} where uuid = '{id}'"
    elif 'target_id' in body or body["config_type"] == 'target':
        id = body['target_id']
        query = f"update target_config set {set_clause} where uuid = '{id}'"
    else:
        return response(400, "Please provide source_id or target_id", None)


    data = cursor.execute(query, (tuple(body["update_values"].values())))
    conn.commit()
    return response(200, "updated successfully", id)


def ListOfConfigurations(user_id,event):
    print(event['queryStringParameters'])
    if 'queryStringParameters' not in event:
        return response(400, "Please provide Queryparameters", None)
    params = event['queryStringParameters']
    recordsperpage = params['recordsperpage']
    offset = params['offset']
    pagination_query = f"offset {offset} ROWS FETCH next {recordsperpage} ROWS ONLY"
    if 'config_type' in params and params["config_type"] == 'source':
        query = f"""select *,count(*) OVER() AS full_count from source_config where status !='deleted'
                ORDER BY id DESC {pagination_query}"""
    elif 'config_type' in params and params["config_type"] == 'target':
        query = f"""select *, count(*) OVER() AS full_count from target_config where status !='deleted'
                 ORDER BY id DESC {pagination_query}"""
    else:
        return response(400, "Please provide config_type", None)

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    res = cursor.fetchall()
    print(res)
    final = []
    for each in res:
        obj = {}
        for index,item in enumerate(each):
        # obj = {
        #     "name": each[1],
        #     "connectivity_type": each[6],
        #     "host": each[5],
        #     "user_name": each[3],
        #     "location_pattern": each[7],
        #     "absolute_file_path": each[8],
        #     "uuid": each[0],
        # }
            obj[column_names[index]] = each[index]
        del obj['created_on']
        final.append(obj)
    return response(200, "The data fetched successfully", final)

def response(stauts_code, message, data):
    """This is response method"""
    return {
        "statusCode": stauts_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": data}),
    }
