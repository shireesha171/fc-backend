"""This module is for forming the error structure"""
import psycopg2
from dbconnection import conn, cursor
import pandas as pd


def forming_error(data):
    job_query = """
                SELECT * from status_codes
                """
    cursor.execute(job_query)
    data_db = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    df = pd.DataFrame(data_db, columns=cols)
    records = df.to_dict(orient="records")
    key_records = {x['key']: x for x in records if x['key'] is not None}
    errorList = []
    for item in data.keys():
        if item in key_records:
            rec = key_records[item]

            error_code = rec['errorcode']
            error_desc = rec['error_desc']

            data_item_value = data[item]['value'] if "value" in data[item] else None
            data_item_status_type = data[item]["status_type"] if "status_type" in data[item] else rec[
                'status_type']

            errorList.append(get_error_obj(error_code, error_desc, item, data_item_value, data_item_status_type))

    return errorList


def get_error_obj(code, message, item, value, status_type):
    """This method is for getting error object"""
    code = "DIGEST-ERR-" + code
    obj = {
        "code": code,
        "message": message,
        "name": item,
        "value": value,
        "status_type": status_type
    }
    return obj
