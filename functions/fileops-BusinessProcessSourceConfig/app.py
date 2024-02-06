import json
import psycopg2
from dbconnection import cursor, conn
import pandas as pd
def lambda_handler(event, context):
  print("Received Event: ", str(event))
  try:
    query_params = event["queryStringParameters"]
    if query_params is not None and "business_process_id" in query_params:
      business_process_id = query_params["business_process_id"]
      get_business_procces_details = """SELECT j.job_name,sfc.standard_validations,sfc.file_config_details,sfc.email_notification,
                                        sfc.file_format,sfc.jira_incident_to_be_raised_for_failures,j.id, 
                                        j.schedule_json as job_schedule,bp.no_of_files,bp."name" as business_name,
                                        tc."name" as target_name,tc.host as target_host,tc.user_name as target_user_name,
                                        tc.connectivity_type as target_connectivity_type,tc.location_pattern as target_location_pattern,
                                        sc."name" as source_name,tc.host as source_host,tc.user_name as source_user_name,
                                        tc.connectivity_type as source_connectivity_type,tc.location_pattern as source_location_pattern
                                        FROM jobs AS j
                                        JOIN business_process as bp on bp.id = j.business_process_id
                                        JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                        left join target_config as tc on sfc.target_id = tc.id
                                        left join source_config as sc on sc.id = sfc.source_id
                                        WHERE bp.uuid =  %s 
                                    """
      cursor.execute(get_business_procces_details, (business_process_id,))
      get_business_procces_details_data = cursor.fetchall()
      cols = list(map(lambda x: x[0], cursor.description))
      df = pd.DataFrame(get_business_procces_details_data, columns=cols)
      records = df.to_dict(orient="records")
      # source_id = []
      # target_id = []
      # for item in records:
      #     if item['source_and_target'] is not None and item['source_and_target']['source_id'] is not None and len(item['source_and_target']['source_id']) > 0:
      #        source_id.append(item['source_and_target']['source_id'])
      #     if item['source_and_target'] is not None and item['source_and_target']['target_id'] is not None and item['source_and_target']['target_id'] != "":
      #        target_id.append(item['source_and_target']['target_id'])
      # source_id = tuple(source_id)
      # target_id = tuple(target_id)
      # if len(source_id) > 0:
      #   source_config = """
      #                       select uuid,name,user_name,connectivity_type,location_pattern,id,host from source_config where uuid in %s
      #                                           """
      #   cursor.execute(source_config, (source_id,))
      #   source_data = cursor.fetchall()
      #   cols = list(map(lambda x: x[0], cursor.description))
      #   df = pd.DataFrame(source_data, columns=cols)
      #   source_dict = df.to_dict(orient="records")
      #   source_dict = {item['uuid']: item for item in source_dict}
      # if len(target_id) > 0:
      #   target_config = """
      #                        select uuid,name,user_name,connectivity_type,location_pattern,id,host  from target_config where uuid in %s
      #                                            """
      #   cursor.execute(target_config, (target_id,))
      #   target_data = cursor.fetchall()
      #   cols = list(map(lambda x: x[0], cursor.description))
      #   df = pd.DataFrame(target_data, columns=cols)
      #   target_dict = df.to_dict(orient="records")
      #   target_dict = {item['uuid']: item for item in target_dict}
      #
      # for item in records:
      #     if len(source_id) > 0  and item['source_and_target']['source_id'] is not None:
      #       item['source_and_target']['source'] = source_dict.get(item['source_and_target']['source_id'])
      #     if len(target_id) > 0 and item['source_and_target']['target_id'] is not None:
      #       item['source_and_target']['target'] = target_dict.get(item['source_and_target']['target_id'])
      return response(200, "retrive succesfully", records)

    else:
       return response(400, "provide business_process_id", "")


  except psycopg2.Error as error:
    conn.rollback()
    print("Error occurred:", error)
    return response(400, str(error), None)
  except Exception as error:
    print(error)
    return response(500, str(error), None)

def response(stauts_code, message, data):
    """
    This is a response method
    """
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
