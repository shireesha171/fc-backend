"""
In this module we are trying to retrive the business processes list and jobs under the those business process.
"""
import json
import psycopg2
from dbconnection import cursor, conn


def lambda_handler(event, context):
    """ Getting Business processes and their details"""
    print("Received Event: ", str(event))

    try:
        if event['resource'] == "/business-process" and event['httpMethod'] == 'DELETE':
            if 'body' not in event:
                return response(400, 'INVALID_PAYLOAD', None)
            body = json.loads(event['body'])
            uuid = body['business_process_id']
            query = "UPDATE business_process SET status = %s WHERE uuid = %s"
            values = ('Deleted', uuid)
            cursor.execute(query, values)
            conn.commit()
            print("DELETE_BUSINESS_PROCESS_SUCCESS", {"business_process_id": uuid})

            update_groups = """
                UPDATE groups 
                SET business_process_list = business_process_list - %s
                    WHERE business_process_list ? %s
            """
            values = (
                uuid,
                uuid
            )
            cursor.execute(update_groups, values)
            conn.commit()
            return response(200, 'DELETE_BUSINESS_PROCESS_SUCCESS', {"business_process_id": uuid})

        else:
            query_params = event.get("queryStringParameters", None)
            # Getting the list of jobs and their details of a given Business Process.
            if query_params is not None and "business_process_id" in query_params and query_params.get("business_process_id") != "":
                business_process_id = query_params["business_process_id"]
                get_business_procces_details = f"""
                                                SELECT B_P.name, B_P.uuid, B_P.email_notification, jobs.job_name, jobs.uuid, jobs.status, 
                                                jobs.file_name, jobs.file_size, B_P.jira_service_management
                                                FROM business_process AS B_P
                                                JOIN jobs AS jobs ON B_P.id = jobs.business_process_id
                                                WHERE B_P.uuid = %s and B_P.status !='Deleted' and jobs.status != 'Deleted'
                                            """
                cursor.execute(get_business_procces_details, (business_process_id,))
                get_business_procces_details_data = cursor.fetchall()
                business_process_jobs_list = []
                for column in get_business_procces_details_data:
                    business_process_job_details = {}
                    business_process_job_details = {
                        "business_process_name": column[0],
                        "business_process_id": column[1],
                        "email_notification": column[2],
                        "job_name": column[3],
                        "job_id": column[4],
                        "status": column[5],
                        "file_name": column[6],
                        "file_size": column[7],
                        "jira_service_management": column[8]
                    }
                    business_process_jobs_list.append(business_process_job_details)
                return response(200, "The fetch results are ready", business_process_jobs_list)
            # Getting the list of Business Processes and their details.
            else:
                recordsperpage = query_params['recordsperpage']
                offset = query_params['offset']
                if query_params is not None and "group_id" in query_params and query_params.get("group_id") != "" and query_params.get("group_id") != "undefined" and query_params.get("role_id") != "0":
                    group_id = query_params['group_id']
                elif query_params is not None and "role_id" in query_params and query_params.get("role_id") == "0":
                    #If user is an Admin
                    # return response(400, "group_id is missing in request params", None)
                    pagination_query = f"offset {offset} ROWS FETCH next {recordsperpage} ROWS ONLY"
                    get_business_processes = f"""SELECT bp.name, bp.uuid, bp.no_of_files, bp.created_at, u.first_name, 
                                                bp.status,(SELECT STRING_AGG(distinct j.job_type, ',') FROM jobs j WHERE j.business_process_id = bp.id) AS run_type, count(*) OVER() AS full_count
                                                FROM business_process bp
                                                INNER JOIN users as u ON u.id = bp.created_by 
                                                where bp.status !='Deleted'
                                                ORDER BY bp.id DESC {pagination_query}
                                            """
                    cursor.execute(get_business_processes)
                    business_processes_data = cursor.fetchall()
                    business_processs_list = []
                    conn.commit()

                    for column1 in business_processes_data:
                        column = list(column1)
                        business_process_details = {}
                        if column[6] == "adhoc":
                            column[6] = "Manual"
                        elif column[6] == "scheduled":
                            column[6] = "Automatic"
                        elif column[6] is None:
                            column[6] = "-"
                        else:
                            column[6] = "Semi Automatic"
                        business_process_details = {
                            "business_process_name": column[0],
                            "no_of_files": column[2],
                            "status": column[5],
                            "created_at": str(column[3]),
                            "created_by": column[4],
                            "business_process_id": column[1],
                            "run_type": column[6],
                            "full_count": column[7]
                        }
                        business_processs_list.append(business_process_details)
                    return response(200, "The fetch results are ready", business_processs_list)
                else:
                    return response(400, "group_id is missing in params", None)

                
            #If user is not an Admin
            pagination_query = f"offset {offset} ROWS FETCH next {recordsperpage} ROWS ONLY"
            group_associated_bp_list = getGroupAssociatedBusinessProcess(group_id)
            if group_associated_bp_list:
                get_business_processes = f"""SELECT bp.name, bp.uuid, bp.no_of_files, bp.created_at, u.first_name, 
                                            bp.status,(SELECT STRING_AGG(distinct j.job_type, ',') FROM jobs j WHERE j.business_process_id = bp.id) AS run_type, count(*) OVER() AS full_count
                                            FROM business_process bp
                                            INNER JOIN users as u ON u.id = bp.created_by 
                                            where bp.status !='Deleted' AND bp.uuid IN %s
                                            ORDER BY bp.id DESC {pagination_query}
                                        """
                cursor.execute(get_business_processes, (tuple(group_associated_bp_list),))
                business_processes_data = cursor.fetchall()
                business_processs_list = []
                conn.commit()

                for column1 in business_processes_data:
                    column = list(column1)
                    business_process_details = {}
                    if column[6] == "adhoc":
                        column[6] = "Manual"
                    elif column[6] == "scheduled":
                        column[6] = "Automatic"
                    elif column[6] is None:
                        column[6] = "-"
                    else:
                        column[6] = "Semi Automatic"
                    business_process_details = {
                        "business_process_name": column[0],
                        "no_of_files": column[2],
                        "status": column[5],
                        "created_at": str(column[3]),
                        "created_by": column[4],
                        "business_process_id": column[1],
                        "run_type": column[6],
                        "full_count": column[7]
                    }
                    business_processs_list.append(business_process_details)
                return response(200, "The fetch results are ready", business_processs_list)
            else:
                print("No business processes are present in the group. Please contact File Central Administrator", group_id)
                return response(404, "No business processes are present in the group. Please contact File Central Administrator", None)
    except psycopg2.Error as error:
        conn.rollback()
        print("Error occurred:", error)
        return response(400, str(error), None)
    except Exception as error:
        conn.rollback()
        print(error)
        return response(500, str(error), None)

def getGroupAssociatedBusinessProcess(group_id):
    print(group_id)
    get_group_details = "SELECT group_name, business_process_list FROM groups WHERE uuid = %s"
    cursor.execute(get_group_details, (group_id,))
    conn.commit()
    group_data = cursor.fetchone()
    if group_data is None:
        print("No business process present in group", group_id)
        return []
    elif group_data[1] is not None:
        business_process_list = group_data[1]
        bp_ids_list = list(business_process_list.keys())
        return bp_ids_list
        
        
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
