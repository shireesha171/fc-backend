
import json
from dbconnection import cursor,conn
import pandas as pd
import json
from functools import reduce
import boto3
import os
import pytz
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
def run_job_detail_fun(run_job_id):
    try:
        job_query = """
           select j.uuid as job_uuid,tc.uuid as target_id, j.job_name,jr.uuid as job_run_id,bp."name" as business_process_name ,bup.file_name,
          jr.created_at,jr.status,jr.errors,jr.job_type,us.first_name,us.last_name, jr.error_records,sc.dqrules,sc.standard_validations
          from  job_runs as jr
          join  jobs as j on j.id = jr.job_id
          join source_file_config as sc on sc.job_id = j.id
          join business_user_validate_process as bup on bup.id = jr.business_user_validate_process_id
          join business_process as bp on bp.id  = j.business_process_id
          left join target_config tc on tc.id = sc.target_id
          join users as us on us.id = bp.created_by
          where jr.uuid = %s
           """
        cursor.execute(job_query,(run_job_id[1],))
        data = cursor.fetchall()
        cols = list(map(lambda x: x[0], cursor.description))
        df = pd.DataFrame(data, columns=cols)
        records = df.to_dict(orient="records")
        print(run_job_id[1])
        if records:
            for record in records:
                record['created_at'] = str(record['created_at'])
                if record['error_records'] is not None and record['dqrules'] is not None:
                    errors_record_filter = [item for item in record['error_records'].values() if len(item) > 0]
                    if len(errors_record_filter) > 0:
                        print("eneeeeeeee")
                        error_records = reduce(lambda x, y: x + y, errors_record_filter)
                        for item in error_records:
                            if item["column_name"] in record['dqrules']:
                                item['dqrule'] = record['dqrules'][item['column_name']]
                del record['dqrules']
                #     # error_records = [ item,dqrules =  record['dqrules'][item['column_name']] for item in error_records if item["column_name"] in record['dqrules']]
                #     for item in error_records:
                #         print("entered")
                #         if item["column_name"] in record['dqrules']:
                #            item['dqrule'] = record['dqrules'][item['column_name']]
                # del record['dqrules']
        # if records is not None:
            record = records[0]
            # print(record['source_and_target'])
            if record is not None and record['target_id'] is not None:
                data = downloads3("data.csv", record['job_uuid'])
                if data is not None:
                    record['pre_signed_url_information'] = data
        return records
    except Exception as error:
        return str(error)



def downloads3(s3_filename, job_uuid):
    """This method is for uploding the validating file to S3"""
    s3_file = "target-files/" + job_uuid + "/" + s3_filename
    try:
        s3_client = boto3.client('s3')
        # Generate the presigned URL
        pre_signed_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_file},
            ExpiresIn=600  # URL expiration time in seconds (adjust as needed)
        )
        data = {
            "presignedURL": pre_signed_url,
            "job_id": job_uuid
        }
        return data

    except Exception as error:
        print("check", error)
        return (401, str(error), None)


def pst_time(utc_time):
    # Set the UTC timezone
    utc_timezone = pytz.utc
    utc_time = utc_timezone.localize(utc_time)

    # Convert to PST timezone
    pst_timezone = pytz.timezone('America/Los_Angeles')  # 'America/Los_Angeles' is the timezone ID for PST
    pst_time = utc_time.astimezone(pst_timezone)

    return pst_time.strftime('%Y-%m-%d %H:%M:%S')