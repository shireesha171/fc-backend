import os

# Environment
env = os.environ.get('Environment')

# S3 File Storage
__s3_file_storage = os.environ.get('S3FileStorage')
s3_fileops_storage_bucket = f'{__s3_file_storage}-{env}'

# Constants
job_run_logs_folder = "job_run_logs"