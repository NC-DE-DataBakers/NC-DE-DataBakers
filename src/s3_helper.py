import boto3
from src.conn import lambda_handler
import os
from datetime import datetime as dt

def s3_list_buckets():
    s3=boto3.client('s3')
    response=s3.list_buckets()['Buckets']
    return [bucket['Name'] for bucket in response]

def s3_list_prefix_buckets():
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-csv-store-" in bucket:
            return bucket
    raise ValueError("Prefix not found in any bucket")

def s3_setup_success():
    s3=boto3.client('s3')
    input_bucket = s3_list_prefix_buckets()
    objects = s3.list_objects(Bucket=input_bucket)['Contents']
    for object in objects:
        if object['Key'] == 'input_csv_key/setup_success_csv_input.txt':
            return True

def s3_upload_csv_files():
    s3=boto3.resource('s3')
    if s3_setup_success():
        lambda_handler()
        csv_files = os.listdir('tmp')
        for file in csv_files:
            s3.Bucket(s3_list_prefix_buckets()).upload_file(f'./tmp/{file}', f'input_csv_key/{file}')
    else:
        raise ValueError("Terraform deployment unsuccessful")

def create_csv_completed_text_file():
    s3_upload_csv_files()
    if not os.path.exists("run_number.txt"):
        with open("run_number.txt", "w") as rn_f:
            rn_f.write("0")
    with open("run_number.txt", "r+") as rn_f:
        run_num = int(rn_f.read()) + 1
        rn_f.seek(0)
        rn_f.write(str(run_num))
    with open(f"csv_export_completed.txt", "a+") as f:
        f.write(f'run {run_num}\n')

def s3_upload_and_local_log():
    create_csv_completed_text_file()
    s3=boto3.resource('s3')
    s3.Bucket(s3_list_prefix_buckets()).upload_file('./csv_export_completed.txt', 'input_csv_key/csv_export_completed.txt')
