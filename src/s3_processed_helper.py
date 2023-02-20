from src.s3_helper import s3_upload_and_local_log
import boto3
import os

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
    processed_bucket = s3_list_prefix_buckets()
    objects = s3.list_objects(Bucket=processed_bucket)['Contents']
    for object in objects:
        if object['Key'] == 'processed_csv_key/setup_success_csv_processed.txt':
            return True

def s3_upload_pqt_files():
    s3=boto3.resource('s3')
    if s3_setup_success():
        pqt_files = os.listdir('pqt_tmp')
        for file in pqt_files:
            s3.Bucket(s3_list_prefix_buckets()).upload_file(f'./pqt_tmp/{file}', f'processed_csv_key/{file}')
    else:
        raise ValueError("Terraform deployment unsuccessful")

def s3_upload_pqt_and_delete_csv_files_from_input_key():
    s3_upload_and_local_log
    s3_upload_pqt_files
    s3=boto3.resource('s3')
    bucket_name = s3_list_prefix_buckets()
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix='input_csv_key/'):
        if obj.key.endswith('.csv'):
            obj.delete()
