import boto3
import pandas as pd
import glob
import os

s3=boto3.client('s3')
def s3_list_buckets():
    response=s3.list_buckets()['Buckets']
    try:
      return [bucket['Name'] for bucket in response]
    except:
      raise ValueError("ERROR: No buckets found")

def s3_parquet_prefix_buckets():
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-parquet-store" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def list_files_to_convert():
  bucket = s3_parquet_prefix_buckets()
  list=s3.list_objects(Bucket=bucket)['Contents']
  for key in list:
    s3.download_file(bucket, key['Key'], f'./tmp/{key["Key"]}')

def convert_csv_to_parquet():
  file_list = glob.glob("./tmp/*.csv")
  for file in file_list:
    filename = os.path.basename(file).split('.')[0]
    df = pd.read_csv(file)
    df.to_parquet(f'./pqt_tmp/{filename}.parquet')
  
