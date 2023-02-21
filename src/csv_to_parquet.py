import boto3
import pandas as pd
import glob
import os

def s3_list_buckets():
  s3=boto3.client('s3')
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
        if "nc-de-databakers-csv-store" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def list_files_to_convert():
  s3=boto3.client('s3')
  bucket = s3_parquet_prefix_buckets()
  list=s3.list_objects(Bucket=bucket)['Contents']
  for key in list:
    s3.download_file(bucket, key['Key'], f'./tmp/{key["Key"]}')

def convert_csv_to_parquet():
  file_list = glob.glob("./tmp/*.csv")
  if len(file_list) < 1:
    raise ValueError('ERROR: No CSV files to convert to parquet')
  for file in file_list:
    filename = os.path.basename(file).split('.')[0]
    df = pd.read_csv(file)
    
    if not os.path.exists("pqt_tmp"):
        os.makedirs("pqt_tmp")
    try:
      df.to_parquet(f'./pqt_tmp/{filename}.parquet')
    except pd.errors.EmptyDataError as EDE:
      raise f'ERROR: {EDE}'
    except Exception:
      raise f'ERROR: {Exception}'
    
def update_csv_conversion_file():
  s3=boto3.client('s3')
  full_list = s3_list_buckets()
  bucket_str = ''
  for bucket in full_list:
      if "nc-de-databakers-csv-store" in bucket:
            bucket_str = bucket
  s3.download_file(bucket_str, 'processed_csv_key/csv_conversion.txt', './tmp/csv_conversion.txt')
  
  contents = open('./tmp/csv_conversion.txt', 'r').read()
  num = int(contents.split(' ')[1])
  with open('./tmp/csv_conversion.txt', 'w+') as file:
    file.write(f'Run {num+1}')
  s3.upload_file("./tmp/csv_conversion.txt", bucket_str, "processed_csv_key/csv_conversion.txt")
  
  
