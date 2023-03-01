import pandas as pd
import logging
import boto3
import glob
import os


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
  """_summary_

  Args:
      event (_type_): _description_
      context (_type_): _description_
  """
  
  try:
    if not os.path.isdir('./tmp'):
      os.makedirs('./tmp')
    if not os.path.isdir('./tmp'):
      os.makedirs('./tmp')
  
    #download csv files from s3
    list_files_to_convert()
    
    #make dimemsions and fact tables
    
   
    #convert to parquet
    convert_csv_to_parquet()
    
    #update conversion csv
    update_csv_conversion_file()
    
    #upload to the parquet input bucket
    
  except:
    pass
  
def s3_list_buckets():
  """ 
    
    Args:
        Not required.

    Returns:
        returns a list of buckets available in s3
  """
  s3=boto3.client('s3')
  response=s3.list_buckets()['Buckets']
  try:
    return [bucket['Name'] for bucket in response]
  except:
    raise ValueError("ERROR: No buckets found")

def s3_parquet_prefix_buckets():
  """ 
    Args:
        Not required.

    Returns:
        returns the full specified bucket name
  """
  full_list = s3_list_buckets()
  if full_list == []:
      raise ValueError("ERROR: No buckets found")
  for bucket in full_list:
      if "nc-de-databakers-csv-store" in bucket:
          return bucket
  raise ValueError("ERROR: Prefix not found in any bucket")

def list_files_to_convert():
  """ 
    downloads all the files to a temporary local directory
    Args:
        Not required.

    Returns:
        nothing
        
  """
  s3=boto3.client('s3')
  bucket = s3_parquet_prefix_buckets()
  list=s3.list_objects(Bucket=bucket)['Contents']
  for key in list:
    if ('input_csv_key' in key['Key']):
      s3.download_file(bucket, key['Key'], f'./tmp/{key["Key"].split("/")[1]}')

def convert_csv_to_parquet():
  """ 
    locally stored files from "./tmp/dim_*.csv" "./tmp/fact*.csv" are converted to parquet,
    and saved in a pqt_tmp folder
    Args:
        Not required.

    Returns:
        nothing
  """
  dims = glob.glob("./tmp/dim_*.csv")
  facts = glob.glob("./tmp/fact_*.csv")
  file_list = dims + facts
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
      raise ValueError(f'ERROR: {EDE}')
    except Exception:
      raise ValueError(f'ERROR: {Exception}')

def update_csv_conversion_file():
  """ 
    function downloads the Run number in the csv_conversion.txt is ammended to increment the count,#
    then uploads to processed_csv_key on the bucket.
    Args:
        Not required.

    Returns:
        nothing
  """
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
      