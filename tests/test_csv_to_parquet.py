from src.csv_to_parquet import convert_csv_to_parquet, list_files_to_convert, update_csv_conversion_file
from unittest.mock import patch
import src.csv_to_parquet
from moto import mock_s3
import pandas as pd
import pytest
import boto3
import csv
import os

if not os.path.isdir('./tmp'):
    os.makedirs('./tmp')

@mock_s3
def test_connection_to_bucket():
  s3=boto3.client('s3')
  response=s3.list_buckets()['Buckets']
  with pytest.raises(ValueError):
    try:
      assert len([bucket['Name'] for bucket in response]) > 0
    except:
      raise ValueError("ERROR: No buckets found")

@mock_s3
def test_list_files_to_convert():
  s3 = boto3.client("s3")
  s3.create_bucket(Bucket='parquet-test-bucket')
  with patch ('src.csv_to_parquet.s3_parquet_prefix_buckets', return_value="parquet-test-bucket"):
    open('mocks3testfile.txt', 'w')
    s3.put_object(Body="./mocks3testfile.txt", Bucket="parquet-test-bucket", Key="mocks3testfile.txt")
    
    list=s3.list_objects(Bucket='parquet-test-bucket')['Contents']
    for key in list:
      s3.download_file('parquet-test-bucket', key['Key'], f'./tmp/{key["Key"]}')
    
    assert os.path.isfile("./tmp/mocks3testfile.txt")
    os.remove('./tmp/mocks3testfile.txt')
    assert os.path.isfile("./tmp/mocks3testfile.txt") is False
    
@mock_s3
def test_files_are_converted_to_parquet():
  fields = ['column1', 'column2', 'column3']  
  rows = [ ['A', '011', '2000'], 
        ['B', '012', '8000'],
        ['C', '351', '5000'],
        ['D', '146', '10000'] ] 
  
  with open('./tmp/mockconversion1.csv', 'w') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(fields)
    csv_writer.writerows(rows)
    
  reader = csv.reader(open("./tmp/mockconversion1.csv"))
  # for row in reader:
  #     print(row)
  convert_csv_to_parquet()
  
  assert os.path.isfile("./pqt_tmp/mockconversion1.parquet")
  os.remove('./pqt_tmp/mockconversion1.parquet')
  os.removedirs('./pqt_tmp')
  
  assert os.path.isfile("./pqt_tmp/mockconversion1.parquet") is False
  assert os.path.isdir('./pqt_tmp') is False
  
def test_files_are_converted_to_parquet():
  open('./tmp/mockconversion1.csv', 'w')
  with pytest.raises(pd.errors.EmptyDataError):
    convert_csv_to_parquet()
  os.remove('./tmp/mockconversion1.csv')
  
  assert os.path.isfile("./tmp/mockconversion1.csv") is False

def test_ValueError_raised_for_empty_tmp_folder():
  with pytest.raises(ValueError):
    convert_csv_to_parquet()

@mock_s3   
def test_csv_conversion_file_downloaded():
  s3 = boto3.client("s3")
  bucket = 'nc-de-databakers-csv-store-1010'
  s3.create_bucket(Bucket=bucket)
  with open('./csv_conversion.txt', 'w') as file:
    file.write('Run 0')
  s3.upload_file("./csv_conversion.txt", bucket, "processed_csv_key/csv_conversion.txt")
  with patch ('src.csv_to_parquet.s3_list_buckets', return_value=[bucket]):
    update_csv_conversion_file()

  assert os.path.isfile("./tmp/csv_conversion.txt")
  os.remove('./tmp/csv_conversion.txt')
  assert os.path.isfile("./tmp/csv_conversion.txt") is False

@mock_s3
def test_csv_conversion_run_number_incremented():
  s3 = boto3.client("s3")
  bucket = 'nc-de-databakers-csv-store-1010'
  s3.create_bucket(Bucket=bucket)
  with open('./csv_conversion.txt', 'w') as file:
    file.write('Run 0')
  s3.upload_file("./csv_conversion.txt", bucket, "processed_csv_key/csv_conversion.txt")
  with patch ('src.csv_to_parquet.s3_list_buckets', return_value=[bucket]):
    update_csv_conversion_file()

  with open('./tmp/csv_conversion.txt', 'r') as file:
    contents = file.read()
    assert contents == 'Run 1'
  
  s3.download_file(bucket, 'processed_csv_key/csv_conversion.txt', './tmp/csv_conversion_s3.txt')
  with open('./tmp/csv_conversion_s3.txt', 'r') as file:
    contents = file.read()
    assert contents == 'Run 1'
  os.remove('./tmp/csv_conversion.txt')
  os.remove('./tmp/csv_conversion_s3.txt')
  assert os.path.isfile("./tmp/csv_conversion.txt") is False
  assert os.path.isfile("./tmp/csv_conversion_s3.txt") is False

def test_cleanup():
  files = os.listdir('./tmp')
  for file in files:
          os.remove(f'./tmp/{file}')
  os.removedirs('./tmp')