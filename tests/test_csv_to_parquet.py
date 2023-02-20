from moto import mock_s3
import boto3
import pytest
from unittest.mock import patch
import os
from src.csv_to_parquet import convert_csv_to_parquet, list_files_to_convert
import src.csv_to_parquet

@mock_s3
def test_connection_to_bucket():
  s3=boto3.client('s3')
  response=s3.list_buckets()['Buckets']
  with pytest.raises(ValueError):
    try:
      print(response)
      assert len([bucket['Name'] for bucket in response]) > 0
    except:
      raise ValueError("ERROR: No buckets found")

@mock_s3
def test_list_files_to_convert():
  s3 = boto3.client("s3")
  s3.create_bucket(Bucket='parquet-test-bucket')
  with patch ('src.csv_to_parquet.s3_parquet_prefix_buckets', return_value="parquet-test-bucket"):
    s3.put_object(Body="./setup_success_csv_input.txt", Bucket="parquet-test-bucket", Key="setup_success_csv_input.txt")
    
    list=s3.list_objects(Bucket='parquet-test-bucket')['Contents']
    for key in list:
      s3.download_file('parquet-test-bucket', key['Key'], f'./tmp/{key["Key"]}')
    
    assert os.path.isfile("./tmp/setup_success_csv_input.txt")