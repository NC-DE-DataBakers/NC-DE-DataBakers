from moto import mock_s3
import boto3
import pytest
from unittest.mock import patch
import os
from src.csv_to_parquet import convert_csv_to_parquet

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

def test_name():
  filename = os.path.basename('/temp/file.txt').split('.')[0]
  print(filename)
  convert_csv_to_parquet()
  assert './'
  