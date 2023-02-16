from src.s3_helper import s3_list_buckets, s3_list_prefix_buckets
import boto3
from moto import mock_s3


@mock_s3
def test_single_s3_list_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='my-test-bucket')
    bucket_names = s3_list_buckets()
    assert 'my-test-bucket' in bucket_names

@mock_s3
def test_multiple_s3_list_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='my-test-bucket')
    s3.create_bucket(Bucket='my-test-bucket-2')
    bucket_names = s3_list_buckets()
    assert 'my-test-bucket' in bucket_names
    assert 'my-test-bucket-2' in bucket_names

@mock_s3
def test_bucket_list_for_prefix():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.create_bucket(Bucket='my-test-bucket')
    bucket_names = s3_list_prefix_buckets()
    assert bucket_names == 'nc-de-databakers-csv-store-20202'

@mock_s3
def test_bucket_for_input_folder_if_bucket_is_prefixed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202', Key='input_csv_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-csv-store-20202')
    assert "input_csv_key" in object_names["Contents"][0]["Key"]

# @mock_s3
# def test_create_
# # # @mock_s3
# # # def test_bucket_list_for_prefix():
# # #     s3 = boto3.client('s3')
# # #     s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
# # #     with open()
# # #     response = s3.list_objects(Bucket='nc-de-databakers-csv-store-20202')
# # #     print(response["Contents"])
# # #     assert