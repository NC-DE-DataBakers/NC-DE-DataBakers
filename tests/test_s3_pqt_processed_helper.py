from src.s3_pqt_processed_helper import s3_list_buckets, s3_list_prefix_parquet_buckets, s3_pqt_input_setup_success, s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input,s3_create_parquet_processed_completed_txt_file, s3_parquet_processed_upload_and_log
import boto3
from moto import mock_s3
import os
import pytest

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
def test_bucket_list_for_parquet_prefix():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.create_bucket(Bucket='my-test-bucket')
    bucket_list = s3_list_buckets()
    bucket_names = s3_list_prefix_parquet_buckets(bucket_li)
    assert bucket_names == 'nc-de-databakers-parquet-store-20202'

@mock_s3
def test_parquet_bucket_for_input_key_if_bucket_is_prefixed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                  Key='input_parquet_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-parquet-store-20202')
    assert "input_parquet_key" in object_names["Contents"][0]["Key"]

@mock_s3
def test_parquet_bucket_for_processed_key_if_bucket_is_prefixed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                  Key='processed_parquet_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-parquet-store-20202')
    assert "processed_parquet_key" in object_names["Contents"][0]["Key"]

@mock_s3
def test_setup_success_txt_file_exists_pqt_input():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202', Key='input_parquet_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    assert s3_pqt_input_setup_success() == True

@mock_s3
def test_setup_success_txt_file_exists_pqt_processed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202', Key='procecced_parquet_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    assert s3_pqt_input_setup_success() == True

@mock_s3
def test_parquet_files_are_uploaded_successfully_to_the_processed_key_within_bucket():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    bucket_name= 'nc-de-databakers-parquet-store-20202'
    key='input_parquet_key'
    parquet_file_path="./pqt_tmp"
    parquet_files = ['address.parquet', 'counterparty.parquet', 'currency.parquet', 'department.parquet', 'design.parquet','payment_type.parquet', 'payment.parquet', 'purchase_order.parquet', 'sales_order.parquet', 'staff.parquet', 'transaction.parquet']
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='input_parquet_key')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    for parquet in os.listdir(parquet_file_path):
        parquet_file = os.path.join(parquet_file_path, parquet)
        s3cli.upload_file(parquet_file, bucket_name, f"{key}/{parquet}")
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='processed_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_processed.txt', 'processed_parquet_key/setup_success_parquet_processed.txt')
    s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input()
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-parquet-store-20202', Prefix='processed_parquet_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert all(f'processed_parquet_key/{file}' in keys for file in parquet_files)

@mock_s3
def test_upload_parquet_to_processed_parquet_and_delete_input_parquet_files():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    bucket_name= 'nc-de-databakers-parquet-store-20202'
    key='input_parquet_key'
    parquet_file_path="./tmp"
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='input_parquet_key')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    for parquet in os.listdir(parquet_file_path):
        parquet_file = os.path.join(parquet_file_path, parquet)
        s3cli.upload_file(parquet_file, bucket_name, f"{key}/{parquet}")
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='processed_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_processed.txt', 'processed_parquet_key/setup_success_parquet_processed.txt')
    s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input()
    input_parquet_objects = [obj.key for obj in s3res.Bucket('nc-de-databakers-parquet-store-20202').objects.filter(Prefix='input_parquet_key/')]
    parquet_count = sum([1 for obj in input_parquet_objects if obj.endswith('.parquet')])
    assert parquet_count == 0

@mock_s3
def test_creates_parquet_processed_export_completed_txt_file_if_files_are_uploaded():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='input_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='processed_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_processed.txt', 'processed_parquet_key/setup_success_parquet_processed.txt')
    s3_create_parquet_processed_completed_txt_file()
    assert os.path.isfile('parquet_processed_export_completed.txt')

@mock_s3
def test_logs_for_each_parquet_processed_run_by_checking_log_line_count():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='input_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='processed_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_processed.txt', 'processed_parquet_key/setup_success_parquet_processed.txt')
    s3_parquet_processed_upload_and_log()
    with open("parquet_processed_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 2
    s3_parquet_processed_upload_and_log()
    with open("parquet_processed_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 3
    s3_parquet_processed_upload_and_log()
    with open("parquet_processed_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 4

@mock_s3
def test_parquet_processed_latest_run_num_matches_final_run_in_parquet_processed_export_completed_file():
    with open("parquet_processed_run_number.txt") as file_1, open("parquet_processed_export_completed.txt") as file_2:
        first_line = file_1.readline().strip()
        last_line = file_2.readlines()[-1].strip()
    assert last_line == f"run {first_line}"

@mock_s3
def test_uploads_parquet_processed_export_file_to_key_within_bucket():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='input_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='processed_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './setup_success_parquet_processed.txt', 'processed_parquet_key/setup_success_parquet_processed.txt')
    s3_parquet_processed_upload_and_log()
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-parquet-store-20202', Prefix='processed_parquet_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert 'processed_parquet_key/parquet_processed_export_completed.txt' in keys

@mock_s3
def test_list_parquet_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_parquet_buckets()
    assert str(errinfo.value) == "ERROR: No buckets found"

@mock_s3
def test_list_parquet_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_parquet_buckets()
    assert str(errinfo.value) == "ERROR: Prefix not found in any bucket"

@mock_s3
def test_setup_unsuccessful_error_message_for_parquet_processed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='processed_parquet_key/')
    with pytest.raises(ValueError) as errinfo:
        s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input()
    assert str(errinfo.value) == "ERROR: Terraform deployment unsuccessful"

@mock_s3
def test_list_pqt_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_parquet_buckets()
    assert str(errinfo.value) == "ERROR: No buckets found"

@mock_s3
def test_list_parquet_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_parquet_buckets()
    assert str(errinfo.value) == "ERROR: Prefix not found in any bucket"

if os.path.exists("parquet_processed_run_number.txt"):
    os.remove("parquet_processed_run_number.txt")
if os.path.exists("parquet_processed_export_completed.txt"):
    os.remove("parquet_processed_export_completed.txt")