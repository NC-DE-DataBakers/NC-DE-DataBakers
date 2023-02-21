from src.s3_processed_helper import s3_list_buckets, s3_list_prefix_buckets, s3_setup_success, s3_upload_pqt_files, s3_upload_pqt_and_delete_csv_files_from_input_key, create_pqt_completed_txt_file, s3_pqt_upload_and_log
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
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',
                  Key='processed_csv_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-csv-store-20202')
    assert "processed_csv_key" in object_names["Contents"][0]["Key"]

@mock_s3
def test_setup_success_txt_file_exists():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202', Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    assert s3_setup_success() == True

@mock_s3
def test_parquet_files_are_uploaded_successfully_to_the_processed_key_within_bucket():
    s3cli = boto3.client('s3')
    pqt_files = ['address.parquet', 'counterparty.parquet', 'currency.parquet', 'department.parquet', 'design.parquet','payment_type.parquet', 'payment.parquet', 'purchase_order.parquet', 'sales_order.parquet', 'staff.parquet', 'transaction.parquet']
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_upload_pqt_files()
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='processed_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert all(f'processed_csv_key/{file}' in keys for file in pqt_files)

def test_upload_pqt_and_delete_input_csv_files():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_upload_pqt_and_delete_csv_files_from_input_key()
    input_csv_objects = [obj.key for obj in s3res.Bucket('nc-de-databakers-csv-store-20202').objects.filter(Prefix='input_csv_key/')]
    csv_count = sum([1 for obj in input_csv_objects if obj.endswith('.csv')])
    assert csv_count == 0

def test_creates_pqt_export_completed_txt_file_if_files_are_uploaded():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    create_pqt_completed_txt_file()
    assert os.path.isfile('pqt_export_completed.txt')

@mock_s3
def test_logs_for_each_run_by_checking_log_line_count():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_pqt_upload_and_log()
    with open("pqt_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 2
    s3_pqt_upload_and_log()
    with open("pqt_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 3
    s3_pqt_upload_and_log()
    with open("pqt_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 4

@mock_s3
def test_latest_run_num_matches_final_run_in_pqt_export_completed_file():
    with open("pqt_run_number.txt") as file_1, open("pqt_export_completed.txt") as file_2:
        first_line = file_1.readline().strip()
        last_line = file_2.readlines()[-1].strip()
    assert last_line == f"run {first_line}"

@mock_s3
def test_uploads_pqt_export_file_to_key_within_bucket():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_pqt_upload_and_log()
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='processed_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert 'processed_csv_key/pqt_export_completed.txt' in keys

@mock_s3
def test_list_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_buckets()
    assert str(errinfo.value) == "ERROR: No buckets found"

@mock_s3
def test_list_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_buckets()
    assert str(errinfo.value) == "ERROR: Prefix not found in any bucket"

@mock_s3
def test_setup_unsuccessful_error_message():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='processed_csv_key/')
    with pytest.raises(ValueError) as errinfo:
        s3_upload_pqt_files()
    assert str(errinfo.value) == "ERROR: Terraform deployment unsuccessful"

if os.path.exists("pqt_run_number.txt"):
    os.remove("pqt_run_number.txt")
if os.path.exists("pqt_export_completed.txt"):
    os.remove("pqt_export_completed.txt")