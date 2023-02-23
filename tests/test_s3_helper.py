from src.s3_helper import s3_list_buckets, s3_list_prefix_buckets, s3_setup_success, s3_upload_csv_files, create_csv_completed_text_file, s3_upload_and_local_log
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
                  Key='input_csv_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-csv-store-20202')
    assert "input_csv_key" in object_names["Contents"][0]["Key"]


@mock_s3
def test_setup_success_txt_file_exists():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',
                  Key='input_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    assert s3_setup_success() == True


@mock_s3
def test_csv_files_are_uploaded_successfully_to_the_input_key_within_bucket():
    s3cli = boto3.client('s3')
    csv_files = ['address.csv', 'counterparty.csv', 'currency.csv', 'department.csv', 'design.csv',
                 'payment_type.csv', 'payment.csv', 'purchase_order.csv', 'sales_order.csv', 'staff.csv', 'transaction.csv']
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    s3_upload_csv_files()
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='input_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert all(f'input_csv_key/{file}' in keys for file in csv_files)


@mock_s3
def test_creates_csv_export_completed_txt_file_if_files_are_uploaded():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    create_csv_completed_text_file()
    assert os.path.isfile('csv_export_completed.txt')



@mock_s3
def test_logs_for_each_run_by_checking_log_line_count():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    s3_upload_and_local_log()
    with open("csv_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 2
    s3_upload_and_local_log()
    with open("csv_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 3
    s3_upload_and_local_log()
    with open("csv_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 4

@mock_s3
def test_latest_run_num_matches_final_run_in_csv_export_completed_file():
    with open("run_number.txt") as file_1, open("csv_export_completed.txt") as file_2:
        first_line = file_1.readline().strip()
        last_line = file_2.readlines()[-1].strip()
    assert last_line == f"run {first_line}"

@mock_s3
def test_uploads_csv_export_file_to_key_within_bucket():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3res = boto3.resource('s3')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    s3_upload_and_local_log()
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='input_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert 'input_csv_key/csv_export_completed.txt' in keys


@mock_s3
def test_list_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_buckets()
    assert str(errinfo.value) == "No buckets found"


@mock_s3
def test_list_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_buckets()
    assert str(errinfo.value) == "Prefix not found in any bucket"


@mock_s3
def test_setup_unsuccessful_error_message():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='input_csv_key/')
    with pytest.raises(ValueError) as errinfo:
        s3_upload_csv_files()
    assert str(errinfo.value) == "Terraform deployment unsuccessful"

if os.path.exists("run_number.txt"):
    os.remove("run_number.txt")
if os.path.exists("csv_export_completed.txt"):
    os.remove("csv_export_completed.txt")
