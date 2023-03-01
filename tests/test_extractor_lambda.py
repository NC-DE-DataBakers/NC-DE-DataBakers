from src.extractor_lambda import s3_list_buckets, get_csv_store_bucket, s3_setup_success, s3_upload_csv_files, update_csv_export_file
from moto import mock_secretsmanager, mock_s3
import pg8000
import pytest
import boto3
import json
import os

if not os.path.isdir('./tmp'):
    os.makedirs('./tmp')
if not os.path.isdir('./tmp/csv_input'):
    os.makedirs('./tmp/csv_input')
if not os.path.isdir('./tmp/csv_processed'):
    os.makedirs('./tmp/csv_processed')
if not os.path.isdir('./tmp/pqt_input'):
    os.makedirs('./tmp/pqt_input')
if not os.path.isdir('./tmp/pqt_processed'):
    os.makedirs('./tmp/pqt_processed')

#####
#DB conn
#####

sm = boto3.client('secretsmanager')

secret_value = sm.get_secret_value(SecretId="totesys_creds")['SecretString']
parsed_secret = json.loads(secret_value)
host = parsed_secret["host"]
port = parsed_secret["port"]
database = parsed_secret["database"]
user = parsed_secret["username"]
password = parsed_secret["password"]

@mock_secretsmanager
def test_correct_credentials_are_retrieved_from_secrets_manager():
    sm = boto3.client('secretsmanager')
    sm.create_secret(Name='totesys_creds', SecretString='{"host":"fake_host", "port":"fake_port", "database":"fake_database", "user":"fake_user", "password":"fake_password"}')
    
    secret_value = sm.get_secret_value(SecretId="totesys_creds")['SecretString']
    parsed_secret = json.loads(secret_value)
    assert parsed_secret["host"] == "fake_host"
    assert parsed_secret["port"] == "fake_port"
    assert parsed_secret["database"] == "fake_database"
    assert parsed_secret["user"] == "fake_user"
    assert parsed_secret["password"] == "fake_password"

@mock_secretsmanager
def test_raises_error_when_secret_name_does_not_exist():
    sm = boto3.client('secretsmanager')
    with pytest.raises(sm.exceptions.ResourceNotFoundException):
        secret_value = sm.get_secret_value(SecretId="totesys_creds")

def test_connection_user_for_interface_error():
        try:
            conn = pg8000.Connection("user=", "password=password", "host=host", "database=database")
            return conn
        except pg8000.exceptions.InterfaceError as IFE:
            assert f"{IFE}" == "Can't create a connection to host password=password and port database=database (timeout is None and source_address is None)."

def test_db_conn_bad_host_exception():
    host = "badhost"
    with pytest.raises(pg8000.exceptions.InterfaceError):
        pg8000.Connection(user=user, password=password, host=host, database=database)
     
def test_db_conn_bad_user_exception():
    user ='Baduser'
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '28P01'

def test_db_conn_bad_password_exception():
    password ='bad_password'
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '28P01'
     
def test_db_conn_bad_password_exception():
    database='bad_database'
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '3D000'
        
def test_ErrorCode_42703_bad_column_query(): 
    conn = pg8000.Connection(user=user, password=password, host=host, database=database)
    table_query = "SELECT _BADTABLEQUERY_ FROM information_schema.tables WHERE table_schema = \'public\';"
    try:
        table_names = conn.run(table_query)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '42703'
        
def test_ErrorCode_42P01_bad_Relationship_query():
    conn = pg8000.Connection(user=user, password=password, host=host, database=database)
    table_query = "SELECT table_name FROM BADTABLE.tables WHERE table_schema = \'public\';"
    try:
        table_names = conn.run(table_query)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '42P01'


#####
#S3 Helpers
#####


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
    bucket_names = get_csv_store_bucket()
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
    if not os.path.isdir('./tmp'):
        os.makedirs('./tmp')
    open('./tmp/setup_success_csv_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    input_bucket = get_csv_store_bucket()
    assert s3_setup_success(input_bucket) == True
    files = os.listdir('./tmp/csv_input')
    for file in files:
        os.remove(f'./tmp/csv_input/{file}')
    

@mock_s3
def test_csv_files_are_uploaded_successfully_to_the_input_key_within_bucket():
    s3cli = boto3.client('s3')

    if not os.path.isdir('./tmp'):
        os.mkdir('./tmp')
    
    open('./tmp/csv_input/address.csv', 'w')
    open('./tmp/csv_input/counterparty.csv', 'w')
    open('./tmp/csv_input/currency.csv', 'w')


    csv_files = ['address.csv', 'counterparty.csv', 'currency.csv']
    #, 'department.csv', 'design.csv','payment_type.csv', 'payment.csv', 'purchase_order.csv', 'sales_order.csv', 'staff.csv', 'transaction.csv'
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')

    s3res = boto3.resource('s3')
    if not os.path.isdir('./tmp'):
        os.makedirs('./tmp')
    open('./tmp/setup_success_csv_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    input_bucket = get_csv_store_bucket()
    s3_upload_csv_files(input_bucket)
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='input_csv_key/')['Contents']
    
    keys = [object['Key'] for object in objects]
    assert all(f'input_csv_key/{file}' in keys for file in csv_files)

    files = os.listdir('./tmp/csv_input')
    for file in files:
        os.remove(f'./tmp/csv_input/{file}')
    
    assert os.path.isfile("./tmp/csv_input/address.csv'") is False
    assert os.path.isfile("./tmp/csv_input/counterparty.csv") is False
    assert os.path.isfile("./tmp/csv_input/currency.csv") is False
    

@mock_s3
def test_creates_csv_export_completed_txt_file_if_files_are_uploaded():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3res = boto3.resource('s3')
    if not os.path.isdir('./tmp'):
        os.makedirs('./tmp')
    with open('./tmp/csv_export.txt', 'wb') as file:
        file.write(b'Run 0')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/csv_export.txt', 'input_csv_key/csv_export.txt')
    input_bucket = get_csv_store_bucket()
    update_csv_export_file(input_bucket)
    assert os.path.isfile('./tmp/csv_export.txt')
    
    files = os.listdir('./tmp/csv_input')
    for file in files:
        os.remove(f'./tmp/csv_input/{file}')


"""Before running the test_logs_for_each_run_by_checking_log_line_count test, ensure the csv_export_completed.txt does not already exist.
"""

@mock_s3
def test_logs_for_each_run_by_checking_log_line_count():
    s3cli = boto3.client('s3')
    bucket = 'nc-de-databakers-csv-store-20202'
    s3cli.create_bucket(Bucket=bucket)
    if not os.path.isdir('./tmp'):
        os.makedirs('./tmp')
    with open('./tmp/csv_export.txt', 'w') as file:
        file.write('Run 0')
    s3cli.upload_file("./tmp/csv_export.txt", bucket, "input_csv_key/csv_export.txt")
    input_bucket = get_csv_store_bucket()
    update_csv_export_file(input_bucket)
    with open("./tmp/csv_export.txt", 'r') as f:
        assert len(f.readlines()) == 1
    update_csv_export_file(input_bucket)
    with open("./tmp/csv_export.txt", 'r') as f:
        assert len(f.readlines()) == 1
    update_csv_export_file(input_bucket)
    with open("./tmp/csv_export.txt", 'r') as f:
        assert len(f.readlines()) == 1

    files = os.listdir('./tmp/csv_input')
    for file in files:
        os.remove(f'./tmp/csv_input/{file}')

@mock_s3
def test_uploads_csv_export_file_to_key_within_bucket():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='input_csv_key/')
    s3res = boto3.resource('s3')
    if not os.path.isdir('./tmp/csv_input'):
        os.makedirs('./tmp/csv_input')
    open('./tmp/csv_input/setup_success_csv_input.txt', 'w')
    open('./tmp/csv_input/csv_export.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/csv_input/setup_success_csv_input.txt', 'input_csv_key/setup_success_csv_input.txt')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/csv_input/csv_export.txt', 'input_csv_key/csv_export.txt')
    input_bucket = get_csv_store_bucket()
    s3_upload_csv_files(input_bucket)
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='input_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert 'input_csv_key/csv_export.txt' in keys

    files = os.listdir('./tmp/csv_input')
    for file in files:
        os.remove(f'./tmp/csv_input/{file}')


@mock_s3
def test_list_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        get_csv_store_bucket()
    assert str(errinfo.value) == "ERROR: No buckets found"

@mock_s3
def test_list_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        get_csv_store_bucket()
    assert str(errinfo.value) == "ERROR: Prefix not found in any bucket"

@mock_s3
def test_setup_unsuccessful_error_message():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='input_csv_key/')
    input_bucket = get_csv_store_bucket()
    with pytest.raises(ValueError) as errinfo:
        s3_upload_csv_files(input_bucket)
    assert str(errinfo.value) == "ERROR: Terraform deployment unsuccessful"

if os.path.exists("run_number.txt"):
    os.remove("run_number.txt")
if os.path.exists("csv_export_completed.txt"):
    os.remove("csv_export_completed.txt")

