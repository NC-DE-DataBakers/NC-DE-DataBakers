from src.stage_2_lambda import s3_list_prefix_parquet_buckets, s3_pqt_input_setup_success, s3_csv_processed_setup_success, s3_pqt_input_upload_and_log, s3_create_pqt_input_completed_txt_file, s3_upload_pqt_files_to_pqt_input_key
from src.stage_2_lambda import s3_list_prefix_csv_buckets, s3_move_csv_files_to_csv_processed_key_and_delete_from_input, s3_create_csv_processed_completed_txt_file, s3_csv_processed_upload_and_log
from src.stage_2_lambda import convert_csv_to_parquet, list_files_to_convert, update_csv_conversion_file, s3_list_buckets
from src.stage_2_lambda import create_dim_counterparty, create_dim_currency, create_dim_staff, create_dim_design, create_dim_location
from unittest.mock import patch
from moto import mock_s3
import pandas as pd
import pytest
import boto3
import glob
import csv
import os

if not os.path.isdir('./tmp'):
    os.makedirs('./tmp')
"""

  csv_to_parquet testing
  
"""
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
  with patch ('src.stage_2_lambda.s3_parquet_prefix_buckets', return_value="parquet-test-bucket"):
    open('./tmp/mocks3testfile.txt', 'w')
    s3.put_object(Body="./tmp/mocks3testfile.txt", Bucket="parquet-test-bucket", Key="mocks3testfile.txt")
    
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
  
  with open('./tmp/dim_mockconversion1.csv', 'w') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(fields)
    csv_writer.writerows(rows)
    
  reader = csv.reader(open("./tmp/dim_mockconversion1.csv"))
  # for row in reader:
  #     print(row)
  convert_csv_to_parquet()
  
  assert os.path.isfile("./pqt_tmp/dim_mockconversion1.parquet")
  os.remove('./pqt_tmp/dim_mockconversion1.parquet')
  os.removedirs('./pqt_tmp')
  
  assert os.path.isfile("./pqt_tmp/dim_mockconversion1.parquet") is False
  assert os.path.isdir('./pqt_tmp') is False
  
def test_files_are_converted_to_parquet():
  open('./tmp/dim_mockconversion1.csv', 'w')
  with pytest.raises(pd.errors.EmptyDataError):
    convert_csv_to_parquet()
  os.remove('./tmp/dim_mockconversion1.csv')
  
  assert os.path.isfile("./tmp/dim_mockconversion1.csv") is False

def test_ValueError_raised_for_empty_tmp_folder():
  files = os.listdir('./tmp')
  for file in files:
      os.remove(f'./tmp/{file}')
  with pytest.raises(ValueError):
    convert_csv_to_parquet()

@mock_s3   
def test_csv_conversion_file_downloaded():
  s3 = boto3.client("s3")
  bucket = 'nc-de-databakers-csv-store-1010'
  s3.create_bucket(Bucket=bucket)
  with open('./tmp/csv_conversion.txt', 'w') as file:
    file.write('Run 0')
  s3.upload_file("./tmp/csv_conversion.txt", bucket, "processed_csv_key/csv_conversion.txt")
  with patch ('src.stage_2_lambda.s3_list_buckets', return_value=[bucket]):
    update_csv_conversion_file()

  assert os.path.isfile("./tmp/csv_conversion.txt")
  os.remove('./tmp/csv_conversion.txt')
  assert os.path.isfile("./tmp/csv_conversion.txt") is False

@mock_s3
def test_csv_conversion_run_number_incremented():
  s3 = boto3.client("s3")
  bucket = 'nc-de-databakers-csv-store-1010'
  s3.create_bucket(Bucket=bucket)
  with open('./tmp/csv_conversion.txt', 'w') as file:
    file.write('Run 0')
  s3.upload_file("./tmp/csv_conversion.txt", bucket, "processed_csv_key/csv_conversion.txt")
  with patch ('src.stage_2_lambda.s3_list_buckets', return_value=[bucket]):
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

def test_dim_fact_converted_only():
  
  dim_a = pd.DataFrame(data={'a': [1], 'b': [2]})
  dim_a.to_csv('./tmp/dim_a.csv', index=False)
  fact_a = pd.DataFrame(data={'c': [2], 'd': [4]})
  fact_a.to_csv('./tmp/fact_a.csv', index=False)

  open('./tmp/address.csv', 'w')
  open('./tmp/date.csv', 'w')

  convert_csv_to_parquet()
  
  assert os.path.isfile("./pqt_tmp/dim_a.parquet")
  assert os.path.isfile("./pqt_tmp/fact_a.parquet")
  assert os.path.isfile("./pqt_tmp/address.parquet") is False
  assert os.path.isfile("./pqt_tmp/date.parquet") is False
  files = os.listdir('./pqt_tmp')
  for file in files:
          os.remove(f'./pqt_tmp/{file}')
  os.removedirs('./pqt_tmp')

  
"""
  dim_counterparty testing
"""

if not os.path.isdir('./tmp'):
      os.makedirs('./tmp')

def test_dataframe_contents_are_not_mutated():
  d = pd.DataFrame(data={'currency_id' : [1,2,3], 'currency_code': ['GBP','USD','EUR']})
  d.to_csv('./tmp/test_1.csv', index=False)

  converted = pd.read_csv('./tmp/test_1.csv')

  os.remove('./tmp/test_1.csv')
  pd.testing.assert_frame_equal(d, converted)

def test_emptyfile_raises_EmptyDataError():
  d = pd.DataFrame()
  d.to_csv('./tmp/EDEerrortest.csv', index=False)
  
  with pytest.raises(pd.errors.EmptyDataError):
    pd.read_csv('./tmp/EDEerrortest.csv')
  os.remove('./tmp/EDEerrortest.csv')

def test_missing_columns_raises_key_error_counterparty():
  d = pd.DataFrame(data={'counterparty_id' : [1,2,3], 'legal_address_id':[1,2,3]})
  d.to_csv('./tmp/counterparty.csv', index=False)
  a = pd.DataFrame(data={'address_id' : [1,2,3], 'address_line_1':[1,2,3]})
  a.to_csv('./tmp/address.csv', index=False)
  
  with pytest.raises(ValueError):
    create_dim_counterparty()
  try:
      create_dim_counterparty()
  except Exception as e:
    assert e.args[0]== "ERROR: dim_counterparty - 'counterparty_legal_name' does not exist"

def test_converted_file_has_correct_column_names_counterparty():
  d = pd.DataFrame(data={'counterparty_id' : [1,2,3], 'counterparty_legal_name': [2,3,4], 
                         'legal_address_id': [5,6,7], 'commercial_contact': [8,9,10],
                         'delivery_contact' : [1,2,3]})
  d.to_csv('./tmp/counterparty.csv', index=False)
  
  d = pd.DataFrame(data={'address_id' : [1,2,3], 'address_line_1': [2,3,4], 
                        'address_line_2': [5,6,7], 'district':[8,9,10],
                        'city':[1,2,3], 'postal_code':[4,5,6],
                        'country': [7,8,9], 'phone':[1,2,3]})
  d.to_csv('./tmp/address.csv', index=False)

  create_dim_counterparty()

  converted = pd.read_csv('./tmp/dim_counterparty.csv')

  columns = ['counterparty_id', 'counterparty_legal_name', 
             'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 
             'counterparty_legal_district', 'counterparty_legal_city',
             'counterparty_legal_postal_code', 'counterparty_legal_country',
             'counterparty_legal_phone_number']
  
  for name in columns:
    assert name in converted.columns

def test_converted_file_has_correctly_joined_addresses_on_address_id_counterparty():
  d = pd.DataFrame(data={'counterparty_id' : [1,2], 'counterparty_legal_name': ['Fahey and Son','Leannon'], 
                         'legal_address_id': [15,28], 'commercial_contact': [8,9],
                         'delivery_contact' : [1,2]})
  d.to_csv('./tmp/counterparty.csv', index=False)
  
  d = pd.DataFrame(data={'address_id' : [1,2,15,28], 'address_line_1': ['6826 Herzog Via','179 Alexie Cliffs', '605 Haskell Trafficway', '079 Horacio Landing'], 
                        'address_line_2': ['','','Axel Freeway',''], 'district':[28441,99305-7380,10,1],
                        'city':[1,2,3,4], 'postal_code':[4,5,6,7],
                        'country': [7,8,9,10], 'phone':[1,2,3,4]})
  d.to_csv('./tmp/address.csv', index=False)

  create_dim_counterparty()

  converted = pd.read_csv('./tmp/dim_counterparty.csv')
  
  assert converted['counterparty_id'][0] == 1
  assert converted['counterparty_legal_address_line_1'][0] == '605 Haskell Trafficway'
  assert converted['counterparty_legal_address_line_2'][0] == 'Axel Freeway'
    
  assert converted['counterparty_id'][1] == 2
  assert converted['counterparty_legal_address_line_1'][1] == '079 Horacio Landing'

def test_converted_file_has_correct_column_names_currency():
  d = pd.DataFrame(data={'currency_id' : [1,2,3], 'currency_code': ['GBP','USD','EUR']})
  d.to_csv('./tmp/currency.csv', index=False)

  create_dim_currency()

  converted = pd.read_csv('./tmp/dim_currency.csv')

  columns = ['currency_id', 'currency_code', 'currency_name']
  
  for name in columns:
    assert name in converted.columns

def test_missing_columns_raises_key_error_currency():
  d = pd.DataFrame(data={'currency_id' : [1,2,3]})
  d.to_csv('./tmp/currency.csv', index=False)
  
  with pytest.raises(ValueError):
    create_dim_currency()
  try:
      create_dim_currency()
  except Exception as e:
    assert e.args[0]== "ERROR: dim_currency - 'currency_code' does not exist"


""" 
TESTS FOR DIM_DATE<DESING<LOCATION<STAFF
"""

def test_dim_staff_star_schema_contains_correct_column():
    correct_column_names = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    staff = pd.DataFrame(data={'staff_id':[], 'first_name':[], 'last_name':[], 'department_name':[], 'location':[], 'email_address':[], 'department_id':[]})
    staff.to_csv('./tmp/staff.csv')
    
    department_test_table = pd.DataFrame(data={'department_id': [] })
    department_test_table.to_csv('./tmp/department.csv', index=False)
    
    dim_staff_table = create_dim_staff()
    staff_columns = [column for column in dim_staff_table]
    assert staff_columns == correct_column_names


def test_staff_department_id_correlates_with_department_name():
    staff_test_table = pd.DataFrame(data={  'staff_id': [1, 2, 3],
                                            'first_name': ['Jeremie', 'Deron', 'Jeanette'],
                                            'last_name': ['Franey', 'Beier', 'Erdman'],
                                            'department_id': [2, 6, 6],
                                            'email_address': ['jeremie.franey@terrifictotes.com', 'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com']})
    staff_test_table.to_csv('./tmp/staff_test_table.csv', index=False)

    dim_staff_test_table = pd.DataFrame(data={  'staff_id': [1, 2, 3],
                                                'first_name': ['Jeremie', 'Deron', 'Jeanette'],
                                                'last_name': ['Franey', 'Beier', 'Erdman'],
                                                'department_name': ['Purchasing', 'Facilities', 'Facilities'],
                                                'location': ['Manchester', 'Manchester', 'Manchester'],
                                                'email_address': ['jeremie.franey@terrifictotes.com', 'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com']})
    dim_staff_test_table.to_csv('./tmp/dim_staff_test_table.csv', index=False)

    department_look_up = {
        'Sales': 1,
        'Purchasing': 2,
        'Production': 3,
        'Dispatch':	4,
        'Finance': 5,
        'Facilities': 6,
        'Communications': 7,
        'HR': 8
    }

    with open('./tmp/staff_test_table.csv', 'r', encoding='utf-8') as csv_file:
        csv_data = csv.reader(csv_file)
        csv_list = [data for data in csv_data]

    with open('./tmp/dim_staff_test_table.csv', 'r', encoding='utf-8') as dim_staff_file:
        dim_staff_data = csv.reader(dim_staff_file)
        dim_staff_list = [data for data in dim_staff_data]

    for index in range(1, len(dim_staff_list)):
        compare_value = department_look_up[dim_staff_list[index][3]]
        assert compare_value == int(csv_list[index][3])

def test_dim_design_star_schema_contains_correct_column():
    correct_column_names = ['design_id', 'design_name', 'file_location', 'file_name']
    design = pd.DataFrame(data={'design_id':[], 'design_name':[], 'file_location':[], 'file_name':[]})
    design.to_csv('./tmp/design.csv')
    dim_design_table = create_dim_design()
    design_columns = [column for column in dim_design_table]
    assert design_columns == correct_column_names
    
def test_dim_location_star_schema_contains_correct_column():
    correct_column_names = ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    dim_location_table = create_dim_location()
    location_columns = [column for column in dim_location_table]
    assert location_columns == correct_column_names

def test_dim_staff_missing_columns_raises_value_error():
    staff_test_table = pd.DataFrame(data={  'staff_id': [1, 2, 3],
                                            'first_name': ['Jeremie', 'Deron', 'Jeanette'],
                                            'last_name': ['Franey', 'Beier', 'Erdman'],
                                            'department_id': [2, 6, 6]})
    staff_test_table.to_csv('./tmp/staff.csv', index=False)
    
    department_test_table = pd.DataFrame(data={'department_id': [2,6,6], 'department_name': ['man', 'dog', 'car'], 'location':['manchester', 'leeds', 'london']})
    department_test_table.to_csv('./tmp/department.csv', index=False)
    with pytest.raises(ValueError):
        create_dim_staff()
    try:
        create_dim_staff()
    except Exception as e:
        assert e.args[0]== "ERROR: dim_staff - 'email_address' does not exist"

def test_dim_design_missing_columns_raises_value_error():
    design_test_table = pd.DataFrame(data={ 'design_id': [1, 5, 8],
                                            'design_name': ['Wooden', 'Granite', 'Wooden'],
                                            'file_location': ['/home/user/dir', '/root', '/usr']})
    design_test_table.to_csv('./tmp/design.csv', index=False)
  
    with pytest.raises(ValueError):
        create_dim_design()
    try:
        create_dim_design()
    except Exception as e:
        assert e.args[0]== "ERROR: dim_design - 'file_name' does not exist"

def test_dim_location_missing_columns_raises_value_error():
    location_test_table = pd.DataFrame(data={   'address_id': [1, 2, 3],
                                                'address_line_1': ['6826 Herzog Via', '179 Alexie Cliffs', '148 Sincere Fort'],
                                                'district': ['Avon', '', ''],
                                                'city': ['New Patienceburgh', 'Aliso Viejo', 'Lake Charles'],
                                                'postal_code': ['28441', '99305-7380', '89360'],
                                                'country': ['Turkey', 'San Marino', 'Samoa'],
                                                'phone': ['1803 637401', '9621 880720', '0730 783349']})
    location_test_table.to_csv('./tmp/address.csv', index=False)
  
    with pytest.raises(ValueError):
        create_dim_location()
    try:
        create_dim_location()
    except Exception as e:
        assert e.args[0]== "ERROR: dim_location - 'address_line_2' does not exist"




"""
TESTS FOR s3_fie_transfers
"""

if not os.path.isdir('./tmp'):
    os.makedirs('./tmp')
if not os.path.isdir('./pqt_tmp'):
      os.makedirs('./pqt_tmp')

open('./tmp/setup_success_csv_processed.txt', 'w')
open('./tmp/setup_success_csv_input.txt', 'w')
open('./tmp/setup_success_parquet_input.txt', 'w')
open('./tmp/csv_processed_export_completed.txt', 'w')

@mock_s3
def test_bucket_list_for_csv_prefix():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.create_bucket(Bucket='my-test-bucket')
    bucket_names = s3_list_prefix_csv_buckets()
    assert bucket_names == 'nc-de-databakers-csv-store-20202'

@mock_s3
def test_bucket_list_for_parquet_prefix():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.create_bucket(Bucket='my-test-bucket')
    bucket_list = s3_list_buckets()
    bucket_names = s3_list_prefix_parquet_buckets(bucket_list)
    assert bucket_names == 'nc-de-databakers-parquet-store-20202'

@mock_s3
def test_csv_bucket_for_processed_key_if_bucket_is_prefixed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',
                  Key='processed_csv_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-csv-store-20202')
    assert "processed_csv_key" in object_names["Contents"][0]["Key"]

@mock_s3
def test_parquet_bucket_for_input_key_if_bucket_is_prefixed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                  Key='input_parquet_key/')
    object_names = s3.list_objects(Bucket='nc-de-databakers-parquet-store-20202')
    assert "input_parquet_key" in object_names["Contents"][0]["Key"]

@mock_s3
def test_setup_success_txt_file_exists_pqt_input():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202', Key='input_parquet_key/')
    s3res = boto3.resource('s3')
    open('./tmp/setup_success_parquet_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './tmp/setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    assert s3_pqt_input_setup_success('nc-de-databakers-parquet-store-20202') == True

@mock_s3
def test_setup_success_txt_file_exists_csv_processed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202', Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    assert s3_csv_processed_setup_success('nc-de-databakers-csv-store-20202') == True

@mock_s3
def test_parquet_files_are_uploaded_successfully_to_the_input_key_within_pqt_bucket():
    if not os.path.isdir('./pqt_tmp'):
      os.makedirs('./pqt_tmp')
    if not os.path.isdir('./tmp'):
      os.makedirs('./tmp')
    s3cli = boto3.client('s3')
    pqt_files = ['dim_date.parquet', 'dim_location.parquet', 'dim_design.parquet', 'dim_currency.parquet', 'dim_counterparty', 'dim_staff.parquet', 'fact_sales_order.parquet' ]
    for file in pqt_files:
      open(f'./pqt_tmp/{file}', 'w')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='input_parquet_key/')
    s3res = boto3.resource('s3')
    open('./tmp/setup_success_parquet_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './tmp/setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    s3_upload_pqt_files_to_pqt_input_key('nc-de-databakers-parquet-store-20202')
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-parquet-store-20202', Prefix='input_parquet_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert all(f'input_parquet_key/{file}' in keys for file in pqt_files)

@mock_s3
def test_creates_pqt_input_export_completed_txt_file_if_files_are_uploaded():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='input_parquet_key/')
    open('./tmp/setup_success_parquet_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './tmp/setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    s3_create_pqt_input_completed_txt_file()
    assert os.path.isfile('./tmp/pqt_input_export_completed.txt')

@mock_s3
def test_logs_for_each_pqt_input_run_by_checking_log_line_count():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='input_parquet_key/')
    open('./tmp/setup_success_parquet_input.txt', 'w')
    open('./tmp/pqt_input_export_completed.txt', 'w')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './tmp/setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    s3_create_pqt_input_completed_txt_file()
    s3_pqt_input_upload_and_log('nc-de-databakers-parquet-store-20202')
    with open("./tmp/pqt_input_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 1

@mock_s3
def test_csv_files_are_uploaded_successfully_to_the_processed_key_within_bucket():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    bucket_name= 'nc-de-databakers-csv-store-20202'
    key='input_csv_key'
    csv_file_path="./tmp"
    csv_files = ['address.csv', 'counterparty.csv', 'currency.csv', 'department.csv', 'design.csv','payment_type.csv', 'payment.csv', 'purchase_order.csv', 'sales_order.csv', 'staff.csv', 'transaction.csv']
    for file in csv_files:
      open(f'./tmp/{file}', 'w')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='input_csv_key')
    open('./tmp/setup_success_csv_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_input.txt', 'processed_csv_key/setup_success_csv_input.txt')
    for csv in os.listdir(csv_file_path):
        csv_file = os.path.join(csv_file_path, csv)
        s3cli.upload_file(csv_file, bucket_name, f"{key}/{csv}")
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='processed_csv_key/')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_move_csv_files_to_csv_processed_key_and_delete_from_input('nc-de-databakers-csv-store-20202')
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='processed_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert all(f'processed_csv_key/{file}' in keys for file in csv_files)

@mock_s3
def test_upload_csv_to_processed_csv_and_delete_input_csv_files():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    bucket_name= 'nc-de-databakers-csv-store-20202'
    key='input_csv_key'
    csv_file_path="./tmp"
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='input_csv_key')
    open('./tmp/setup_success_csv_input.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_input.txt', 'processed_csv_key/setup_success_csv_input.txt')
    for csv in os.listdir(csv_file_path):
        csv_file = os.path.join(csv_file_path, csv)
        s3cli.upload_file(csv_file, bucket_name, f"{key}/{csv}")
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='processed_csv_key/')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_move_csv_files_to_csv_processed_key_and_delete_from_input('nc-de-databakers-csv-store-20202')
    input_csv_objects = [obj.key for obj in s3res.Bucket('nc-de-databakers-csv-store-20202').objects.filter(Prefix='input_csv_key/')]
    csv_count = sum([1 for obj in input_csv_objects if obj.endswith('.csv')])
    assert csv_count == 0

@mock_s3
def test_creates_csv_processed_export_completed_txt_file_if_files_are_uploaded():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3_create_csv_processed_completed_txt_file()
    assert os.path.isfile('./tmp/csv_processed_export_completed.txt')

@mock_s3
def test_logs_for_each_csv_processed_run_by_checking_log_line_count():
    s3cli = boto3.client('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res = boto3.resource('s3')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    open('./tmp/csv_processed_export_completed.txt', 'w')
    s3_create_csv_processed_completed_txt_file()
    s3_csv_processed_upload_and_log('nc-de-databakers-csv-store-20202')
    with open("./tmp/csv_processed_export_completed.txt", 'r') as f:
        assert len(f.readlines()) == 1

@mock_s3
def test_csv_processed_latest_run_num_matches_final_run_in_csv_processed_export_completed_file():
    with open("csv_processed_run_number.txt") as file_1, open("csv_processed_export_completed.txt") as file_2:
        first_line = file_1.readline().strip()
        last_line = file_2.readlines()[-1].strip()
    assert last_line == f"run {first_line}"

@mock_s3
def test_uploads_csv_processed_export_file_to_key_within_bucket():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    open('./tmp/csv_processed_export_completed.txt', 'w')
    s3_csv_processed_upload_and_log('nc-de-databakers-csv-store-20202')
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-csv-store-20202', Prefix='processed_csv_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert 'processed_csv_key/csv_processed_export_completed.txt' in keys

@mock_s3
def test_csv_processed_latest_run_num_matches_final_run_in_csv_processed_export_completed_file():
    s3_create_csv_processed_completed_txt_file()
    with open("./tmp/csv_processed_run_number.txt") as file_1, open("./tmp/csv_processed_export_completed.txt") as file_2:
        first_line = file_1.readline().strip()
        last_line = file_2.readlines()[-1].strip()
    assert last_line == f"run {first_line}"

@mock_s3
def test_uploads_pqt_input_export_file_to_key_within_bucket():
    s3cli = boto3.client('s3')
    s3res = boto3.resource('s3')
    open('./tmp/setup_success_csv_processed.txt', 'w')
    open('./tmp/setup_success_parquet_input.txt', 'w')
    s3cli.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-csv-store-20202',
                     Key='processed_csv_key/')
    s3res.Bucket('nc-de-databakers-csv-store-20202').upload_file(
        './tmp/setup_success_csv_processed.txt', 'processed_csv_key/setup_success_csv_processed.txt')
    s3cli.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3cli.put_object(Bucket='nc-de-databakers-parquet-store-20202',
                     Key='input_parquet_key/')
    s3res.Bucket('nc-de-databakers-parquet-store-20202').upload_file(
        './tmp/setup_success_parquet_input.txt', 'input_parquet_key/setup_success_parquet_input.txt')
    open('./tmp/pqt_input_export_completed.txt', 'w')
    s3_pqt_input_upload_and_log('nc-de-databakers-parquet-store-20202')
    objects = s3cli.list_objects(
        Bucket='nc-de-databakers-parquet-store-20202', Prefix='input_parquet_key/')['Contents']
    keys = [object['Key'] for object in objects]
    assert 'input_parquet_key/pqt_input_export_completed.txt' in keys

@mock_s3
def test_list_csv_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_csv_buckets()
    assert str(errinfo.value) == "ERROR: No buckets found"

@mock_s3
def test_list_csv_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_csv_buckets()
    assert str(errinfo.value) == "ERROR: Prefix not found in any bucket"

@mock_s3
def test_setup_unsuccessful_error_message_for_csv_processed():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-csv-store-20202')
    s3.put_object(Bucket='nc-de-databakers-csv-store-20202',Key='processed_csv_key/')
    with pytest.raises(ValueError) as errinfo:
        s3_move_csv_files_to_csv_processed_key_and_delete_from_input('nc-de-databakers-csv-store-20202')
    assert str(errinfo.value) == "ERROR: Terraform deployment unsuccessful"

@mock_s3
def test_list_pqt_prefix_buckets_raises_exception_when_there_are_no_buckets():
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_parquet_buckets([])
    assert str(errinfo.value) == "ERROR: No buckets found"

@mock_s3
def test_list_parquet_prefix_buckets_raises_exception_when_there_are_no_prefix_buckets():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test_bucket_val_error_707352')
    with pytest.raises(ValueError) as errinfo:
        s3_list_prefix_parquet_buckets('test_bucket_val_error')
    assert str(errinfo.value) == "ERROR: Prefix not found in any bucket"

@mock_s3
def test_setup_unsuccessful_error_message_for_parquet_input():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='nc-de-databakers-parquet-store-20202')
    s3.put_object(Bucket='nc-de-databakers-parquet-store-20202',Key='input_csv_key/')
    with pytest.raises(ValueError) as errinfo:
        s3_upload_pqt_files_to_pqt_input_key('nc-de-databakers-parquet-store-20202')
    assert str(errinfo.value) == "ERROR: Terraform deployment unsuccessful"

def test_cleanup():
  files = os.listdir('./tmp')
  for file in files:
      os.remove(f'./tmp/{file}')
  os.removedirs('./tmp')
  files = os.listdir('./pqt_tmp')
  for file in files:
      os.remove(f'./pqt_tmp/{file}')
  os.removedirs('./pqt_tmp')
