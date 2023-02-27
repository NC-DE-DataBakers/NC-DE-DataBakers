from datetime import date, timedelta
import pandas as pd
import logging
import boto3
import glob
import os
import re

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
    if not os.path.isdir('./pqt_tmp'):
      os.makedirs('./pqt_tmp')
  
    #buckets
    bucket_list = s3_list_buckets()
    
    #move csv into processed
    csv_prefix = s3_list_prefix_csv_buckets(bucket_list)
    s3_move_csv_files_to_csv_processed_key_and_delete_from_input(csv_prefix)
    s3_create_csv_processed_completed_txt_file()
    s3_csv_processed_upload_and_log(csv_prefix)
    
    #download csv files from s3
    list_files_to_convert(bucket_list)
    
    #make dimemsions and fact tables
    create_dim_currency()
    create_dim_counterparty()
    create_dim_date()
    create_dim_design()
    create_dim_location()
    create_dim_staff()
    
    create_fact_sales_order()
   
    #convert to parquet
    convert_csv_to_parquet()
    
    #update conversion csv
    update_csv_conversion_file()
    
    #upload to the parquet input bucket
    parquet_prefix = s3_list_prefix_parquet_buckets(bucket_list)
    s3_upload_pqt_files_to_pqt_input_key(parquet_prefix)
    s3_create_pqt_input_completed_txt_file()
    s3_pqt_input_upload_and_log(parquet_prefix)

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

def s3_parquet_prefix_buckets(bucket_list):
  """ 
    Args:
        Not required.

    Returns:
        returns the full specified bucket name
  """
  full_list = bucket_list
  if full_list == []:
      raise ValueError("ERROR: No buckets found")
  for bucket in full_list:
      if "nc-de-databakers-csv-store" in bucket:
          return bucket
  raise ValueError("ERROR: Prefix not found in any bucket")

def list_files_to_convert(bucket_list):
  """ 
    downloads all the files to a temporary local directory
    Args:
        Not required.

    Returns:
        nothing
        
  """
  s3=boto3.client('s3')
  bucket = s3_parquet_prefix_buckets(bucket_list)
  list=s3.list_objects(Bucket=bucket)['Contents']
  for key in list:
    s3.download_file(bucket, key['Key'], f'./tmp/{key["Key"].split("/")[1]}')

def create_dim_counterparty():
  """
  Summary: using pandas, read the counterparty and address csv injested from 
  the totesys DB,  merges the two dataframes on the address_id. creates a new 
  dataframe from the merged data in the schema required.

  Raises:
      ValueError: on KeyError - ERROR: dim_counterparty - "column" does not exist
      ValueError: on FileNotFoundError - missing files or directory
      ValueError: on pd.errors.EmptyDataError - files exist but are empty
      ValueError: on pd.errors.DtypeWarning - column data mismatch
      Exception: Blanket to catch unhandled error for cloudwatch
  """
  try:
    counter_party_df = pd.read_csv('./tmp/counterparty.csv')
    address = pd.read_csv('./tmp/address.csv')
    counter_party_df = counter_party_df.rename(columns={"legal_address_id":"address_id"})
    merged_df = pd.merge(counter_party_df, address, on='address_id')
   
    dim_counterparty = pd.DataFrame(data={'counterparty_id': merged_df['counterparty_id'], 
                                      'counterparty_legal_name': merged_df['counterparty_legal_name'],
                                      'counterparty_legal_address_line_1' : merged_df['address_line_1'], 
                                      'counterparty_legal_address_line_2' : merged_df['address_line_2'], 
                                      'counterparty_legal_district': merged_df['district'],
                                      'counterparty_legal_city':merged_df['city'],
                                      'counterparty_legal_postal_code':merged_df['postal_code'],
                                      'counterparty_legal_country': merged_df['country'],
                                      'counterparty_legal_phone_number': merged_df['phone']}).sort_values(by=['counterparty_id'])

    dim_counterparty.to_csv('./tmp/dim_counterparty.csv', index=False)
  except KeyError as error:
    raise ValueError(f'ERROR: dim_counterparty - {error} does not exist')
  except FileNotFoundError as error:
    raise ValueError(f'ERROR: {error}')
  except pd.errors.EmptyDataError as EDE:
    raise ValueError(f'ERROR: {EDE}')
  except pd.errors.DtypeWarning as DTW:
    raise ValueError(f'ERROR: {DTW}')
  except Exception as error:
    raise ValueError(f'ERROR: {error}')

def create_dim_currency():

  """
  Summary: using pandas, read the currency csv injested from 
  the totesys DB. creates a new dataframe dataframe, changes the shorthand 
  currency code to the full name, formats in the schema required.

  Raises:
      ValueError: on KeyError - ERROR: dim_counterparty - "column" does not exist
      ValueError: on FileNotFoundError - missing files or directory
      ValueError: on pd.errors.EmptyDataError - files exist but are empty
      ValueError: on pd.errors.DtypeWarning - column data mismatch
      Exception: Blanket to catch unhandled error for cloudwatch
  """

  try:
    cur_df = pd.read_csv('./tmp/currency.csv')
  
    dim_currency = pd.DataFrame(data={'currency_id': cur_df['currency_id'], 'currency_code': cur_df['currency_code'], 'currency_name': cur_df['currency_code']})
    dim_currency['currency_name'] = dim_currency['currency_name'].str.replace('GBP', 'Pounds')
    dim_currency['currency_name'] = dim_currency['currency_name'].str.replace('USD', 'Dollars')
    dim_currency['currency_name'] = dim_currency['currency_name'].str.replace('EUR', 'Euros')
  
    dim_currency.to_csv('./tmp/dim_currency.csv', index=False)
  except KeyError as error:
    raise ValueError(f'ERROR: dim_currency - {error} does not exist')
  except FileNotFoundError as error:
    raise ValueError(f'ERROR: {error}')
  except pd.errors.EmptyDataError as EDE:
    raise ValueError(f'ERROR: {EDE}')
  except pd.errors.DtypeWarning as DTW:
    raise ValueError(f'ERROR: {DTW}')
  except Exception as error:
    raise ValueError(f'ERROR: {error}')

def create_dim_date():
  pass

def create_dim_staff():
    """Using pandas to read the staff.csv and department.csv files to merge and create
    the star schema dataframe.
    
    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the staff.csv and department.csv files,
        storing the dim_staff.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_staff - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        staff_df = pd.read_csv('./tmp/staff.csv')
        dept_df = pd.read_csv('./tmp/department.csv')
        staff_dept_df = pd.merge(
            staff_df, dept_df, on='department_id').sort_values('staff_id')
        dim_staff = pd.DataFrame(data={'staff_id': staff_dept_df['staff_id'],
                                    'first_name': staff_dept_df['first_name'],
                                    'last_name': staff_dept_df['last_name'],
                                    'department_name': staff_dept_df['department_name'],
                                    'location': staff_dept_df['location'],
                                    'email_address': staff_dept_df['email_address']
                                    })
        dim_staff.to_csv('./tmp/dim_staff.csv', index=False)
        return dim_staff
    except KeyError as error:
        raise ValueError(f'ERROR: dim_staff - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')


def create_dim_design():
    """Using pandas to read the design.csv file to create the star schema dataframe.
    
    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the staff.csv and department.csv files,
        storing the dim_design.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_design - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        des_df = pd.read_csv('./tmp/design.csv')
        dim_design = pd.DataFrame(data={'design_id': des_df['design_id'],
                                        'design_name': des_df['design_name'],
                                        'file_location': des_df['file_location'],
                                        'file_name': des_df['file_name']})
        dim_design.to_csv('./tmp/dim_design.csv', index=False)
        return dim_design
    except KeyError as error:
        raise ValueError(f'ERROR: dim_design - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

def create_dim_location():
    """Using pandas to read the address.csv file to create the star schema dataframe.
    
    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the address.csv file,
        storing the dim_location.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_location - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        loc_df = pd.read_csv('./tmp/address.csv')
        dim_location = pd.DataFrame(data={'location_id': loc_df['address_id'],
                                        'address_line_1': loc_df['address_line_1'],
                                        'address_line_2': loc_df['address_line_2'],
                                        'district': loc_df['district'],
                                        'city': loc_df['city'],
                                        'postal_code': loc_df['postal_code'], 
                                        'country': loc_df['country'],
                                        'phone': loc_df['phone']})
        dim_location.to_csv('./tmp/dim_location.csv', index=False)
        return dim_location
    except KeyError as error:
        raise ValueError(f'ERROR: dim_location - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

def create_dim_date():
    try:
        df = pd.read_csv('./tmp/sales_order.csv')

        df[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']] = df[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']] .apply(pd.to_datetime, format="%Y-%m-%d %H:%M:%S.%f")
        
        min_dates = []
        max_dates = []

        min_dates.append(df['created_at'].min())
        min_dates.append(df['last_updated'].min())
        min_dates.append(df['agreed_delivery_date'].min())
        min_dates.append(df['agreed_payment_date'].min())

        max_dates.append(df['created_at'].max())
        max_dates.append(df['last_updated'].max())
        max_dates.append(df['agreed_delivery_date'].max())
        max_dates.append(df['agreed_payment_date'].max())

        min_date = min(min_dates)
        max_date = max(max_dates)
        
        dim_date = pd.DataFrame()
        dim_date['date_id'] = [min_date+timedelta(days=x) for x in range(((max_date + timedelta(days=1))-min_date).days)]

        dim_date['year'] = dim_date['date_id'].dt.year
        dim_date['month'] = dim_date['date_id'].dt.month
        dim_date['day'] = dim_date['date_id'].dt.day
        dim_date['day_of_week'] = dim_date['date_id'].dt.day_of_week
        dim_date['day_name'] = dim_date['date_id'].dt.day_name()
        dim_date['month_name'] = dim_date['date_id'].dt.month_name()
        dim_date['quarter'] = dim_date['date_id'].dt.quarter

        dim_date.to_csv('./tmp/dim_date.csv', index=False)

    except KeyError as error:
        raise ValueError(f'ERROR: dim_date - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except AttributeError as Attr:
        raise AttributeError(f'ERROR: {Attr}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

def create_fact_sales_order():
    try:
        fact_sales = pd.read_csv('./tmp/sales_order.csv')

        fact_sales[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']] = fact_sales[['created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date']].apply(pd.to_datetime, format="%Y-%m-%d %H:%M:%S.%f")

        fact_sales['created_time'] = fact_sales['created_at'].dt.time
        fact_sales['last_updated_time'] = fact_sales['last_updated'].dt.time
        fact_sales['created_at'] = fact_sales['created_at'].dt.date
        fact_sales['last_updated'] = fact_sales['last_updated'].dt.date
        fact_sales['agreed_delivery_date'] = fact_sales['agreed_delivery_date'].dt.date
        fact_sales['agreed_payment_date'] = fact_sales['agreed_payment_date'].dt.date

        fact_sales.rename({'created_at': 'created_date', 'last_updated': 'last_updated_date', 'staff_id': 'sales_staff_id'}, axis=1, inplace=True)

        fact_sales = fact_sales[['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']]

        fact_sales.to_csv('./tmp/fact_sales_order.csv', index=False)


    except KeyError as error:
        raise ValueError(f'ERROR: dim_counterparty - {error} does not exist')
    except FileNotFoundError as error:
        raise ValueError(f'ERROR: {error}')
    except pd.errors.EmptyDataError as EDE:
        raise ValueError(f'ERROR: {EDE}')
    except pd.errors.DtypeWarning as DTW:
        raise ValueError(f'ERROR: {DTW}')
    except AttributeError as Attr:
        raise AttributeError(f'ERROR: {Attr}')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

def convert_csv_to_parquet():
  """ 
    locally stored files from "./tmp/dim_*.csv" "./tmp/fact*.csv" are converted to parquet,
    and saved in a pqt_tmp folder
    Args:
        Not required.

    Returns:
        nothing

  """
  
  file_list = os.listdir('./tmp')
  dim_re = re.compile(r'dim_\w+.csv')
  dims = [ file for file in file_list if dim_re.match(file) ]

  fact_re = re.compile(r'fact_\w+.csv')
  facts = [ file for file in file_list if fact_re.match(file) ]

  file_list = dims + facts
  
  if len(file_list) < 1:
    raise ValueError('ERROR: No CSV files to convert to parquet')
  for file in file_list:
    filename = os.path.basename(file).split('.')[0]
    df = pd.read_csv(f'./tmp/{file}')

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


#s3 proccessed


def s3_list_prefix_csv_buckets():
    """Using the s3 client, we will return the name of the s3 bucket containing the "nc-de-databakers-csv-store-" prefix buckets through a for loop of the list returned in s3_list_buckets, we will also add some error handling that checks if the bucket is empty, or if the prefix is not found
    
    Args:
        Not required.

    Returns:
        The name of the correct prefixed bucket
    """
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-csv-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def s3_list_prefix_parquet_buckets(bucket_list):
    """Using the s3 client, we will return the name of the s3 bucket containing the "nc-de-databakers-parquet-store-" prefix buckets through a for loop of the list returned in s3_list_buckets, we will also add some error handling that checks if the bucket is empty, or if the prefix is not found
    
    Args:
        Not required.

    Returns:
        The name of the correct prefixed bucket, ValueError otherwise
    """
    full_list = bucket_list
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-parquet-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def s3_pqt_input_setup_success(parquet_prefix):
    """Using the s3 client and a for loop, we will check if the parquet bucket contains the setup success text file in the input key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
    Args:
        Not required.

    Returns:
        True if file is found, ValueError otherwise.
    """
    s3=boto3.client('s3')
    bucket = parquet_prefix
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if object['Key'] == 'input_parquet_key/setup_success_parquet_input.txt':
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_csv_processed_setup_success(csv_prefix):
    """Using the s3 client and a for loop, we will check if the csv bucket contains the setup success text file in the processed key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
    Args:
        Not required.

    Returns:
        True if file is found, ValueError otherwise.
    """
    s3=boto3.client('s3')
    bucket = csv_prefix
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if object['Key'] == 'processed_csv_key/setup_success_csv_processed.txt':
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_upload_pqt_files_to_pqt_input_key(parquet_prefix):
    """Using the s3 resource and a for loop, we will first check if the csv bucket contains the setup success text file in the processed key, to ensure that terraform has been setup correctly, the convert parquet function would have stored the converted parquets in the pqt_temp locat directory. The for loop will allow us to iterrate throught these files and upload them one by one using the s3 upload file resource. If there is an error, when looking for the directory, a value error will be raised informing the user of a terraform issue.
    
    Args:
        Not required.

    Returns:
        Nothing, unless there is an error.
    """
    s3=boto3.resource('s3')
    if s3_pqt_input_setup_success(parquet_prefix):
        pqt_files = os.listdir('./pqt_tmp')
        for file in pqt_files:
            s3.Bucket(parquet_prefix).upload_file(f'./pqt_tmp/{file}', f'input_parquet_key/{file}')
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_create_pqt_input_completed_txt_file():
    """Using the os module we will check if the local directory contains a run number file. This will happen after the previos upload function is invoked. If this file does not exist, we will pass it in "0", then we will increment it each time this current function is run by "1". At the same time a separate file will read from the run number file and log each time this function is run appending the run number on another line.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    if not os.path.exists("./tmp/pqt_input_run_number.txt"):
        with open("./tmp/pqt_input_run_number.txt", "w") as rn_f:
            rn_f.write("0")
    with open("./tmp/pqt_input_run_number.txt", "r+") as rn_f:
        run_num = int(rn_f.read()) + 1
        rn_f.seek(0)
        rn_f.write(str(run_num))
    with open(f"./tmp/pqt_input_export_completed.txt", "a+") as f:
        f.write(f'run {run_num}\n')


def s3_move_csv_files_to_csv_processed_key_and_delete_from_input(csv_prefix):
    """Using the s3 resource and s3 client we will attempt to move files from the csv bucket input key to the csv buckety processed key. We will check the name of the csv bucket using the s3 list and we will also check that the set-up for the processed key waws successful. If not, it will throw an error.
 
    Args:
        Not required.

    Returns:
        Nothing, unless there is an error.
    """
    s3res=boto3.resource('s3')
    s3cli=boto3.client('s3')
    bucket_name = csv_prefix
    if s3_csv_processed_setup_success(csv_prefix):
        source_key = 'input_csv_key/'
        destination_key = 'processed_csv_key/'
        bucket = s3res.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=source_key)
        for obj in objects:
            if obj.key.endswith('.csv'):
                obj_key = obj.key.replace(source_key, '')
                s3cli.copy(CopySource={'Bucket': bucket_name, 'Key':obj.key}, Bucket=bucket_name, Key=destination_key+obj_key)
                obj.delete()
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_create_csv_processed_completed_txt_file():
    """Using the os module we will check if the local directory contains a run number file. This will happen after the previos upload function is invoked. If this file does not exist, we will pass it in "0", then we will increment it each time this current function is run by "1". At the same time a separate file will read from the run number file and log each time this function is run appending the run number on another line.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    
    if not os.path.exists("./tmp/csv_processed_run_number.txt"):
        with open("./tmp/csv_processed_run_number.txt", "w") as rn_f:
            rn_f.write("0")
    with open("./tmp/csv_processed_run_number.txt", "r+") as rn_f:
        run_num = int(rn_f.read()) + 1
        rn_f.seek(0)
        rn_f.write(str(run_num))
    with open(f"./tmp/csv_processed_export_completed.txt", "a+") as f:
        f.write(f'run {run_num}\n')

def s3_pqt_input_upload_and_log(parquet_prefix):
    """Using the s3 resource we will upload the pqt input completed text file to the input parquet bucket in the s3, after uploading and updating the run count.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """

    s3=boto3.resource('s3')
    s3.Bucket(parquet_prefix).upload_file('./tmp/pqt_input_export_completed.txt', 'input_parquet_key/pqt_input_export_completed.txt')

def s3_csv_processed_upload_and_log(csv_prefix):
    """Using the s3 resource we will upload the csv processed completed text file to the processed csv bucket in the s3, after moving the csv files from the csv input key to the processed csv key and updating the run count.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    
    s3=boto3.resource('s3')
    s3.Bucket(csv_prefix).upload_file('./tmp/csv_processed_export_completed.txt', 'processed_csv_key/csv_processed_export_completed.txt')

