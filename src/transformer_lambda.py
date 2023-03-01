"""This file contains lambda handler 2, transfers the csv files from
input key to processed key, also converts the raw data to star schema.
This also converts the star schema data to parquet and uploads to the
parquet input bucket.

The file is run by calling this module with the python keyword.
Example:
    python src/stage_2_lambda.py
To run the test file, please use the below:
    pytest tests/test_stage_2_lambda.py
"""

from datetime import timedelta
import pandas as pd
import logging
import boto3
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
        create_dirs()

        # buckets
        bucket_list = s3_list_buckets()

        # move csv into processed
        csv_prefix = s3_list_prefix_csv_buckets(bucket_list)
        s3_move_csv_files_to_csv_processed_key_and_delete_from_input(
            csv_prefix)

        # download csv files from s3
        list_files_to_convert(bucket_list)

        # make dimemsions and fact tables
        create_dim_currency()
        create_dim_counterparty()
        create_dim_date()
        create_dim_design()
        create_dim_location()
        create_dim_staff()
        create_fact_sales_order()

        # convert to parquet
        convert_csv_to_parquet()

        # upload to the parquet input bucket
        parquet_prefix = s3_list_prefix_parquet_buckets(bucket_list)
        s3_upload_pqt_files_to_pqt_input_key(parquet_prefix)

        # update the run number
        s3_create_csv_processed_completed_txt_file(csv_prefix)
        s3_create_pqt_input_completed_txt_file(parquet_prefix)

        clean_tmp()

    except Exception as error:
        logger.error(error)

######
# helper functions
######


def create_dirs():
    clean_tmp()

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


def clean_tmp():
    folders = [f.path for f in os.scandir('./tmp') if f.is_dir()]

    for folder in folders:
        files = os.listdir(folder)
        for file in files:
            os.remove(f'{folder}{file}')

        os.rmdir(folder)

    if os.path.isdir('./tmp'):
        files = os.listdir('./tmp')
        for file in files:
            os.remove(f'./tmp/{file}')


def s3_list_buckets():
    """Using boto3 to list all buckets available in the s3.

    Args:
        Not required.

    Returns:
        Returns a list of buckets available in s3.
    """
    s3 = boto3.client('s3')
    response = s3.list_buckets()['Buckets']
    try:
        return [bucket['Name'] for bucket in response]
    except Exception:
        raise ValueError("ERROR: No buckets found")


def s3_parquet_prefix_buckets(bucket_list):
    """
    Using the returned list of buckets from s3_list_buckets function,
    filters the buckets for only those with a prefix of
    nc-de-databaskers-csv-store.

    Args:
        Not required.

    Returns:
        Returns the full specified bucket name.
    """
    full_list = bucket_list
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-csv-store" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")


def list_files_to_convert(bucket_list):
    """Using boto3, downloads all the files to a temporary local
    directory 'tmp'.

    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3 = boto3.client('s3')
    bucket = s3_parquet_prefix_buckets(bucket_list)
    list = s3.list_objects(Bucket=bucket)['Contents']
    for key in list:
        if ('input_csv_key' in key['Key']):
            s3.download_file(
                bucket, key['Key'],
                f'./tmp/csv_processed/{key["Key"].split("/")[1]}')


def create_dim_counterparty():
    """Using pandas to read the counterparty.csv and address.csv files
    injested from the totesys DB,  merges the two dataframes on the
    address_id. creates a new dataframe from the merged data in the schema
    required.

    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the counterparty.csv and
        address.csv files, storing the dim_counterparty.csv in the tmp
        directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_counterparty - "column" does
        not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        counter_party_df = pd.read_csv('./tmp/csv_processed/counterparty.csv')
        address = pd.read_csv('./tmp/csv_processed/address.csv')
        counter_party_df = counter_party_df.rename(
            columns={"legal_address_id": "address_id"})
        merged_df = pd.merge(counter_party_df, address, on='address_id')

        dim_counterparty = pd.DataFrame(
            data={'counterparty_id': merged_df['counterparty_id'],
                  'counterparty_legal_name':
                      merged_df['counterparty_legal_name'],
                  'counterparty_legal_address_line_1':
                      merged_df['address_line_1'],
                  'counterparty_legal_address_line_2':
                      merged_df['address_line_2'],
                  'counterparty_legal_district': merged_df['district'],
                  'counterparty_legal_city': merged_df['city'],
                  'counterparty_legal_postal_code': merged_df['postal_code'],
                  'counterparty_legal_country': merged_df['country'],
                  'counterparty_legal_phone_number': merged_df['phone']}
            ).sort_values(by=['counterparty_id'])

        dim_counterparty.to_csv(
            './tmp/csv_processed/dim_counterparty.csv', index=False)
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
    """Using pandas to read the currency.csv file injested from
    the totesys DB, creates a new dataframe from the data in the
    schema required.

    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the currency.csv file,
        storing the dim_currency.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_currency - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """

    try:
        cur_df = pd.read_csv('./tmp/csv_processed/currency.csv')

        dim_currency = pd.DataFrame(
            data={'currency_id': cur_df['currency_id'],
                  'currency_code': cur_df['currency_code'],
                  'currency_name': cur_df['currency_code']})

        dim_currency['currency_name'] = dim_currency['currency_name'
                                                     ].str.replace('GBP',
                                                                   'Pounds')
        dim_currency['currency_name'] = dim_currency['currency_name'
                                                     ].str.replace('USD',
                                                                   'Dollars')
        dim_currency['currency_name'] = dim_currency['currency_name'
                                                     ].str.replace('EUR',
                                                                   'Euros')

        dim_currency.to_csv(
            './tmp/csv_processed/dim_currency.csv', index=False)
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


def create_dim_staff():
    """Using pandas to read the staff.csv and department.csv files
    injested from the totesys DB,  merges the two dataframes on
    the department_id.
    creates a new dataframe from the merged data in the schema required.

    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the staff.csv and department.csv
        files, storing the dim_staff.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_staff - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        staff_df = pd.read_csv('./tmp/csv_processed/staff.csv')
        dept_df = pd.read_csv('./tmp/csv_processed/department.csv')
        staff_dept_df = pd.merge(
            staff_df, dept_df, on='department_id').sort_values('staff_id')
        dim_staff = pd.DataFrame(
            data={'staff_id': staff_dept_df['staff_id'],
                  'first_name': staff_dept_df['first_name'],
                  'last_name': staff_dept_df['last_name'],
                  'department_name': staff_dept_df['department_name'],
                  'location': staff_dept_df['location'],
                  'email_address': staff_dept_df['email_address']})

        dim_staff.to_csv('./tmp/csv_processed/dim_staff.csv', index=False)
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
    """
    Using pandas to read the design.csv file injested from the totesys DB,
    creates a new dataframe from the data in the schema required.

    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the design.csv file,
        storing the dim_design.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_design - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        des_df = pd.read_csv('./tmp/csv_processed/design.csv')
        dim_design = pd.DataFrame(
            data={'design_id': des_df['design_id'],
                  'design_name': des_df['design_name'],
                  'file_location': des_df['file_location'],
                  'file_name': des_df['file_name']})

        dim_design.to_csv('./tmp/csv_processed/dim_design.csv', index=False)
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
    """Using pandas to read the address.csv file injested from the
    totesys DB, creates a new dataframe from the data in the schema required.

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
        loc_df = pd.read_csv('./tmp/csv_processed/address.csv')
        dim_location = pd.DataFrame(
            data={'location_id': loc_df['address_id'],
                  'address_line_1': loc_df['address_line_1'],
                  'address_line_2': loc_df['address_line_2'],
                  'district': loc_df['district'],
                  'city': loc_df['city'],
                  'postal_code': loc_df['postal_code'],
                  'country': loc_df['country'],
                  'phone': loc_df['phone']})

        dim_location.to_csv(
            './tmp/csv_processed/dim_location.csv', index=False)
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
    """Using pandas to read the sales_order.csv file injested from the
    totesys DB, quantifying the lower and upper bounds of minimum and
    maximum dates to create a new dataframe with each date attribute
    from the data in the schema required.

    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the sales_order.csv file,
        storing the dim_date.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: dim_date - "column" does not exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        df = pd.read_csv('./tmp/csv_processed/sales_order.csv')

        df[['created_at',
            'last_updated',
            'agreed_delivery_date',
            'agreed_payment_date']] = df[['created_at',
                                          'last_updated',
                                          'agreed_delivery_date',
                                          'agreed_payment_date']].apply(
                                              pd.to_datetime,
                                              format="%Y-%m-%d %H:%M:%S.%f")

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
        dim_date['date_id'] = [
            min_date+timedelta(days=x)
            for x in range(((
                max_date + timedelta(days=1))-min_date).days)]

        dim_date['year'] = dim_date['date_id'].dt.year
        dim_date['month'] = dim_date['date_id'].dt.month
        dim_date['day'] = dim_date['date_id'].dt.day
        dim_date['day_of_week'] = dim_date['date_id'].dt.day_of_week
        dim_date['day_name'] = dim_date['date_id'].dt.day_name()
        dim_date['month_name'] = dim_date['date_id'].dt.month_name()
        dim_date['quarter'] = dim_date['date_id'].dt.quarter

        dim_date.to_csv('./tmp/csv_processed/dim_date.csv', index=False)

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
    """Using pandas to read the sales_order.csv file injested from the totesys
    DB, creates a new dataframe from the data, utilising datetime to conform
    to the schema required.

    Args:
        Not required.
    Returns:
        Writes a star schema CSV file from the sales_order.csv file,
        storing the fact_sales_order.csv in the tmp directory.
    Raises:
        ValueError: on KeyError - ERROR: fact_sales_order - "column" does not
            exist
        ValueError: on FileNotFoundError - missing files or directory
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        ValueError: on pd.errors.DtypeWarning - column data mismatch
        Exception: Blanket to catch unhandled error for cloudwatch
    """
    try:
        fact_sales = pd.read_csv('./tmp/csv_processed/sales_order.csv')

        fact_sales[
            ['created_at',
             'last_updated',
             'agreed_delivery_date',
             'agreed_payment_date'
             ]] = fact_sales[['created_at',
                              'last_updated',
                              'agreed_delivery_date',
                              'agreed_payment_date']].apply(
                                  pd.to_datetime,
                                  format="%Y-%m-%d %H:%M:%S.%f")

        fact_sales['created_time'] = fact_sales['created_at'].dt.time
        fact_sales['last_updated_time'] = fact_sales['last_updated'].dt.time
        fact_sales['created_at'] = fact_sales['created_at'].dt.date
        fact_sales['last_updated'] = fact_sales['last_updated'].dt.date
        fact_sales['agreed_delivery_date'] = fact_sales['agreed_delivery_date'
                                                        ].dt.date

        fact_sales['agreed_payment_date'] = fact_sales['agreed_payment_date'
                                                       ].dt.date

        fact_sales.rename({'created_at': 'created_date',
                           'last_updated': 'last_updated_date',
                          'staff_id': 'sales_staff_id'}, axis=1, inplace=True)
        fact_sales = fact_sales[['sales_order_id', 'created_date',
                                 'created_time', 'last_updated_date',
                                 'last_updated_time', 'sales_staff_id',
                                 'counterparty_id', 'units_sold',
                                 'unit_price', 'currency_id',
                                 'design_id', 'agreed_payment_date',
                                 'agreed_delivery_date',
                                 'agreed_delivery_location_id']]
        fact_sales.to_csv(
            './tmp/csv_processed/fact_sales_order.csv', index=False)

    except KeyError as error:
        raise ValueError(f'ERROR: fact_sales_order - {error} does not exist')
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
    Obtains all dim and fact tables from tmp directory and converts to
    parquet format using Pandas.
    This is then exported to a temporary directory "tmp/pqt_processed".

    Args:
        Not required.
    Returns:
        Parquet converted files into a tmp/pqt_processed directory.
    Raises:
        ValueError: on pd.errors.EmptyDataError - files exist but are empty
        Exception: Blanket to catch unhandled errors
    """

    file_list = os.listdir('./tmp/csv_processed')
    dim_re = re.compile(r'dim_\w+.csv')
    dims = [file for file in file_list if dim_re.match(file)]

    fact_re = re.compile(r'fact_\w+.csv')
    facts = [file for file in file_list if fact_re.match(file)]

    file_list = dims + facts

    if len(file_list) < 1:
        raise ValueError('ERROR: No CSV files to convert to parquet')
    for file in file_list:
        filename = os.path.basename(file).split('.')[0]
        df = pd.read_csv(f'./tmp/csv_processed/{file}')

        if not os.path.exists("./tmp/pqt_processed"):
            os.makedirs("./tmp/pqt_processed")
        try:
            df.to_parquet(f'./tmp/pqt_processed/{filename}.parquet')
        except pd.errors.EmptyDataError as EDE:
            raise ValueError(f'ERROR: {EDE}')
        except Exception:
            raise ValueError(f'ERROR: {Exception}')


def update_csv_conversion_file():
    """
    Downloads the Run number in the csv_conversion.txt and is amended to
    increment the count, then uploads to processed_csv_key in the s3 bucket.

    Args:
        Not required.
    Returns:
        Uploads to processed_csv_key in the s3 bucket.
    Raises:
        No error handling.
    """
    s3 = boto3.client('s3')
    full_list = s3_list_buckets()
    bucket_str = ''
    for bucket in full_list:
        if "nc-de-databakers-csv-store" in bucket:
            bucket_str = bucket
    s3.download_file(bucket_str,
                     'processed_csv_key/csv_conversion.txt',
                     './tmp/csv_processed/csv_conversion.txt')

    contents = open('./tmp/csv_processed/csv_conversion.txt', 'r').read()
    num = int(contents.split(' ')[1])
    with open('./tmp/csv_processed/csv_conversion.txt', 'w+') as file:
        file.write(f'Run {num+1}')
    s3.upload_file("./tmp/csv_processed/csv_conversion.txt",
                   bucket_str,
                   "processed_csv_key/csv_conversion.txt")


def s3_list_prefix_csv_buckets(bucket_list):
    """Uses s3_list_buckets function to list all buckets and a for loop to
    filter with prefix for csv store.

    Args:
        Not required.
    Returns:
        Matching prefixed bucket.
    Raises:
        ValueError: ERROR: If prefix not found in any bucket.
    """
    full_list = bucket_list
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-csv-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")


def s3_list_prefix_parquet_buckets(bucket_list):
    """Uses boto3 to list all buckets with prefix for parquet store.

    Args:
        bucket_list: return value of s3_list_buckets function
    Returns:
        Matching prefixed bucket.
    Raises:
        ValueError: ERROR: If prefix not found in any bucket.
    """
    full_list = bucket_list
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-parquet-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")


def s3_pqt_input_setup_success(parquet_prefix):
    """Using the s3 client and a for loop, this function will check if the
    parquet bucket contains the setup success text file in the input key,
    to ensure that terraform has been setup correctly.

    Args:
        parquet_prefix: return value of function
        s3_list_prefix_parquet_buckets(bucket_list)
    Returns:
        True (boolean)
    Raises:
        ValueError: ERROR: If setup file not found.
    """
    s3 = boto3.client('s3')
    bucket = parquet_prefix
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if (object['Key'] ==
                'input_parquet_key/setup_success_parquet_input.txt'):
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")


def s3_csv_processed_setup_success(csv_prefix):
    """Using the s3 client and a for loop, we will check if the csv bucket
    contains the setup success text file in the processed key,
    to ensure that terraform has been setup correctly.

    Args:
        parquet_prefix: return value of function
        s3_list_prefix_parquet_buckets(bucket_list)
    Returns:
        True (boolean)
    Raises:
        ValueError: ERROR: If setup file not found.
    """
    s3 = boto3.client('s3')
    bucket = csv_prefix
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if (object['Key'] ==
                'processed_csv_key/setup_success_csv_processed.txt'):
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")


def s3_upload_pqt_files_to_pqt_input_key(parquet_prefix):
    """
    function to move files from processed_csv_key to the input_parquet_key,
    using boto3 resource. invoking s3_csv_processed_setup_success(csv_prefix)
    if this returns true, transfers the files

    Args:
        Not Required.
    Returns:
        Nothing.
    Raises:
         ValueError() - if s3_pqt_input_setup_success(parquet_prefix)
         returns false
    """
    s3 = boto3.resource('s3')
    if s3_pqt_input_setup_success(parquet_prefix):
        pqt_files = os.listdir('./tmp/pqt_processed')
        for file in pqt_files:
            s3.Bucket(parquet_prefix).upload_file(
                f'./tmp/pqt_processed/{file}', f'input_parquet_key/{file}')
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")


def s3_create_pqt_input_completed_txt_file(bucket):
    """
    Using the os module we will check if the local directory contains
    a run number file, If this file does not exist, create and
    pass it in "0", then we will increment it each time this current
    function is run by "1".
    At the same time a separate file will read from the run number
    file and log each time this function is run appending the run number
    on another line.

    Args:
        Not Required.
    Returns:
        Nothing.
    Raises:
        Nothing
    """

    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket,
                         'input_parquet_key/parquet_export.txt',
                         './tmp/pqt_input/parquet_export.txt')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

    contents = open('./tmp/pqt_input/parquet_export.txt', 'r').read()
    num = int(contents.split(' ')[1])
    with open('./tmp/pqt_input/parquet_export.txt', 'w+') as file:
        file.write(f'Run {num+1}')

    try:
        s3.upload_file("./tmp/pqt_input/parquet_export.txt",
                       bucket,
                       "input_parquet_key/parquet_export.txt")
    except Exception as error:
        raise ValueError(f'ERROR: {error}')


def s3_move_csv_files_to_csv_processed_key_and_delete_from_input(csv_prefix):
    """
    function to move files from input_csv_key to the processed_csv_key,
    using both boto3 client and resource.
    invoking s3_csv_processed_setup_success(csv_prefix) if this returns
    true, transfers the files
    Args:
        csv_prefix
    Returns:
        Nothing.
    Raises:
        ValueError() - if s3_csv_processed_setup_success(csv_prefix)
        returns false
    """
    s3res = boto3.resource('s3')
    s3cli = boto3.client('s3')
    bucket_name = csv_prefix
    if s3_csv_processed_setup_success(csv_prefix):
        source_key = 'input_csv_key/'
        destination_key = 'processed_csv_key/'
        bucket = s3res.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=source_key)
        for obj in objects:
            if obj.key.endswith('.csv'):
                obj_key = obj.key.replace(source_key, '')
                s3cli.copy(CopySource={'Bucket': bucket_name,
                                       'Key': obj.key},
                           Bucket=bucket_name, Key=destination_key+obj_key)
                obj.delete()
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")


def s3_create_csv_processed_completed_txt_file(bucket):
    """
    function designed to increment the run number and export history
    saved in the log files.
    if the initial run does not contain a run number file,
    it will make one.
    Args:
        Not required.
    Returns:
        Nothing.
    Raises:
        Nothing.
    """
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket, 'processed_csv_key/csv_processed.txt',
                         './tmp/csv_processed/csv_processed.txt')
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

    contents = open('./tmp/csv_processed/csv_processed.txt', 'r').read()
    num = int(contents.split(' ')[1])
    with open('./tmp/csv_processed/csv_processed.txt', 'w+') as file:
        file.write(f'Run {num+1}')

    try:
        s3.upload_file("./tmp/csv_processed/csv_processed.txt",
                       bucket,
                       "processed_csv_key/csv_processed.txt")
    except Exception as error:
        raise ValueError(f'ERROR: {error}')


def s3_pqt_input_upload_and_log(parquet_prefix):
    """
    uploads the pqt_input_exported_completed.txt to the input_parquet_key
    Args:
        parquet_prefix
    Returns:
        Nothing.
    Raises:
        Nothing.
    """

    s3 = boto3.resource('s3')
    s3.Bucket(parquet_prefix).upload_file(
        './tmp/pqt_input/pqt_input_export_completed.txt',
        'input_parquet_key/pqt_input_export_completed.txt')


def s3_csv_processed_upload_and_log(csv_prefix):
    """
    uploads the csv_processesd_exported_completed.txt to the processed_csv_key
    Args:
        csv_prefix
    Returns:
        Nothing.
    Raises:
        Nothing.
    """

    s3 = boto3.resource('s3')
    s3.Bucket(csv_prefix).upload_file(
        './tmp/csv_processed/csv_processed_export_completed.txt',
        'processed_csv_key/csv_processed_export_completed.txt')
