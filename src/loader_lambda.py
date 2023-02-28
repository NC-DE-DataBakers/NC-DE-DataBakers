"""Defines lambda function to populate Data Warehouse."""

#from botocore.exceptions import ClientError
from pg8000 import Connection
import logging
import pg8000
import boto3
import json
import os
import pandas as pd
import sqlalchemy as sa
# import psycopg2


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

sm = boto3.client('secretsmanager')

host = ""
database = ""
user = ""
password = ""

def lambda_handler(event, context):

    """This application should allow the user to fill the Data Warehouse with appropriate logging. 
        This lambda:
        1- fill the DW
        2- moves parquet files from input to processed keys in the parquet bucket
        3- registers the run number.
    """
    try:
        if not os.path.isdir('./pqt_tmp'):
            os.makedirs('./pqt_tmp')

        # download .parquet files to pqt_tmp folder
        parquet_bucket = s3_list_prefix_parquet_buckets()
        dowload_parquet_files_to_process(parquet_bucket)

        # Empty the data warehouse
        empty_tables()

        # Fill the data warehouse
        fill_tables()
        
        # move .parquet files from the input key to the processed key
        s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input(parquet_bucket)
        s3_create_parquet_processed_completed_txt_file(parquet_bucket)

        files = os.listdir('./pqt_tmp')
        for file in files:
            os.remove(f'./pqt_tmp/{file}')
        os.removedirs('./pqt_tmp')

    except Exception as error:
        logger.error(error)


#####
# S3 helper functions
#####


def s3_list_buckets():
    """Using the s3 client, we will return a list containing the names of all the buckets.
    
    Args:
        Not required.

    Returns:
        List of all s3 buckets.
    """
    s3=boto3.client('s3')
    response=s3.list_buckets()['Buckets']
    return [bucket['Name'] for bucket in response]

def s3_list_prefix_parquet_buckets():
    """Using the s3 client, we will return the name of the s3 bucket containing the "nc-de-databakers-parquet-store-" prefix buckets through a for loop of the list returned in s3_list_buckets, we will also add some error handling that checks if the bucket is empty, or if the prefix is not found
    
    Args:
        Not required.

    Returns:
        The name of the correct prefixed bucket, ValueError otherwise
    """
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-parquet-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Parquet prefix not found in any bucket")

def s3_pqt_input_setup_success(bucket):
    """Using the s3 client and a for loop, we will check if the parquet bucket contains the setup success text file in the input key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
    Args:
        Not required.

    Returns:
        True if file is found, ValueError otherwise.
    """
    s3=boto3.client('s3')
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if object['Key'] == 'input_parquet_key/setup_success_parquet_input.txt':
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_pqt_processed_setup_success(bucket):
    """Using the s3 client and a for loop, we will check if the parquet bucket contains the setup success text file in the processed key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
    Args:
        Not required.

    Returns:
        True if file is found, ValueError otherwise.
    """
    s3=boto3.client('s3')

    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if object['Key'] == 'input_parquet_key/setup_success_parquet_input.txt':
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def dowload_parquet_files_to_process(bucket):
    """ 
    downloads all the parquet files to a temporary pqt_tmp local directory
    Args:
        Not required.

    Returns:
        nothing
        
    """
    s3=boto3.client('s3')

    if s3_pqt_input_setup_success(bucket):
        list=s3.list_objects(Bucket=bucket)['Contents']
        for key in list:
            if('input_parquet_key' in key['Key']):
                s3.download_file(bucket, key['Key'], f'./pqt_tmp/{key["Key"].split("/")[1]}')
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")
    
def s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input(bucket_name):
    """Using the s3 resource and s3 client we will attempt to move files from the parquet bucket input key to the parquet bucket processed key. We will check the name of the parquet bucket using the s3 list and we will also check that the set-up for the processed key was successful. If not, it will throw an error.
    
    Args:
        Not required.

    Returns:
        Nothing, unless there is an error.
    """
    s3res=boto3.resource('s3')
    s3cli=boto3.client('s3')

    if s3_pqt_processed_setup_success(bucket_name):
        source_key = 'input_parquet_key/'
        destination_key = 'processed_parquet_key/'
        bucket = s3res.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=source_key)
        for obj in objects:
            if obj.key.endswith('.parquet'):
                obj_key = obj.key.replace(source_key, '')
                s3cli.copy(CopySource={'Bucket': bucket_name, 'Key':obj.key}, Bucket=bucket_name, Key=destination_key+obj_key)
                obj.delete()
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_create_parquet_processed_completed_txt_file(bucket):
    """Using the os module we will check if the local directory contains a run number file. This will happen after the previos upload function is invoked. If this file does not exist, we will pass it in "0", then we will increment it each time this current function is run by "1". At the same time a separate file will read from the run number file and log each time this function is run appending the run number on another line.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3=boto3.client('s3')

    try:
        s3.download_file(bucket, 'processed_parquet_key/parquet_processed.txt', './pqt_tmp/parquet_processed.txt')
    except Exception as error:
            raise ValueError(f'ERROR: {error}')

    contents = open('./pqt_tmp/parquet_processed.txt', 'r').read()
    num = int(contents.split(' ')[1])
    with open('./pqt_tmp/parquet_processed.txt', 'w+') as file:
        file.write(f'Run {num+1}')
    
    try:
        s3.upload_file("./pqt_tmp/parquet_processed.txt", bucket, "processed_parquet_key/parquet_processed.txt")
    except Exception as error:
            raise ValueError(f'ERROR: {error}')



######
# DW helper functions
######

def conn_db():
    """Using the required dotenv variables to feed the pg8000 connection function with the correct host name, database name and credentials.
    We will be able to access the database in a pythonic context and use python and PostgreSQL to achieve the intended functionality.
    
    Args:
        Not required.

    Returns:
        conn (pg8000.legacy.Connection): A database connection to be used by name_fetcher function. 
    """
    try:
        secret_value = sm.get_secret_value(SecretId="dw_creds")['SecretString']
    except sm.exceptions.ResourceNotFoundException as RNFE:
        return 'ERROR: Secrets Manager can''t find the specified DW secret.'

    parsed_secret = json.loads(secret_value)
    host = parsed_secret["host"]
    database = parsed_secret["database"]
    user = parsed_secret["username"]
    password = parsed_secret["password"]
    port = parsed_secret["port"]

    try:
        conn = Connection(user=user, password=password, host=host, database=database, port=port)
        return conn
    except pg8000.exceptions.InterfaceError as IFE:
        return f'ERROR: {IFE}'
    except pg8000.exceptions.DatabaseError as DBE:
        if PE.args[0]['C'] == '28P01':
            return 'ERROR: user or password incorrect'
        return DBE
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '28P01':
            return 'ERROR: user or password incorrect'
        elif PE.args[0]['C'] == '3D000':
            return f"ERROR: {PE.args[0]['M']}"
        return PE
    except Exception as error:
        return f"ERROR: {error}"

def empty_tables():
    """This PSQL query empties all tables from data.
    
    Args:
        Not required.

    Returns:
        Not required. 
    """
    
    #delete_query = "DELETE FROM dim_date WHERE year > 1800 RETURNING *;"
    delete_query = 'TRUNCATE dim_date, dim_staff, dim_location, dim_currency, dim_design, dim_counterparty, fact_sales_order;'

    try:
        conn = conn_db()
        cur = conn.cursor()
        result = cur.execute(delete_query)
        return True
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '42703':
            return f"ERROR: {PE.args[0]['M']}"
        elif PE.args[0]['C'] == '42P01':
            return f"ERROR: {PE.args[0]['M']}"
    except Exception as error:
        return f'ERROR: {error}'

def fill_tables():
    """This PSQL query fills all tables in the data warehouse.
    
    Args:
        Not required.

    Returns:
        Not required. 
    """

    engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:5432/{database}')


    dim_date = pd.read_parquet('./pqt_tmp/dim_date.parquet')
    dim_date.to_sql('dim_date', engine, if_exists='append', index=False)

    dim_design = pd.read_parquet('./pqt_tmp/dim_design.parquet')
    dim_design.to_sql('dim_design', engine, if_exists='append', index=False)
    
    dim_counterparty = pd.read_parquet('./pqt_tmp/dim_counterparty.parquet')
    dim_counterparty.to_sql('dim_counterparty', engine, if_exists='append', index=False)
    
    dim_currency = pd.read_parquet('./pqt_tmp/dim_currency.parquet')
    dim_currency.to_sql('dim_currency', engine, if_exists='append', index=False)

    dim_location = pd.read_parquet('./pqt_tmp/dim_location.parquet')
    dim_location.to_sql('dim_location', engine, if_exists='append', index=False)

    dim_staff = pd.read_parquet('./pqt_tmp/dim_staff.parquet')
    dim_staff.to_sql('dim_staff', engine, if_exists='append', index=False)

    fact_sales_order = pd.read_parquet('./pqt_tmp/fact_sales_order.parquet')
    fact_sales_order.to_sql('fact_sales_order', engine, if_exists='append', index=False)