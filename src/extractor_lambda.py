"""Defines lambda function to handle creation of S3 text object."""

#from botocore.exceptions import ClientError
from pg8000 import Connection
from pg8000.native import identifier
import logging
import pg8000
import boto3
import json
import csv
import os

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Handles S3 PutObject event and logs the contents of file.

    On receipt of a PutObject event, checks that the file type is txt and
    then logs the contents.

    Args:
        event:
            a valid S3 PutObject event -
            see https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
        context:
            a valid AWS lambda Python context object - see
            https://docs.aws.amazon.com/lambda/latest/dg/python-context.html

    Raises:
        RuntimeError: An unexpected error occurred in execution. Other errors
        result in an informative log message.
    """
    try:
        if not os.path.isdir('./tmp'):
            os.makedirs('./tmp')
        # call DB and save CSV files
        put_tables_to_csv()

        # upload CSV files to S3 bucket
        input_bucket = get_csv_store_bucket()
        s3_setup_success(input_bucket)
        s3_upload_csv_files(input_bucket)
        update_csv_export_file(input_bucket)
        
        files = os.listdir('./tmp')
        for file in files:
            os.remove(f'./tmp/{file}')
        os.removedirs('./tmp')
    except Exception as error:
        logger.error(error)


######
# DB defs
######

def conn_db():
    """Using the required dotenv variables to feed the pg8000 connection function with the correct host name, database name and credentials.
    We will be able to access the database in a pythonic context and use python and PostgreSQL to achieve the intended functionality.
    
    Args:
        Not required.

    Returns:
        conn (pg8000.legacy.Connection): A database connection to be used by name_fetcher function. 
    """
    sm = boto3.client('secretsmanager')
    try:
        secret_value = sm.get_secret_value(SecretId="totesys_creds")['SecretString']
    except sm.exceptions.ResourceNotFoundException as RNFE:
        return 'ERROR: Secrets Manager can''t find the specified Totesys secret.'

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
        raise ValueError(f'ERROR: {IFE}')
    except pg8000.exceptions.DatabaseError as DBE:
        if DBE.args[0]['C'] == '28P01':
            raise ValueError('ERROR: user or password incorrect')
        else:
            raise ValueError(f"ERROR: {DBE}")
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '28P01':
            raise ValueError('ERROR: user or password incorrect')
        elif PE.args[0]['C'] == '3D000':
            raise ValueError(f"ERROR: {PE.args[0]['M']}")
        else:
            raise ValueError(f"ERROR: {PE}")
    except Exception as error:
        raise ValueError(f"ERROR: {error}")

def db_tables_fetcher():
    """This PSQL query retrieves all public tables, within the ToteSys database, in this instance.
    Running the query, the results are stored in a variable, table_names.
    
    Args:
        Not required.

    Returns:
        table_names (tuple): A tuple of all table names, to be used by further SQL query in lambda_handler function. 
    """
    table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';"
    try:
        table_names = conn_db().run(table_query)
        return table_names
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '42703':
            raise ValueError(f"ERROR: {PE.args[0]['M']}")
        elif PE.args[0]['C'] == '42P01':
            raise ValueError(f"ERROR: {PE.args[0]['M']}")
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

def put_tables_to_csv():
    """Using the os library, a new directory 'tmp' is created providing it doesn't already exist.

    Using a for loop, we will access the table_names variable which contains tuple of each table.
    For each table, we use PSQL to select all the data and store the value of this query to a variable named table_data.
    Using with open, we will use the CSV library to write this table_data to a new file in CSV format.
    
    Args:
        Not required.

    Returns:
        Writes a CSV file per table from the database and stores in a tmp directory.
    """
    
    if not os.path.exists("tmp"):
        os.makedirs("tmp")
    
    table_names = db_tables_fetcher()
    for table_name in table_names:
        try:
            query = f"SELECT * FROM {identifier(table_name[0])}"
            columns = f"""SELECT attname, format_type(atttypid, atttypmod) AS type
                        FROM   pg_attribute
                        WHERE  attrelid = :table::regclass
                        AND    attnum > 0
                        AND    NOT attisdropped
                        ORDER  BY attnum;"""
            columns_data = conn_db().run(columns, table=table_name[0])
            column_names = []
            for col in columns_data:
                column_names.append(col[0])

            table_data = conn_db().run(query)
            with open(f"tmp/{table_name[0]}.csv", "w", newline="") as csvfile:
                w = csv.DictWriter(csvfile, column_names)
                if csvfile.tell() == 0:
                    w.writeheader()
                writer = csv.writer(csvfile)
                writer.writerows(table_data)
        except IndexError:
            raise ValueError('ERROR: SQL query invalid, nothing with that table name')
        except pg8000.dbapi.ProgrammingError as PE:
            if PE.args[0]['C'] == '42703':
                raise ValueError(f"ERROR: {PE.args[0]['M']}")
            elif PE.args[0]['C'] == '42P01':
                raise ValueError(f"ERROR: {PE.args[0]['M']}")
        except Exception as error:
            raise ValueError(f'ERROR: {error}')
    conn_db().close()

#####
# S3 Handling
#####

def s3_list_buckets():
    s3=boto3.client('s3')
    try:
        response=s3.list_buckets()['Buckets']
        return [bucket['Name'] for bucket in response]
    except Exception as error:
        raise ValueError(f'ERROR: {error}')
    
def get_csv_store_bucket():
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-csv-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def s3_setup_success(input_bucket):
    s3=boto3.client('s3')

    
    try:
        objects = s3.list_objects(Bucket=input_bucket)['Contents']
    except Exception as error:
        raise ValueError(f'ERROR: {error}')

    for object in objects:
        if object['Key'] == 'input_csv_key/setup_success_csv_input.txt':
            return True
    return False

def s3_upload_csv_files(input_bucket):
    s3=boto3.resource('s3')
    if s3_setup_success(input_bucket):
        if os.path.exists('./tmp'):
            csv_files = os.listdir('./tmp')
            if len(csv_files) > 0:
                for file in csv_files:
                    s3.Bucket(get_csv_store_bucket()).upload_file(f'./tmp/{file}', f'input_csv_key/{file}')
            else:
                raise ValueError('ERROR: No CSV files to upload to S3 are found')
        else:
            raise ValueError('ERROR: can''t locate temp directory to read CSV files!')
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")
    
def update_csv_export_file(bucket):
  """ 
    function downloads the Run number in the csv_export.txt is ammended to increment the count,#
    then uploads to input_csv_key on the bucket.
    Args:
        Not required.

    Returns:
        nothing
  """
  s3=boto3.client('s3')

  try:
    s3.download_file(bucket, 'input_csv_key/csv_export.txt', './tmp/csv_export.txt')
  except Exception as error:
        raise ValueError(f'ERROR: {error}')

  contents = open('./tmp/csv_export.txt', 'r').read()
  num = int(contents.split(' ')[1])
  with open('./tmp/csv_export.txt', 'w+') as file:
    file.write(f'Run {num+1}')
  
  try:
    s3.upload_file("./tmp/csv_export.txt", bucket, "input_csv_key/csv_export.txt")
  except Exception as error:
        raise ValueError(f'ERROR: {error}')