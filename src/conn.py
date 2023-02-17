"""This application should allow the user to retrieve and convert the data from the ToteSys database to CSV format by using python and PostgreSQL queries.
The database will be accessed through the dotenv environment ensuring the credentials are kept out of plain sight and are kept confidential.
This application is designed to be run as a lambda function.

The file is run by calling this module with the python keyword.

Example:
    python src/conn.py

To run the test file, please use the below:
    pytest tests/test_conn.py
"""
import os
from pg8000 import Connection
import csv
import pg8000
import boto3
import json

sm = boto3.client('secretsmanager')

def conn_db():
    """Using the required dotenv variables to feed the pg8000 connection function with the correct host name, database name and credentials.
    We will be able to access the database in a pythonic context and use python and PostgreSQL to achieve the intended functionality.
    
    Args:
        Not required.

    Returns:
        conn (pg8000.legacy.Connection): A database connection to be used by name_fetcher function. 
    """
    try:
        secret_value = sm.get_secret_value(SecretId="totesys_creds")['SecretString']
    except sm.exceptions.ResourceNotFoundException as RNFE:
        return 'ERROR: Secrets Manager can''t find the specified secret.'

    parsed_secret = json.loads(secret_value)
    host = parsed_secret["host"]
    database = parsed_secret["database"]
    user = parsed_secret["username"]
    password = parsed_secret["password"]
    
    try:
        conn = Connection(user=user, password=password, host=host, database=database)
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

def name_fetcher():
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
            return f"ERROR: {PE.args[0]['M']}"
        elif PE.args[0]['C'] == '42P01':
            return f"ERROR: {PE.args[0]['M']}"
    except Exception as error:
        return f'ERROR: {error}'

def lambda_handler():
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
    
    table_names = name_fetcher()
    for table_name in table_names:
        try:
            query = f"SELECT * FROM {table_name[0]}"
            table_data = conn_db().run(query)
            with open(f"tmp/{table_name[0]}.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(table_data)
        except IndexError:
            return 'ERROR, SQL query invalid, nothing with that table name'
        except pg8000.dbapi.ProgrammingError as PE:
            if PE.args[0]['C'] == '42703':
                return f"ERROR: {PE.args[0]['M']}"
            elif PE.args[0]['C'] == '42P01':
                return f"ERROR: {PE.args[0]['M']}"
        except Exception as error:
            return f'ERROR: {error}'   
    conn_db().close()

lambda_handler()