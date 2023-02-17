"""This application should allow the user to retrieve and convert the data from the ToteSys database to CSV format by using python and PostgreSQL queries.

The database will be accessed through the dotenv environment ensuring the credentials are kept out of plain sight and are kept confidential.

This application is designed to be run as a lambda function.
"""

import os
from dotenv import load_dotenv, find_dotenv
from pg8000 import Connection
import csv
import pg8000
​
"""Find and retrieve environment variables from the .env file containing confidential credentials.
"""
load_dotenv(find_dotenv())
host=os.environ.get("tote_host")
database=os.environ.get("tote_database")
user=os.environ.get("tote_user")
password=os.environ.get("tote_password")
​
def conn_db():
    """Using the required dotenv variables to feed the pg8000 connection function with the correct host name, database name and credentials.
    We will be able to access the database in a pythonic context and use python and PostgreSQL to achieve the intended functionality.
    """
    
    try:
        conn = Connection(user=user, password=password, host=host, database=database)
        print(conn, '<-- this is conn')
        return conn
    except pg8000.exceptions.InterfaceError as IFE:
        return IFE
        # this error occurs host is incorrect
    except pg8000.exceptions.DatabaseError as DBE:
        if PE.args[0]['C'] == '28P01':
            return 'user or password incorrect'
        return DBE
        # error occurs for incorrect user or password
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '28P01':
            return 'user or password incorrect'
            # error occurs when user or password is incorrect
        elif PE.args[0]['C'] == '3D000':
            return PE.args[0]['M']
            # error occurs when database does not exist
        return PE
    except Exception as error:
        return f"{error}"
​
def name_fetcher():
    """This PSQL query retrieves all public tables, within the ToteSys database, in this instance.
​
    Running the query, the results are stored in a variable, table_names.
    """
​
    table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';"
    try:
        table_names = conn_db().run(table_query)
        return table_names
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '42703':
            return PE.args[0]['M']
        elif PE.args[0]['C'] == '42P01':
            return PE.args[0]['M']

def lambda_handler():
    """Using the os library, a new directory 'tmp' is created providing it doesn't already exist.
    """

    if not os.path.exists("tmp"):
        os.makedirs("tmp")

    """Using a for loop, we will access the table_names variable which contains dictionaries of each table.

    For each table, we use PSQL to select all the data and store the value of this query to a variable named table_data.

    Using with open, we will use the CSV library to write this table_data to a new file in CSV format.
    """
    table_names = name_fetcher()
    for table_name in table_names:
        query = f"SELECT * FROM {table_name[0]}"
        table_data = conn_db().run(query)
        with open(f"tmp/{table_name[0]}.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(table_data)
    conn_db().close()

lambda_handler()