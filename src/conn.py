"""This application should allow the user to retrieve and convert the data from the ToteSys database to CSV format by using python and PostgreSQL queries.

The database will be accessed through the dotenv environment ensuring the credentials are kept out of plain sight and are kept confidential.

This application is designed to be run as a lambda function.
"""

import os
from dotenv import load_dotenv, find_dotenv
from pg8000 import Connection
import csv

"""Find and retrieve environment variables from the .env file containing confidential credentials.
"""

load_dotenv(find_dotenv())

def lambda_handler():
    """Using the required dotenv variables to feed the pg8000 connection function with the correct host name, database name and credentials.
    
    We will be able to access the database in a pythonic context and use python and PostgreSQL to achieve the intended functionality.
    """
    host=os.environ.get("tote_host")
    database=os.environ.get("tote_database")
    user=os.environ.get("tote_user")
    password=os.environ.get("tote_password")

    conn = Connection(user=user, password=password, host=host, database=database)

    """This PSQL query retrieves all public tables, within the ToteSys database, in this instance.

    Running the query, the results are stored in a variable, table_names.
    """

    table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';"
    table_names = conn.run(table_query)

    """Using the os library, a new directory 'DB/data' is created providing it doesn't already exist.
    """

    if not os.path.exists("DB/data"):
        os.makedirs("DB/data")

    """Using a for loop, we will access the table_names variable which contains dictionaries of each table.

    For each table, we use PSQL to select all the data and store the value of this query to a variable named table_data.

    Using with open, we will use the CSV library to write this table_data to a new file in CSV format.
    """

    for table_name in table_names:
        query = f"SELECT * FROM {table_name[0]}"
        table_data = conn.run(query)
        with open(f"DB/data/{table_name[0]}.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(table_data)

    conn.close()

lambda_handler()