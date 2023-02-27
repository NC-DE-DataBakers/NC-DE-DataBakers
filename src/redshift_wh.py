from pg8000 import Connection
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
    # try:
    #     secret_value = sm.get_secret_value(SecretId="postgres_creds")['SecretString']
    # except sm.exceptions.ResourceNotFoundException as RNFE:
    #     return 'ERROR: Secrets Manager can''t find the specified secret.'

    # parsed_secret = json.loads(secret_value)
    # host = parsed_secret["host"]
    # database = parsed_secret["database"]
    # user = parsed_secret["username"]
    # password = parsed_secret["password"]
    
    host = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    database = "postgres"
    user = "project_team_2"
    password = "PYmh2xrDFRBpMBS"

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
    select_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'project_team_2';"
    try:
        table_names = conn_db().run(select_query)
        print(conn_db().run(select_query))
        return table_names
    except pg8000.dbapi.ProgrammingError as PE:
        if PE.args[0]['C'] == '42703':
            return f"ERROR: {PE.args[0]['M']}"
        elif PE.args[0]['C'] == '42P01':
            return f"ERROR: {PE.args[0]['M']}"
    except Exception as error:
        return f'ERROR: {error}'

# def create_postgres_tables():
#     table_names = ["fact_sales_order", "dim_date", "dim_staff", "dim_location", "dim_currency", "dim_design", "dim_counterparty"] 
#     conn = conn_db()
#     print(conn)
#     cur = conn.cursor()
#     for table_name in table_names:
#         create_query = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, name VARCHAR(255))"
#         cur.execute(create_query)
#         print(f"Table {table_name} created successfully")
#     conn.commit()
#     cur.close()
#     conn.close()

# conn_db()
# create_postgres_tables()

name_fetcher()