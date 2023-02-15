from src.conn import lambda_handler
from dotenv import load_dotenv, find_dotenv
import os
from pg8000 import Connection
import csv

table_names = ['address', 'counterparty', 'currency', 'department', 'design', 'payment_type', 'payment', 'purchase_order', 'sales_order', 'staff', 'transaction']

def test_correct_credentials_are_retrieved():
    load_dotenv(find_dotenv())
    assert os.environ.get("tote_host") == "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    assert os.environ.get("tote_port") == "5432"
    assert os.environ.get("tote_database") == "totesys"
    assert os.environ.get("tote_user") == "project_user_2"
    assert os.environ.get("tote_password") == "paxjekPK3hDXu2aXcJ9xyuBS"

def test_csv_file_is_created_for_each_table_name_containing_data_in_DB_data_directory():
    lambda_handler()
    target_dir = "DB/data"
    
    for table_name in table_names:
        csv_file = f"{target_dir}/{table_name}.csv"
        assert os.path.exists(csv_file)

def test_valid_data_is_saved_to_CSV_file():
    host=os.environ.get("tote_host")
    database=os.environ.get("tote_database")
    user=os.environ.get("tote_user")
    password=os.environ.get("tote_password")

    conn = Connection(user=user, password=password, host=host, database=database)

    for table_name in table_names:
        query = f'SELECT * FROM {table_name}'
        original_data = conn.run(query)
        print(original_data)
        with open(f'DB/data/{table_name}.csv', 'r') as csv_file:
            csv_data = list(csv.reader(csv_file))
            print(csv_data)
        assert original_data == csv_data
