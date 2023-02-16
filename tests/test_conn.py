from src.conn import lambda_handler
from src.conn import conn_db
from dotenv import load_dotenv, find_dotenv
import os
import pg8000
import csv
import boto3
import pytest

table_names = ['address', 'counterparty', 'currency', 'department', 'design', 'payment_type', 'payment', 'purchase_order', 'sales_order', 'staff', 'transaction']

def test_correct_credentials_are_retrieved():
    load_dotenv(find_dotenv())
    assert os.environ.get("tote_host") == "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    assert os.environ.get("tote_port") == "5432"
    assert os.environ.get("tote_database") == "totesys"
    assert os.environ.get("tote_user") == "project_user_2"
    assert os.environ.get("tote_password") == "paxjekPK3hDXu2aXcJ9xyuBS"

def test_connection_failss_when_credentials_are_correct():
    load_dotenv(find_dotenv())
    host=os.environ.get("tote_host")
    database=os.environ.get("tote_database")
    user=os.environ.get("tote_user")
    password=os.environ.get("tote_password")
    with pytest.raises(pg8000.exceptions.InterfaceError):
        conn = pg8000.Connection("mybaduser", "password=password", "host=host", "database=database")
def test_connection_user_for_interface_error():
        try:
            conn = pg8000.Connection("user=", "password=password", "host=host", "database=database")
            return conn
        except pg8000.exceptions.InterfaceError as IFE:
            assert f"{IFE}" == "Can't create a connection to host password=password and port database=database (timeout is None and source_address is None)."

def test_csv_file_is_created_for_each_table_name_containing_data_in_DB_data_directory():
    lambda_handler()
    target_dir = "tmp"
    
    for table_name in table_names:
        csv_file = f"{target_dir}/{table_name}.csv"
        assert os.path.exists(csv_file)
