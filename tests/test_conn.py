from src.conn import conn_db, name_fetcher, host, database, user, password
import src.conn
from dotenv import load_dotenv, find_dotenv
import os
import pg8000
import csv
import pytest
from unittest.mock import Mock, patch
​
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
​
def test_db_conn_bad_host_exception():
    load_dotenv(find_dotenv())
    host="badhost"
    database=os.environ.get("tote_database")
    user=os.environ.get("tote_user")
    password=os.environ.get("tote_password")
    
    with pytest.raises(pg8000.exceptions.InterfaceError):
        pg8000.Connection(user=user, password=password, host=host, database=database)
     
def test_db_conn_bad_user_exception():
    load_dotenv(find_dotenv())
    host=os.environ.get("tote_host")
    database=os.environ.get("tote_database")
    user='Baduser'
    password=os.environ.get("tote_password")
    
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '28P01'
​
def test_db_conn_bad_password_exception():
    load_dotenv(find_dotenv())
    host=os.environ.get("tote_host")
    database=os.environ.get("tote_database")
    user=os.environ.get("tote_user")
    password='bad_password'
    
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '28P01'
     
def test_db_conn_bad_password_exception():
    load_dotenv(find_dotenv())
    host=os.environ.get("tote_host")
    database='bad_database'
    user=os.environ.get("tote_user")
    password=os.environ.get("tote_password")
    
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '3D000'