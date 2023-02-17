import pg8000
import pytest
import boto3
from moto import mock_secretsmanager
import json

sm = boto3.client('secretsmanager')
secret_value = sm.get_secret_value(SecretId="totesys_creds")['SecretString']
parsed_secret = json.loads(secret_value)
host = parsed_secret["host"]
port = parsed_secret["port"]
database = parsed_secret["database"]
user = parsed_secret["username"]
password = parsed_secret["password"]

@mock_secretsmanager
def test_correct_credentials_are_retrieved_from_secrets_manager():
    sm = boto3.client('secretsmanager')
    sm.create_secret(Name='totesys_creds', SecretString='{"host":"fake_host", "port":"fake_port", "database":"fake_database", "user":"fake_user", "password":"fake_password"}')
    
    secret_value = sm.get_secret_value(SecretId="totesys_creds")['SecretString']
    parsed_secret = json.loads(secret_value)
    assert parsed_secret["host"] == "fake_host"
    assert parsed_secret["port"] == "fake_port"
    assert parsed_secret["database"] == "fake_database"
    assert parsed_secret["user"] == "fake_user"
    assert parsed_secret["password"] == "fake_password"

@mock_secretsmanager
def test_raises_error_when_secret_name_does_not_exist():
    sm = boto3.client('secretsmanager')
    with pytest.raises(sm.exceptions.ResourceNotFoundException):
        secret_value = sm.get_secret_value(SecretId="totesys_creds")


def test_connection_user_for_interface_error():
        try:
            conn = pg8000.Connection("user=", "password=password", "host=host", "database=database")
            return conn
        except pg8000.exceptions.InterfaceError as IFE:
            assert f"{IFE}" == "Can't create a connection to host password=password and port database=database (timeout is None and source_address is None)."

def test_db_conn_bad_host_exception():
    host = "badhost"
    with pytest.raises(pg8000.exceptions.InterfaceError):
        pg8000.Connection(user=user, password=password, host=host, database=database)
     
def test_db_conn_bad_user_exception():
    user ='Baduser'
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '28P01'

def test_db_conn_bad_password_exception():
    password ='bad_password'
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '28P01'
     
def test_db_conn_bad_password_exception():
    database='bad_database'
    try:
        pg8000.Connection(user=user, password=password, host=host, database=database)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '3D000'
        
def test_ErrorCode_42703_bad_column_query(): 
    conn = pg8000.Connection(user=user, password=password, host=host, database=database)
    table_query = "SELECT _BADTABLEQUERY_ FROM information_schema.tables WHERE table_schema = \'public\';"
    try:
        table_names = conn.run(table_query)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '42703'
        
def test_ErrorCode_42P01_bad_Relationship_query():
    conn = pg8000.Connection(user=user, password=password, host=host, database=database)
    table_query = "SELECT table_name FROM BADTABLE.tables WHERE table_schema = \'public\';"
    try:
        table_names = conn.run(table_query)
    except pg8000.dbapi.ProgrammingError as PE:
        assert PE.args[0]['C'] == '42P01'



