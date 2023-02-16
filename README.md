# NC-DE-DataBakers
Data Engineering Project - create applications that will Extract, Transform and Load data from a prepared source into a data lake and warehouse hosted in AWS.

Prerequisites
This is an example of how to list things you need to use the software and how to install them.

Installation:
pip install -r requirements.txt

conn.py
using pg8000, a connection is made using credentials sourced from AWS secrets manager (currently local .env file).
querys to the data base to SELECT table_name, passing them to lambda_handler() where a new SQL query is made to SELECT * from each table provided. this data is saved to csv, and uploaded to the processing bucket hosted on aws s3.

common errorhandling include:
Connection errors - DatabaseError: '28P01' - user/password is incorrect
Connection errors - ProgrammingError: '28P01' - user/password is incorrect
Connection errors - ProgrammingError: '3D000' - database does not exist
Connection errors - InterfaceError: - typically a bad host name

Query errors - ProgrammingError: '42703' - column does not exist
Query errors - ProgrammingError: '42P01' - column does not exist


