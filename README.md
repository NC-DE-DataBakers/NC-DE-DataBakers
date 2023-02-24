# NC-DE-DataBakers
Data Engineering Project - create applications that will Extract, Transform and Load data from a prepared source into a data lake and warehouse hosted in AWS.

Prerequisites
This is an example of how to list things you need to use the software and how to install them.

Installation:
pip install -r requirements.txt

conn.py
  using pg8000, a connection is made using credentials sourced from AWS secrets manager, retreiving the stored database credentials.
  querys to the database, SELECT table_name, passing them to lambda_handler() where a new SQL query is made to SELECT * from each table provided. this data is saved to csv, and uploaded to the processing bucket hosted on aws s3.

  Common errorhandling include:
  Connection errors - DatabaseError: '28P01' - user/password is incorrect
  Connection errors - ProgrammingError: '28P01' - user/password is incorrect
  Connection errors - ProgrammingError: '3D000' - database does not exist
  Connection errors - InterfaceError: - typically a bad host name

  Query errors - ProgrammingError: '42703' - column does not exist
  Query errors - ProgrammingError: '42P01' - relation to table does not exist

s3_helper.py
  Using boto3, we are able to access AWS directly. This enables us to upload files to the input key, within the CSV store bucket, by implementing Python logic.

  Ensuring the setup_success_csv_input.txt file exists, indicates the terraform has been deployed succesfully. The CSV files generated (locally) from the lambda_handler() function can then be uploaded to the input key, within the CSV store bucket.

  After completion of uploading the CSV files, a text file csv_export_completed.txt, containing the most recent run number is created and uploaded to the input key, within the CSV store bucket. The latest run number can be found in the run_number.txt file.

  Common error-handling includes:
  No buckets have been created - Error raised: 'No buckets found'
  The CSV store bucket has not been created - Error raised: 'Prefix not found in any bucket'
  Terraform has not been deployed - Error: 'Terraform deployment unsuccessful'

s3_processed_helper.py
  Using boto3, we are able to access AWS directly. This enables us to upload the converted parquet files to the parquet input key, within the CSV store bucket, by implementing Python logic.

  Ensuring the setup_success_csv_input.txt file exists, indicates the terraform has been deployed succesfully. The converted Parquet files saved (locally) can then be uploaded to the input key, within the Parquet store bucket.

  After completion of uploading the Parquet files, a text file parquet_export_completed.txt, containing the most recent run number is created and uploaded to the input key, within the Parquet store bucket. The latest run number can be found in the run_number.txt file.

  Thereafter, the CSVs from the csv bucket will be moved from input to processed and a log will be created and updated in the csv processed once the files are moved in.

  Common error-handling includes:
  No buckets have been created - Error raised: 'No buckets found'
  The CSV store bucket has not been created - Error raised: 'Prefix not found in any bucket'
  The Parquet store bucket has not been created - Error raised: 'Prefix not found in any bucket'
  Terraform has not been deployed - Error: 'Terraform deployment unsuccessful'

  s3_pqt_processed_helper.py
  Using boto3, we are able to access AWS directly. After the star schema conversion and upload to the datastore has been completed, we will need to indicate which parquets have been processed. 

  To show this, the Parquets from the parquet bucket will need to be moved from input to processed and a log will be created and updated in the parquet_processed text file once the files are moved in.

  Common error-handling includes:
  No buckets have been created - Error raised: 'No buckets found'
  The CSV store bucket has not been created - Error raised: 'Prefix not found in any bucket'
  The Parquet store bucket has not been created - Error raised: 'Prefix not found in any bucket'
  Terraform has not been deployed - Error: 'Terraform deployment unsuccessful'