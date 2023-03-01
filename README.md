# **NC-DE-DataBakers**

Data Engineering Project - create applications that will Extract, Transform and Load (ETL) data from a prepared source into a data lake and warehouse hosted in AWS.

---

## **Prerequisites**

This is an example of how to list things you need to use the software and how to install them.

### AWS

Remember to set up your AWS credentials by running `aws configure` in your terminal.

Note: You can set the region and output format as _default_ by leaving the fields empty.
```python
aws configure
AWS Access Key ID [****************ABCD]:
# YOUR_ACCESS_KEY
AWS Secret Access Key [****************tr68]:
# YOUR_SECRET_KEY
```

### Setup Virtual Environment

*_**Ensure you have a supported version of Python installed.**_

Create and activate your virtual environment with the commands:

```python
python -m venv venv
source venv/bin/activate
```

### Installation

Once the virtual environment has been activated, run the following command to install the required packages:

```python
pip install -r requirements.txt
```

---

## **ETL**

The extractor_lambda, transformer_lambda and loader_lambda are used to automate the ETL process.

### Extractor Lambda

Using `pg8000`, a connection is made using credentials sourced from the AWS secrets manager.

The `extractor_lambda` extracts data from the ToteSys database and loads this to AWS. 

For each table in the ToteSys database, data is extracted using PSQL queries and temporarily saving them to a local `tmp/csv_input` directory as CSV files.  
With `boto3`, we can access the AWS console and load these files to the `input_csv_key` within the `csv-store` S3 bucket.

<table>
<tr><th>Local tmp directory</th><th>AWS S3 bucket</th></tr>
<tr><td>

_Extracted from ToteSys database_

<img src="readme_media/tmp.png" height="250">

</td><td>

_Loaded from local tmp directory_

<img src="readme_media/s3_csv_input.png" height="250">

</td></tr> </table>

Common error handling includes:

ConnectionErrors - `InterfaceError:` - Typically a bad host name.  
ConnectionErrors - `DatabaseError: 28P01` - User/Password is incorrect.  
ConnectionErrors - `ProgrammingError: 28P01` - User/Password is incorrect.  
ConnectionErrors - `ProgrammingError: 3D000` - Database does not exist.

QueryErrors - `ProgrammingError: 42703` - Column does not exist.  
QueryErrors - `ProgrammingError: 42P01` - Relation to table does not exist.

### Transformer Lambda

The `transformer_lambda` remodels the data into a star schema ready for conversion to parquet format.

With `boto3`, the CSV files are moved from the `input_csv_key` and `processed_csv_key` within the `csv-store` S3 bucket hosted on AWS.  
These are then downloaded from the `processed_csv_key` and temporarily stored in a local `tmp/csv_processed` directory.  
Utilising `pandas`, the CSV files are remodelled as required to create dimension tables and a fact table forming a star schema conforming to the warehouse data model.

_Example:
Using `pandas`, the `staff` and `department` dataframes are merged on a correlation of column such as `department_id`. The `dim_staff` dataframe is created from the merged dataframe and saved to the local `tmp/csv_processed` as a CSV file._
<table>
<tr><th>staff</th><th>department</th><th>dim_staff</th></tr>
<tr><td>

|first_name|department_id|
|--|--|
|Jeremie|1|
|Deron|2|

</td><td>

|department_id|department_name|
|--|--|
|1|Sales|
|2|Purchasing|

</td><td>

|first_name|department_name|
|--|--|
|Jeremie|Sales|
|Deron|Purchasing|

</td></tr> </table>

These new tables are then temporarily stored locally in `tmp/csv_processed` ready to be converted to parquet format and exported to the local `tmp/pqt_processed` directory.  
Once the conversion is complete, the parquet files are loaded into the `input_parquet_key` within the `parquet-store` S3 bucket.

Common error handling includes:

ValueError - `pd.errors.EmptyDataError` - Files exist but are empty.  
ValueError - `pd.errors.DtypeWarning` - Column data mismatch.

### Loader Lambda

The `loader_lambda` populates the tables in the data warehouse with data from the parquet files.

With `boto3`, the parquet files are downloaded from the `input_parquet_key` and temporarily stored locally in `tmp/pqt_input`.  
Connecting to the data warehouse with `pg8000`, `pandas` is used to populate the data warehouse with data from the downloaded parquet files.  
The parquet files within the `input_parquet_key` are then moved to the `processed_parquet_key` before emptying the `input_parquet_key` ready for new data.

## Testing

You can run the tests using `pytest` which, by default, will run all tests. To run individual tests, provide the path of the specfic test file.

Example:
```python
pytest tests/test_extractor_lambda.py
```

<!--
## csv_to_parquet.py


common error-handling include:
no data in CSV - pd.errors.EmptyDataError 
No files to convert - ValueError - 'ERROR: No CSV files to convert to parquet'
No bucket found - Value Error - "ERROR: No buckets found"
No bucket found - Value Error - "ERROR: Prefix not found in any bucket"

s3-pqt-processed-update-and-delete
No buckets have been created - Error raised: 'No buckets found'
The CSV store bucket has not been created - Error raised: 'Prefix not found in any bucket'
Terraform has not been deployed - Error: 'Terraform deployment unsuccessful'

## s3_processed_helper.py
Using boto3, we are able to access AWS directly. This enables us to upload the converted parquet files to the parquet input key, within the CSV store bucket, by implementing Python logic.

Ensuring the setup_success_csv_input.txt file exists, indicates the terraform has been deployed succesfully. The converted Parquet files saved (locally) can then be uploaded to the input key, within the Parquet store bucket.

After completion of uploading the Parquet files, a text file parquet_export_completed.txt, containing the most recent run number is created and uploaded to the input key, within the Parquet store bucket. The latest run number can be found in the run_number.txt file.

Thereafter, the CSVs from the csv bucket will be moved from input to processed and a log will be created and updated in the csv processed once the files are moved in.

Common error-handling includes:No buckets have been created - Error raised: 'No buckets found'
The CSV store bucket has not been created - Error raised: 'Prefix not found in any bucket'
The Parquet store bucket has not been created - Error raised: 'Prefix not found in any bucket'
Terraform has not been deployed - Error: 'Terraform deployment unsuccessful'

## s3_pqt_processed_helper.py
Using boto3, we are able to access AWS directly. After the star schema conversion and upload to the datastore has been completed, we will need to indicate which parquets have been processed. 

To show this, the Parquets from the parquet bucket will need to be moved from input to processed and a log will be created and updated in the parquet_processed text file once the files are moved in.

Common error-handling includes:
No buckets have been created - Error raised: 'No buckets found'
The CSV store bucket has not been created - Error raised: 'Prefix not found in any bucket'
The Parquet store bucket has not been created - Error raised: 'Prefix not found in any bucket'
Terraform has not been deployed - Error: 'Terraform deployment unsuccessful' -->
