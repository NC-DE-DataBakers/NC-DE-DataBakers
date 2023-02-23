import boto3
import os


"""This application should allow the user to retrieve and convert the data from the ToteSys database to CSV format by using python and PostgreSQL queries.
The database will be accessed through the dotenv environment ensuring the credentials are kept out of plain sight and are kept confidential.
This application is designed to be run as a lambda function.

The file is run by calling this module with the python keyword.

Example:
    python src/conn.py

To run the test file, please use the below:
    pytest tests/test_conn.py
"""

def s3_list_buckets():
    """Using the s3 client, we will return a list containing the names of all the buckets.
    
    Args:
        Not required.

    Returns:
        List of all s3 buckets.
    """
    s3=boto3.client('s3')
    response=s3.list_buckets()['Buckets']
    return [bucket['Name'] for bucket in response]

def s3_list_prefix_csv_buckets():
    """Using the s3 client, we will return the name of the s3 bucket containing the "nc-de-databakers-csv-store-" prefix buckets through a for loop of the list returned in s3_list_buckets, we will also add some error handling that checks if the bucket is empty, or if the prefix is not found
    
    Args:
        Not required.

    Returns:
        The name of the correct prefixed bucket
    """
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-csv-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def s3_list_prefix_parquet_buckets():
    """Using the s3 client, we will return the name of the s3 bucket containing the "nc-de-databakers-parquet-store-" prefix buckets through a for loop of the list returned in s3_list_buckets, we will also add some error handling that checks if the bucket is empty, or if the prefix is not found
    
    Args:
        Not required.

    Returns:
        The name of the correct prefixed bucket, ValueError otherwise
    """
    full_list = s3_list_buckets()
    if full_list == []:
        raise ValueError("ERROR: No buckets found")
    for bucket in full_list:
        if "nc-de-databakers-parquet-store-" in bucket:
            return bucket
    raise ValueError("ERROR: Prefix not found in any bucket")

def s3_pqt_input_setup_success():
    """Using the s3 client and a for loop, we will check if the parquet bucket contains the setup success text file in the input key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
    Args:
        Not required.

    Returns:
        True if file is found, ValueError otherwise.
    """
    s3=boto3.client('s3')
    bucket = s3_list_prefix_parquet_buckets()
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if object['Key'] == 'input_parquet_key/setup_success_parquet_input.txt':
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_csv_processed_setup_success():
    """Using the s3 client and a for loop, we will check if the csv bucket contains the setup success text file in the processed key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
    Args:
        Not required.

    Returns:
        True if file is found, ValueError otherwise.
    """
    s3=boto3.client('s3')
    bucket = s3_list_prefix_csv_buckets()
    objects = s3.list_objects(Bucket=bucket)['Contents']
    for object in objects:
        if object['Key'] == 'processed_csv_key/setup_success_csv_processed.txt':
            return True
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_upload_pqt_files_to_pqt_input_key():
    """Using the s3 resource and a for loop, we will first check if the csv bucket contains the setup success text file in the processed key, to ensure that terraform has been setup correctly, the convert parquet function would have stored the converted parquets in the pqt_temp locat directory. The for loop will allow us to iterrate throught these files and upload them one by one using the s3 upload file resource. If there is an error, when looking for the directory, a value error will be raised informing the user of a terraform issue.
    
    Args:
        Not required.

    Returns:
        Nothing, unless there is an error.
    """
    s3=boto3.resource('s3')
    if s3_pqt_input_setup_success():
        pqt_files = os.listdir('pqt_tmp')
        for file in pqt_files:
            s3.Bucket(s3_list_prefix_parquet_buckets()).upload_file(f'./pqt_tmp/{file}', f'input_parquet_key/{file}')
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_create_pqt_input_completed_txt_file():
    """Using the os module we will check if the local directory contains a run number file. This will happen after the previos upload function is invoked. If this file does not exist, we will pass it in "0", then we will increment it each time this current function is run by "1". At the same time a separate file will read from the run number file and log each time this function is run appending the run number on another line.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3_upload_pqt_files_to_pqt_input_key()
    if not os.path.exists("pqt_input_run_number.txt"):
        with open("pqt_input_run_number.txt", "w") as rn_f:
            rn_f.write("0")
    with open("pqt_input_run_number.txt", "r+") as rn_f:
        run_num = int(rn_f.read()) + 1
        rn_f.seek(0)
        rn_f.write(str(run_num))
    with open(f"pqt_input_export_completed.txt", "a+") as f:
        f.write(f'run {run_num}\n')


def s3_move_csv_files_to_csv_processed_key_and_delete_from_input():
    """Using the s3 resource and s3 client we will attempt to move files from the csv bucket input key to the csv buckety processed key. We will check the name of the csv bucket using the s3 list and we will also check that the set-up for the processed key waws successful. If not, it will throw an error.
    
    Args:
        Not required.

    Returns:
        Nothing, unless there is an error.
    """
    s3res=boto3.resource('s3')
    s3cli=boto3.client('s3')
    bucket_name = s3_list_prefix_csv_buckets()
    if s3_csv_processed_setup_success():
        source_key = 'input_csv_key/'
        destination_key = 'processed_csv_key/'
        bucket = s3res.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=source_key)
        for obj in objects:
            if obj.key.endswith('.csv'):
                obj_key = obj.key.replace(source_key, '')
                s3cli.copy(CopySource={'Bucket': bucket_name, 'Key':obj.key}, Bucket=bucket_name, Key=destination_key+obj_key)
                obj.delete()
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_create_csv_processed_completed_txt_file():
    """Using the os module we will check if the local directory contains a run number file. This will happen after the previos upload function is invoked. If this file does not exist, we will pass it in "0", then we will increment it each time this current function is run by "1". At the same time a separate file will read from the run number file and log each time this function is run appending the run number on another line.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3_move_csv_files_to_csv_processed_key_and_delete_from_input()
    if not os.path.exists("csv_processed_run_number.txt"):
        with open("csv_processed_run_number.txt", "w") as rn_f:
            rn_f.write("0")
    with open("csv_processed_run_number.txt", "r+") as rn_f:
        run_num = int(rn_f.read()) + 1
        rn_f.seek(0)
        rn_f.write(str(run_num))
    with open(f"csv_processed_export_completed.txt", "a+") as f:
        f.write(f'run {run_num}\n')

def s3_pqt_input_upload_and_log():
    """Using the s3 resource we will upload the pqt input completed text file to the input parquet bucket in the s3, after uploading and updating the run count.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3_create_pqt_input_completed_txt_file()
    s3=boto3.resource('s3')
    s3.Bucket(s3_list_prefix_parquet_buckets()).upload_file('./pqt_input_export_completed.txt', 'input_parquet_key/pqt_input_export_completed.txt')

def s3_csv_processed_upload_and_log():
    """Using the s3 resource we will upload the csv processed completed text file to the processed csv bucket in the s3, after moving the csv files from the csv input key to the processed csv key and updating the run count.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3_create_csv_processed_completed_txt_file()
    s3=boto3.resource('s3')
    s3.Bucket(s3_list_prefix_csv_buckets()).upload_file('./csv_processed_export_completed.txt', 'processed_csv_key/csv_processed_export_completed.txt')
