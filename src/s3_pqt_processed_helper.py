import boto3
import os

"""This application should allow the user to move the original csv files from the input csv key to the processed csv key with appropriate logging. 
The file is run by calling this module with the python keyword.

Example:
    python src/s3_processed_helper.py

To run the test file, please use the below:
    pytest tests/test_s3_processed_helper.py
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

def s3_pqt_processed_setup_success():
    """Using the s3 client and a for loop, we will check if the parquet bucket contains the setup success text file in the processed key, to ensure that terraform has been setup correctly, if it does not return with the desired outcome, an error will be raised regarding the terraform deployment
    
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

def s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input():
    """Using the s3 resource and s3 client we will attempt to move files from the parquet bucket input key to the parquet bucket processed key. We will check the name of the parquet bucket using the s3 list and we will also check that the set-up for the processed key was successful. If not, it will throw an error.
    
    Args:
        Not required.

    Returns:
        Nothing, unless there is an error.
    """
    s3res=boto3.resource('s3')
    s3cli=boto3.client('s3')
    bucket_name = s3_list_prefix_parquet_buckets()
    if s3_pqt_processed_setup_success():
        source_key = 'input_parquet_key/'
        destination_key = 'processed_parquet_key/'
        bucket = s3res.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=source_key)
        for obj in objects:
            if obj.key.endswith('.parquet'):
                obj_key = obj.key.replace(source_key, '')
                s3cli.copy(CopySource={'Bucket': bucket_name, 'Key':obj.key}, Bucket=bucket_name, Key=destination_key+obj_key)
                obj.delete()
    else:
        raise ValueError("ERROR: Terraform deployment unsuccessful")

def s3_create_parquet_processed_completed_txt_file():
    """Using the os module we will check if the local directory contains a run number file. This will happen after the previos upload function is invoked. If this file does not exist, we will pass it in "0", then we will increment it each time this current function is run by "1". At the same time a separate file will read from the run number file and log each time this function is run appending the run number on another line.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3_move_parquet_files_to_parquet_processed_key_and_delete_from_input()
    if not os.path.exists("parquet_processed_run_number.txt"):
        with open("parquet_processed_run_number.txt", "w") as rn_f:
            rn_f.write("0")
    with open("parquet_processed_run_number.txt", "r+") as rn_f:
        run_num = int(rn_f.read()) + 1
        rn_f.seek(0)
        rn_f.write(str(run_num))
    with open(f"parquet_processed_export_completed.txt", "a+") as f:
        f.write(f'run {run_num}\n')

def s3_parquet_processed_upload_and_log():
    """Using the s3 resource we will upload the parquet processed completed text file to the processed parquet bucket in the s3. This will happen once the parquet files are moved from the parquet input key to the processed parquet key and after the run count is updated.
    
    Args:
        Not required.

    Returns:
        Nothing.
    """
    s3_create_parquet_processed_completed_txt_file()
    s3=boto3.resource('s3')
    s3.Bucket(s3_list_prefix_parquet_buckets()).upload_file('./parquet_processed_export_completed.txt', 'processed_parquet_key/parquet_processed_export_completed.txt')
