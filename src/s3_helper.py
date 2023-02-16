import boto3
def s3_list_buckets():
    s3=boto3.client('s3')
    response=s3.list_buckets()['Buckets']
    names = []
    for bucket in response:
        names.append(bucket['Name'])
    return names

def s3_list_prefix_buckets():
    full_list = s3_list_buckets()
    for bucket in full_list:
        if "nc-de-databakers-csv-store-" in bucket:
            return bucket