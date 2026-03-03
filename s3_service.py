import boto3
import yaml

s3 = boto3.client('s3')
bucket_name = "frances"


def write_data_to_s3(data, destination):
    s3.put_object(Bucket=bucket_name, Key=destination, Body=data)

def read_data_in_s3(file_name):
    return s3.get_object(Bucket=bucket_name, Key=file_name)['Body'].read()