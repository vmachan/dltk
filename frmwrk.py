import argparse
import os
import boto3
import datetime
import sys

# Init vars
today_yyyy_mm_dd = str(datetime.date.today())
input_file_name = "MSFT_HistoricalQuotes.csv"
input_file_path = "./ticker-data"
file_name_to_use = input_file_path + "/" + input_file_name
s3_tgt_file_name = today_yyyy_mm_dd + "_" + input_file_name
s3_bucket_name = "frmwrk-ticker-data"

print("today_yyyy_mm_dd -> ", today_yyyy_mm_dd)
print("input_file_name -> ", input_file_name)
print("input_file_path -> ", input_file_path)
print("file_name_to_use -> ", file_name_to_use) 
print("s3_tgt_file_name -> ", s3_tgt_file_name) 
print("s3_bucket_name -> ", s3_bucket_name) 

# sys.exit()

# Stock Ticker Data

# Use the regular isengard account NOT the one that is used for CUR data
session = boto3.Session(profile_name="default")
s3_client = session.client("s3")

# 1. Create S3 bucket to use for processing - this is a one-time setup
s3_client.create_bucket(Bucket=s3_bucket_name)
s3_client.upload_file(file_name_to_use, s3_bucket_name, s3_tgt_file_name)

# Create table in Glue catalog for above CSV data
glue_client = session.client("glue")

response = glue_client.create_crawler(
        Name='ticker-csv-crawler',
        Role='service-role/AWSGlueServiceRole-IAMAccessCURinS3',
        DatabaseName='default',
        Description='Crawler for generating table from ticker csv data in s3 bucket - ticker-data',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://' + s3_bucket_name
                }
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        },
        TablePrefix=''
    )

print(response)

response = glue_client.start_crawler(
    Name='ticker-csv-crawler'
)

print(response)

response = glue_client.update_table(
    DatabaseName='default',
    TableInput={
        'Name': s3_bucket_name.replace('-', '_'),
        'Description': 'Ticker',
        'StorageDescriptor': {
            'SerdeInfo': {
                'Name': 'OpenCSVSerde',
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                'Parameters': {
                    'separatorChar': ','
                }
            }
        }
    }
)

print(response)

