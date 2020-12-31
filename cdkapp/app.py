#!/usr/bin/env python3

from aws_cdk import core
import argparse
import xlrd
import boto3
import botocore
from botocore.exceptions import ClientError
import logging

from frmwrk.frmwrk_stack import frmwrkStack

# Source-to-target mapping - metadata format i.e. header row column positions in Excel 
SOURCE_NAME_POS = 0
SOURCE_LOCATION_POS = 1
SOURCE_FORMAT_POS = 2
SOURCE_DELIMITER_POS = 3
TARGET_NAME_POS = 4
TARGET_LOCATION_POS = 5
TARGET_FORMAT_POS = 6
TARGET_DELIMITER_POS = 7
SOURCE_SAMPLE_DATAFILE_POS = 8

_APP_HOME_DIR = "/home/machanv/xplrtst/frmwrk"
_SAMPLE_DATA_DIR = _APP_HOME_DIR + "/" + "sampledata"
_META_DATA_DIR = _APP_HOME_DIR + "/" + "metadata"
_META_DATA_FILENAME = "metadata.xlsx"

class sttm_record:
    def __init__(self
            ,source_name
            ,source_location
            ,source_format
            ,source_delimiter
            ,target_name
            ,target_location
            ,target_format
            ,target_delimiter
            ,source_sample_datafile
            ):
        self.source_name = source_name
        self.source_location = source_location
        self.source_format = source_format
        self.source_delimiter = source_delimiter
        self.target_name = target_name
        self.target_location = target_location
        self.target_format = target_format
        self.target_delimiter = target_delimiter
        self.source_sample_datafile = _SAMPLE_DATA_DIR + "/" + source_sample_datafile

def parse_metadata_from_excel(sttm_file_name):
    print(f"Parsing meta data from {sttm_file_name}..")
    sttm_map = []

    wb = xlrd.open_workbook(sttm_file_name)
    src2tgt_map = wb.sheet_by_name("Source-to-Target Mapping")
    if (src2tgt_map):
        for row in range(1, src2tgt_map.nrows): # Skipping the 1st header row - subscript 0
            # for col in range(src2tgt_map.ncols):
            _source_name = src2tgt_map.cell_value(row, SOURCE_NAME_POS)
            _source_location = src2tgt_map.cell_value(row, SOURCE_LOCATION_POS)
            _source_format = src2tgt_map.cell_value(row, SOURCE_FORMAT_POS)
            _source_delimiter = src2tgt_map.cell_value(row, SOURCE_DELIMITER_POS)
            _target_name = src2tgt_map.cell_value(row, TARGET_NAME_POS)
            _target_location = src2tgt_map.cell_value(row, TARGET_LOCATION_POS)
            _target_format = src2tgt_map.cell_value(row, TARGET_FORMAT_POS)
            _target_delimiter = src2tgt_map.cell_value(row, TARGET_DELIMITER_POS)
            _source_sample_datafile = src2tgt_map.cell_value(row, SOURCE_SAMPLE_DATAFILE_POS)
            sttm_map.append(
                            sttm_record(
                                         _source_name
                                        ,_source_location
                                        ,_source_format
                                        ,_source_delimiter
                                        ,_target_name
                                        ,_target_location
                                        ,_target_format
                                        ,_target_delimiter
                                        ,_source_sample_datafile
                                       )
                           )
    return sttm_map

#
# Main
#
argparser = argparse.ArgumentParser()
# argparser.add_argument("metadatafilename", type=str, help="Full path name of Excel file holding metadata")
argparser.add_argument("apphomedir", type=str, help="Full path name of CDK aplication home directory")
args = argparser.parse_args()
_APP_HOME_DIR = args.apphomedir 
# metadata_file_name = args.metadatafilename 
metadata_file_name = _META_DATA_DIR + "/" + _META_DATA_FILENAME

sttm_map = parse_metadata_from_excel(metadata_file_name)

curr_ACCOUNT = "424767260197"
curr_REGION = "us-east-1"
curr_ENV = core.Environment(account = curr_ACCOUNT, region = curr_REGION)

app = core.App()

frmwrkStack(app, "frmwrkS3", deploy_step=1, sttm_map=sttm_map)

frmwrkStack(app, "frmwrkNEXT", deploy_step=2, sttm_map=sttm_map)

app.synth()

# Place test data files 
def put_sample_datafile(sample_file, s3bucket):
    print(f"In put_sample_datafile: sample_file is {sample_file}, s3bucket is {s3bucket}")
    s3client = boto3.client('s3')
    try:
        response = s3client.upload_file(sample_file, s3bucket, sample_file)
    except ClientError as e:
        logging.error(e)
        return False
    return True

for sttm_record in sttm_map:
    if (sttm_record.source_location != "NULL"):
        print(f"put_sample_datafile({sttm_record.source_sample_datafile}, {sttm_record.source_location})")
        # put_sample_datafile(sttm_record.source_sample_datafile, sttm_record.source_location)

# Now run stack for creating crawlers based off of the sample/test data files 

