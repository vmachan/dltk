from aws_cdk import (
        core,
        aws_ec2 as _ec2,
        aws_rds as _rds,
        aws_s3 as _s3,
        aws_glue as _glue,
        aws_iam as _iam,
        )
import boto3
import botocore
from botocore.exceptions import ClientError
import logging
import os

def print_kwargs(sttm_map_holder):
    print(f' Kwargs: {sttm_map_holder}')
    for sttm_record in sttm_map_holder:
        print(sttm_record.source_name)
        print(sttm_record.source_location)
        print(sttm_record.source_format)
        print(sttm_record.source_delimiter)
        print(sttm_record.target_name)
        print(sttm_record.target_location)
        print(sttm_record.target_format)
        print(sttm_record.target_delimiter)
        print(sttm_record.source_sample_datafile)
        print(sttm_record.target_database)

def deploy_s3_components(currStack, sttm_map_holder):
    for sttm_record in sttm_map_holder:
        if (sttm_record.source_location != "NULL"):
            bucket = _s3.Bucket(currStack
                    ,"frmwrkStack_s3_" + sttm_record.source_location
                    ,bucket_name = sttm_record.source_location
                    # ,versioned = true
                    ,removal_policy = core.RemovalPolicy.DESTROY # Not recommended for production
                    # ,autoDeleteObjects = True # Not recommended for production
                    )
            # print(bucket.to_string())

def deploy_glue_crawlers(currStack, sttm_map_holder):
    glue_role = _iam.Role(
                            currStack
                          , 'glue_role'
                          , role_name = 'frmwrkCreateGlueComponents_glue_role'
                          , assumed_by = _iam.ServicePrincipal('glue.amazonaws.com')
                )

    glue_role.add_managed_policy(_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))
    glue_role.add_managed_policy(_iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'))
    # 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'

    for sttm_record in sttm_map_holder:
        print(f'database_name is {sttm_record.target_database} for glue crawler for {sttm_record.source_location} and s3Target is {sttm_record.target_location}')
        if (sttm_record.source_location != "NULL"):
            crawler = _glue.CfnCrawler(currStack
                    ,"frmwrkStack_glue_" + sttm_record.target_location
                    ,name = "frmwrkStack_glue_" + sttm_record.target_location
                    ,database_name = sttm_record.target_database
                    ,role = glue_role.role_arn
                    ,targets = {"s3Targets" : [{"path": f'{sttm_record.target_location}/'}]}
                    ,description = "Create crawler for " + sttm_record.source_location
                    )
            # print(bucket.to_string())

# Place test data files
def put_sample_datafile(sample_file, s3bucket, sample_file_basename):
    print(f"In put_sample_datafile: sample_file is {sample_file}, s3bucket is {s3bucket}, sample_file_basename is {sample_file_basename}")
    s3client = boto3.client('s3')
    try:
        response = s3client.upload_file(sample_file, s3bucket, sample_file_basename)
    except ClientError as e:
        logging.error(e)
        return False
    return True

class frmwrkStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, deploy_step=None, sttm_map=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # super().__init__(scope, construct_id)

        # The code that defines your stack goes here

        # Print the **kwargs
        # print_kwargs(kwargs)

        print(f"deploy_step is {deploy_step}")

        if (deploy_step == None):
            print("Nothing to do.. exiting..")

        elif (deploy_step == "CreateS3Buckets"):
            # deploy_s3_components(self, kwargs)
            deploy_s3_components(self, sttm_map)

        elif (deploy_step == "CopySampleDataFiles"):
            print("Copying Sample data files to S3 buckets..")
            for sttm_record in sttm_map:
                if (sttm_record.source_location != "NULL"):
                    datafilebasename = os.path.basename(sttm_record.source_sample_datafile)
                    # print(f"put_sample_datafile({sttm_record.source_sample_datafile}, {sttm_record.source_location}, {datafilebasename})")
                    put_sample_datafile(sttm_record.source_sample_datafile, sttm_record.source_location, datafilebasename)

        elif (deploy_step == "CreateGlueComponents"):
            print("Creating Glue Database first.. ")
            _glue.Database(self, "findb", database_name = "findb")
            print("Creating Glue components.. Crawlers, Tables, and Jobs..")
            deploy_glue_crawlers(self, sttm_map)

        elif (deploy_step == "CreateS3BucketHandlers"):
            print("Creating event handlers on S3 buckets to handle new files when receieved..")

        else:
            print("******* INVALID DEPLOY STEP *******")

