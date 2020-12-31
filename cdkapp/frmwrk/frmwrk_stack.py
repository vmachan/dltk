from aws_cdk import (
        core,
        aws_ec2 as _ec2,
        aws_rds as _rds,
        aws_s3 as _s3,
        aws_glue as _glue,
        aws_iam as _iam,
        )

def print_kwargs(sttm_map_holder):
    print(f' Kwargs: {sttm_map_holder}')
    for sttm_record in sttm_map_holder["sttm_map"]:
        print(sttm_record.source_name)
        print(sttm_record.source_location)
        print(sttm_record.source_format)
        print(sttm_record.source_delimiter)
        print(sttm_record.target_name)
        print(sttm_record.target_location)
        print(sttm_record.target_format)
        print(sttm_record.target_delimiter)
        print(sttm_record.source_sample_datafile)

def deploy_s3_components(currStack, sttm_map_holder):
    # print(f' Kwargs: {sttm_map_holder}')
    # for sttm_record in sttm_map_holder["sttm_map"]:
    for sttm_record in sttm_map_holder:
        if (sttm_record.source_location != "NULL"):
            bucket = _s3.Bucket(currStack
                    ,"frmwrkStack_s3_" + sttm_record.source_location
                    ,bucket_name = sttm_record.source_location
                    # ,versioned = true
                    ,removal_policy = core.RemovalPolicy.DESTROY # Not recommended for production
                    )
            print(bucket.to_string())

class frmwrkStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, deploy_step=1, sttm_map=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # super().__init__(scope, construct_id)

        # The code that defines your stack goes here

        # Print the **kwargs
        # print_kwargs(kwargs)

        if (deploy_step == 1):
            # deploy_s3_components(self, kwargs)
            deploy_s3_components(self, sttm_map)
        else:
            print("******* DEPLOY_STEP NOT 1 *******")

