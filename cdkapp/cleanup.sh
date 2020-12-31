#!/bin/bash

cdk destroy 

aws s3 ls | grep landing
aws s3 rb s3://ticker-landing --force
aws s3 rb s3://ticker-parquet --force
aws s3 rb s3://party-landing --force
aws s3 rb s3://party-parquet --force
aws s3 ls | grep landing
