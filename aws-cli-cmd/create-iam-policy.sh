#!/bin/bash

BUCKET_NAME="instacart-data-eng-project"

# Define policy names
POLICY_NAME="instacart-glue-s3-policy"

# Create IAM policy to allow access to the S3 bucket
aws iam create-policy \
    --policy-name "$POLICY_NAME" \
    --policy-document file:///Users/allen.chen/Documents/allen-repos/aws-data-eng-project-demo/aws-cli-cmd/glue-s3-iam-policy.json \
    --description "iam policy to access the s3 bucket" 

