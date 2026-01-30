#!/bin/bash

ROLE_NAME="instacart-glue-service-role"

# Create IAM role for Glue service
aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document file:///Users/allen.chen/Documents/allen-repos/aws-data-eng-project-demo/aws-cli-cmd/glue-trust-policy.json \
    --description "Glue ETL Job role"



