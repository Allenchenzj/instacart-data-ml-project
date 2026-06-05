#!/bin/bash

# Create SageMaker managed MLflow tracking server
# Run create-sagemaker-role.sh first if the role doesn't exist yet

aws sagemaker create-mlflow-tracking-server \
    --tracking-server-name instacart-mlflow \
    --artifact-store-uri s3://instacart-aws-data-ml-eng-project/mlflow-artifacts/ \
    --tracking-server-size Small \
    --role-arn arn:aws:iam::606476261726:role/instacart-sagemaker-role \
    --region ap-southeast-2 

echo "MLflow tracking server creation started (takes ~15-20 minutes)"
echo "Run the info script to check progress:"
echo "  bash aws-cli-cmd/mlflow-tracking-server-info.sh"

# delete command:
# aws sagemaker delete-mlflow-tracking-server --tracking-server-name instacart-mlflow --region ap-southeast-2