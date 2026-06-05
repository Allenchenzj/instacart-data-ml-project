#!/bin/bash

# Create IAM role for SageMaker with access to S3, Athena, and Glue catalog

aws iam create-role \
    --role-name instacart-sagemaker-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "sagemaker.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }'

aws iam attach-role-policy \
    --role-name instacart-sagemaker-role \
    --policy-arn arn:aws:iam::aws:policy/AmazonSageMakerFullAccess

aws iam put-role-policy \
    --role-name instacart-sagemaker-role \
    --policy-name SageMakerDataAccess \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::instacart-aws-data-ml-eng-project",
                    "arn:aws:s3:::instacart-aws-data-ml-eng-project/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                    "athena:GetWorkGroup"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetTable",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetPartitions",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:BatchGetPartition"
                ],
                "Resource": "*"
            }
        ]
    }'

echo "✅ IAM role instacart-sagemaker-role created"
echo "Role ARN: arn:aws:iam::606476261726:role/instacart-sagemaker-role"
