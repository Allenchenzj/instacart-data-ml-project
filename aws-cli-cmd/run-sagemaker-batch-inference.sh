#!/bin/bash

# Upload inference script to S3
aws s3 cp sagemaker-scripts/batch_inference.py s3://instacart-aws-data-ml-eng-project/scripts/batch_inference.py

JOB_NAME="instacart-batch-inference-$(date +%Y%m%d-%H%M%S)"

aws sagemaker create-processing-job \
    --processing-job-name "$JOB_NAME" \
    --role-arn "arn:aws:iam::606476261726:role/instacart-sagemaker-role" \
    --app-specification '{
        "ImageUri": "683313688378.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3",
        "ContainerEntrypoint": ["python3", "/opt/ml/processing/code/batch_inference.py"],
        "ContainerArguments": [
            "--bucket-name", "instacart-aws-data-ml-eng-project",
            "--model-name", "instacart-reorder-model",
            "--mlflow-tracking-uri", "arn:aws:sagemaker:ap-southeast-2:606476261726:mlflow-tracking-server/instacart-mlflow",
            "--top-n", "10"
        ]
    }' \
    --processing-inputs '[{
        "InputName": "code",
        "S3Input": {
            "S3Uri": "s3://instacart-aws-data-ml-eng-project/scripts/batch_inference.py",
            "LocalPath": "/opt/ml/processing/code",
            "S3DataType": "S3Prefix",
            "S3InputMode": "File"
        }
    }]' \
    --processing-resources '{
        "ClusterConfig": {
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 20
        }
    }' \
    --stopping-condition '{"MaxRuntimeInSeconds": 3600}' \
    --region ap-southeast-2

echo "Batch inference job '$JOB_NAME' submitted"
echo ""
echo "Check status:"
echo "  aws sagemaker describe-processing-job --processing-job-name $JOB_NAME --region ap-southeast-2 --query ProcessingJobStatus --output text"
echo ""
echo "Stream logs:"
echo "  aws logs tail /aws/sagemaker/ProcessingJobs --log-stream-name-prefix $JOB_NAME --follow"
