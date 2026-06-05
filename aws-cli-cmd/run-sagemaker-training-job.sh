#!/bin/bash

# Upload training script to S3
aws s3 cp sagemaker-scripts/train.py s3://instacart-aws-data-ml-eng-project/scripts/train.py

JOB_NAME="instacart-reorder-training-$(date +%Y%m%d-%H%M%S)"

aws sagemaker create-training-job \
    --training-job-name "$JOB_NAME" \
    --role-arn "arn:aws:iam::606476261726:role/instacart-sagemaker-role" \
    --algorithm-specification '{
        "TrainingImage": "683313688378.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3",
        "TrainingInputMode": "File"
    }' \
    --hyper-parameters '{
        "bucket-name": "instacart-aws-data-ml-eng-project",
        "model-name": "instacart-reorder-model",
        "mlflow-tracking-uri": "arn:aws:sagemaker:ap-southeast-2:606476261726:mlflow-tracking-server/instacart-mlflow",
        "pr-auc-gate": "0.35",
        "sample-users": "10000",
        "sagemaker_program": "train.py",
        "sagemaker_submit_directory": "s3://instacart-aws-data-ml-eng-project/scripts/"
    }' \
    --output-data-config '{
        "S3OutputPath": "s3://instacart-aws-data-ml-eng-project/sagemaker-output/"
    }' \
    --resource-config '{
        "InstanceType": "ml.m5.xlarge",
        "InstanceCount": 1,
        "VolumeSizeInGB": 20
    }' \
    --stopping-condition '{"MaxRuntimeInSeconds": 3600}' \
    --region ap-southeast-2

echo "Training job '$JOB_NAME' submitted"
echo ""
echo "Check status:"
echo "  aws sagemaker describe-training-job --training-job-name $JOB_NAME --region ap-southeast-2 --query TrainingJobStatus --output text"
echo ""
echo "Stream logs:"
echo "  aws logs tail /aws/sagemaker/TrainingJobs --log-stream-name-prefix $JOB_NAME --follow"
