#!/bin/bash

# Show key info for the instacart-mlflow tracking server

aws sagemaker describe-mlflow-tracking-server \
    --tracking-server-name instacart-mlflow \
    --region ap-southeast-2 \
    --query '{
        Name: TrackingServerName,
        ARN: TrackingServerArn,
        Status: TrackingServerStatus,
        Url: TrackingServerUrl,
        ArtifactStore: ArtifactStoreUri,
        Size: TrackingServerSize,
        Created: CreationTime
    }' \
    --output table
