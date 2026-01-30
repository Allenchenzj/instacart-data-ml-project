#!/bin/bash

AWS_ACCOUNT_ID="471112945220"
POLICY_NAME="instacart-glue-s3-policy"
ROLE_NAME="instacart-glue-service-role"
POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"

# Attach the created policy to the role
aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn "$POLICY_ARN"
