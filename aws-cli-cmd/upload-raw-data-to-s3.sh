#!/bin/bash

BUCKET_NAME="instacart-data-eng-project"

aws s3 cp raw_data/aisles.csv "s3://${BUCKET_NAME}/raw/aisles/year=2026/month=01/day=28/aisles.csv"

aws s3 cp raw_data/departments.csv "s3://${BUCKET_NAME}/raw/departments/year=2026/month=01/day=28/departments.csv"

aws s3 cp raw_data/products.csv "s3://${BUCKET_NAME}/raw/products/year=2026/month=01/day=28/products.csv"

aws s3 cp raw_data/orders.csv "s3://${BUCKET_NAME}/raw/orders/year=2026/month=01/day=28/orders.csv"

aws s3 cp raw_data/order_products__prior.csv "s3://${BUCKET_NAME}/raw/order_products/year=2026/month=01/day=28/order_products__prior.csv"

