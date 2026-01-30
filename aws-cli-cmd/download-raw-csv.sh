#!/bin/bash
curl -o raw_data/aisles.csv https://instacart-raw-data.s3.ap-southeast-2.amazonaws.com/raw/aisles.csv
curl -o raw_data/departments.csv https://instacart-raw-data.s3.ap-southeast-2.amazonaws.com/raw/departments.csv
curl -o raw_data/products.csv https://instacart-raw-data.s3.ap-southeast-2.amazonaws.com/raw/products.csv
curl -o raw_data/orders.csv https://instacart-raw-data.s3.ap-southeast-2.amazonaws.com/raw/orders.csv
curl -o raw_data/order_products__prior.csv https://instacart-raw-data.s3.ap-southeast-2.amazonaws.com/raw/order_products__prior.csv