#!/bin/bash

# Upload script to S3
aws s3 cp glue-scripts/bronze-to-silver-job.py s3://instacart-aws-data-ml-eng-project/scripts/bronze-to-silver-job.py

# Create Glue job
aws glue create-job \
    --name "bronze-to-silver-job" \
    --role "arn:aws:iam::606476261726:role/DataLakehouseGlueRole" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://instacart-aws-data-ml-eng-project/scripts/bronze-to-silver-job.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--job-language": "python",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-glue-datacatalog": "true",
        "--datalake-formats": "iceberg",
        "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://instacart-aws-data-ml-eng-project/warehouse --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--bucket_name": "instacart-aws-data-ml-eng-project",
        "--catalog_name": "glue_catalog",
        "--bronze_db": "bronze",
        "--silver_db": "silver"
    }' \
    --glue-version "4.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --timeout 60 \
    --profile allen-dev
