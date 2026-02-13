#!/bin/bash

# Upload script to S3
aws s3 cp glue-scripts/raw-to-bronze-job.py s3://instacart-data-eng-project/scripts/raw-to-bronze-job.py

# Create Glue job
aws glue create-job \
    --name "raw-to-bronze-job" \
    --role "arn:aws:iam::471112945220:role/instacart-glue-service-role" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://instacart-data-eng-project/scripts/raw-to-bronze-job.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--job-language": "python",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-glue-datacatalog": "true",
        "--datalake-formats": "iceberg",
        "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://instacart-data-eng-project/warehouse --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--bucket_name": "instacart-data-eng-project",
        "--catalog_name": "glue_catalog",
        "--database_name": "bronze",
        "--is_full_refresh": "true",
        "--ingest_date": "2026-02-13"
    }' \
    --glue-version "4.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --timeout 60
