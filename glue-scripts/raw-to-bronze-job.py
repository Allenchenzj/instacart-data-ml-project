from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.types import *

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Spark version:", spark.version)
print("Initialization complete!")

BUCKET_NAME = "instacart-data-eng-project"
CATALOG_NAME = "glue_catalog"
DATABASE_NAME = "bronze"

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket_name", "catalog_name", "database_name", "is_full_refresh", "ingest_date"],
)

BUCKET_NAME = args["bucket_name"]
CATALOG_NAME = args["catalog_name"]
DATABASE_NAME = args["database_name"]
IS_FULL_REFRESH = args["is_full_refresh"]
INGEST_DATE = args["ingest_date"]
INGEST_YEAR, INGEST_MONTH, INGEST_DAY = INGEST_DATE.split("-")

def read_csv_from_raw(table_name):
    """
    read CSV files from S3 raw layer
    param table_name: table name (corresponding to folder name in raw layer)
    return:
        DataFrame
    """
    if IS_FULL_REFRESH.lower() == "true":
        path = f"s3://{BUCKET_NAME}/raw/{table_name}/"
    else:
        path = f"s3://{BUCKET_NAME}/raw/{table_name}/year={INGEST_YEAR}/month={INGEST_MONTH}/day={INGEST_DAY}/"

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    return df


def write_to_bronze_iceberg_full(df, table_name):
    """
    Write DataFrame to Bronze Iceberg table

    param df: DataFrame to write
    param table_name: table name
    """
    full_table_name = f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}"

    table_location = f"s3://{BUCKET_NAME}/bronze/{table_name}"

    df.writeTo(full_table_name).tableProperty("format-version", "2").tableProperty(
        "location", table_location
    ).createOrReplace()

    print(f"✅ Successfully wrote to {full_table_name}")
    print(f"   Location: {table_location}")


def write_to_bronze_iceberg_incremental(spark, df, table_name):
    """Write DataFrame to Iceberg table (incremental - append)."""
    full_table_name = f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}"

    df.writeTo(full_table_name).option("check-ordering", "false").append()


if __name__ == "__main__":
    tables = ["orders", "products", "aisles", "departments", "order_products"]

    for table in tables:
        df = read_csv_from_raw(table)
        if IS_FULL_REFRESH.lower() == "true":
            write_to_bronze_iceberg_full(df, table)
        else:
            write_to_bronze_iceberg_incremental(spark, df, table)
