import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Spark version:", spark.version)
print("Bronze to Silver ETL Job - Initialization complete!")

# Parse job arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bucket_name",
        "catalog_name",
        "bronze_db",
        "silver_db",
    ],
)

BUCKET_NAME = args["bucket_name"]
CATALOG_NAME = args["catalog_name"]
BRONZE_DB = args["bronze_db"]
SILVER_DB = args["silver_db"]


def create_silver_database():
    """Create Silver database if not exists"""
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{SILVER_DB} LOCATION 's3://{BUCKET_NAME}/{SILVER_DB}/'"
    )
    print(f"✅ Silver database ready: {CATALOG_NAME}.{SILVER_DB}")


def create_dim_products():
    """
    Create dim_products - Product Dimension (Denormalized)
    Merge products + aisles + departments into one table to avoid multi-table JOINs at query time
    """
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {CATALOG_NAME}.{SILVER_DB}.dim_products
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    AS
    SELECT 
        p.product_id,
        p.product_name,
        p.aisle_id,
        a.aisle AS aisle_name,
        p.department_id,
        d.department AS department_name,
        current_timestamp() AS audit_transform_time
    FROM {CATALOG_NAME}.{BRONZE_DB}.products p
    LEFT JOIN {CATALOG_NAME}.{BRONZE_DB}.aisles a ON p.aisle_id = a.aisle_id
    LEFT JOIN {CATALOG_NAME}.{BRONZE_DB}.departments d ON p.department_id = d.department_id
    """
    )
    print(f"✅ Created {CATALOG_NAME}.{SILVER_DB}.dim_products")


def create_dim_users():
    """
    Create dim_users - User Dimension (Aggregated)
    Aggregate user features from orders table to avoid real-time computation at query time
    """
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {CATALOG_NAME}.{SILVER_DB}.dim_users
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    AS
    WITH first_order AS (
        SELECT 
            user_id,
            order_dow AS first_order_dow,
            order_hour_of_day AS first_order_hour
        FROM {CATALOG_NAME}.{BRONZE_DB}.orders
        WHERE order_number = 1
    ),
    last_order AS (
        SELECT 
            o.user_id,
            o.order_dow AS last_order_dow,
            o.order_hour_of_day AS last_order_hour
        FROM {CATALOG_NAME}.{BRONZE_DB}.orders o
        INNER JOIN (
            SELECT user_id, MAX(order_number) AS max_order_number
            FROM {CATALOG_NAME}.{BRONZE_DB}.orders
            GROUP BY user_id
        ) m ON o.user_id = m.user_id AND o.order_number = m.max_order_number
    )
    SELECT 
        o.user_id,
        COUNT(o.order_id) AS total_orders,
        MAX(o.order_number) AS max_order_number,
        AVG(o.days_since_prior_order) AS avg_days_between_orders,
        MIN(o.days_since_prior_order) AS min_days_between_orders,
        MAX(o.days_since_prior_order) AS max_days_between_orders,
        f.first_order_dow,
        f.first_order_hour,
        l.last_order_dow,
        l.last_order_hour,
        current_timestamp() AS audit_transform_time
    FROM {CATALOG_NAME}.{BRONZE_DB}.orders o
    LEFT JOIN first_order f ON o.user_id = f.user_id
    LEFT JOIN last_order l ON o.user_id = l.user_id
    GROUP BY o.user_id, f.first_order_dow, f.first_order_hour, l.last_order_dow, l.last_order_hour
    """
    )
    print(f"✅ Created {CATALOG_NAME}.{SILVER_DB}.dim_users")



def create_fact_orders():
    """
    Create fact_orders - Orders Fact Table (With Pre-computed Metrics)
    """
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {CATALOG_NAME}.{SILVER_DB}.fact_orders
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    AS
    WITH order_metrics AS (
        SELECT 
            order_id,
            COUNT(product_id) AS total_products,
            SUM(reordered) AS total_reordered
        FROM {CATALOG_NAME}.{BRONZE_DB}.order_products
        GROUP BY order_id
    )
    SELECT 
        o.order_id,
        o.user_id,
        o.eval_set,
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order,
        m.total_products,
        m.total_reordered,
        CASE 
            WHEN m.total_products > 0 THEN m.total_reordered / m.total_products 
            ELSE 0 
        END AS reorder_ratio,
        current_timestamp() AS audit_transform_time
    FROM {CATALOG_NAME}.{BRONZE_DB}.orders o
    LEFT JOIN order_metrics m ON o.order_id = m.order_id
    """
    )
    print(f"✅ Created {CATALOG_NAME}.{SILVER_DB}.fact_orders")


def create_fact_order_products():
    """
    Create fact_order_products - Order-Product Fact Table (Factless Fact)
    This is a Factless Fact Table with no traditional measures (amount/quantity),
    but records the "purchase" business event
    """
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {CATALOG_NAME}.{SILVER_DB}.fact_order_products
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    AS
    SELECT 
        op.order_id,
        op.product_id,
        o.user_id,
        op.add_to_cart_order,
        op.reordered,
        current_timestamp() AS audit_transform_time
    FROM {CATALOG_NAME}.{BRONZE_DB}.order_products op
    LEFT JOIN {CATALOG_NAME}.{BRONZE_DB}.orders o ON op.order_id = o.order_id
    """
    )
    print(f"✅ Created {CATALOG_NAME}.{SILVER_DB}.fact_order_products")


def verify_silver_tables():
    """Verify all Silver tables were created successfully"""
    print("\n" + "=" * 50)
    print("Silver Layer Tables Verification")
    print("=" * 50)

    tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SILVER_DB}").collect()
    print(f"\nTables in {CATALOG_NAME}.{SILVER_DB}:")
    for table in tables:
        table_name = table["tableName"]
        count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {CATALOG_NAME}.{SILVER_DB}.{table_name}"
        ).collect()[0]["cnt"]
        print(f"  - {table_name}: {count:,} rows")

    print("\n✅ Silver layer verification complete!")


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("Bronze to Silver ETL Job Starting")
    print("=" * 50)
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Catalog: {CATALOG_NAME}")
    print(f"Bronze DB: {BRONZE_DB}")
    print(f"Silver DB: {SILVER_DB}")
    print("=" * 50 + "\n")

    # Step 1: Create Silver database
    create_silver_database()

    # Step 2: Create Dimension tables
    print("\n--- Creating Dimension Tables ---")
    create_dim_products()
    create_dim_users()

    # Step 3: Create Fact tables
    print("\n--- Creating Fact Tables ---")
    create_fact_orders()
    create_fact_order_products()

    # Step 4: Verify all tables
    verify_silver_tables()

    print("\n" + "=" * 50)
    print("Bronze to Silver ETL Job Completed Successfully!")
    print("=" * 50)
