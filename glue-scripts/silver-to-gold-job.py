import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Spark version:", spark.version)
print("Silver to Gold ETL Job - Initialization complete!")

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bucket_name",
        "catalog_name",
        "silver_db",
        "gold_db",
    ],
)

BUCKET_NAME = args["bucket_name"]
CATALOG_NAME = args["catalog_name"]
SILVER_DB = args["silver_db"]
GOLD_DB = args["gold_db"]

MIN_ORDERS_REQUIRED = 5


def create_gold_database():
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{GOLD_DB} LOCATION 's3://{BUCKET_NAME}/{GOLD_DB}/'"
    )
    print(f"✅ Gold database ready: {CATALOG_NAME}.{GOLD_DB}")


def create_training_features():
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {CATALOG_NAME}.{GOLD_DB}.training_features
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    AS
    WITH user_order_bounds AS (
        SELECT
            user_id,
            MAX(order_number) AS max_order_number
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_orders
        GROUP BY user_id
        HAVING MAX(order_number) >= {MIN_ORDERS_REQUIRED}
    ),
    global_product_features AS (
        SELECT
            fop.product_id,
            COUNT(fop.order_id)                  AS item_total_sales,
            AVG(CAST(fop.reordered AS DOUBLE))   AS item_reorder_rate
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        GROUP BY fop.product_id
    ),
    train_feature_orders AS (
        SELECT
            fo.order_id,
            fo.user_id,
            fo.order_number,
            fo.days_since_prior_order,
            ub.max_order_number
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_orders fo
        INNER JOIN user_order_bounds ub ON fo.user_id = ub.user_id
        WHERE fo.order_number BETWEEN (ub.max_order_number - 4) AND (ub.max_order_number - 2)
    ),
    train_label_orders AS (
        SELECT fo.order_id, fo.user_id
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_orders fo
        INNER JOIN user_order_bounds ub ON fo.user_id = ub.user_id
        WHERE fo.order_number = (ub.max_order_number - 1)
    ),
    train_candidates AS (
        SELECT DISTINCT fop.user_id, fop.product_id
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        INNER JOIN train_feature_orders tfo ON fop.order_id = tfo.order_id
    ),
    train_positive_labels AS (
        SELECT DISTINCT fop.user_id, fop.product_id, 1 AS label
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        INNER JOIN train_label_orders tlo ON fop.order_id = tlo.order_id
    ),
    train_up_features AS (
        SELECT
            fop.user_id,
            fop.product_id,
            COUNT(fop.order_id)                                          AS user_product_buy_count,
            MAX(tfo.order_number)                                        AS user_product_last_order_in_window,
            AVG(fop.add_to_cart_order)                                   AS user_product_avg_add_to_cart,
            MAX(tfo.max_order_number) - 2 - MAX(tfo.order_number)        AS user_product_orders_since_last_buy
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        INNER JOIN train_feature_orders tfo ON fop.order_id = tfo.order_id
        GROUP BY fop.user_id, fop.product_id
    ),
    train_user_features AS (
        SELECT
            user_id,
            COUNT(DISTINCT order_id)         AS user_window_orders,
            AVG(days_since_prior_order)      AS user_avg_days_between,
            MAX(max_order_number)            AS user_total_orders
        FROM train_feature_orders
        GROUP BY user_id
    )
    SELECT
        tc.user_id,
        tc.product_id,
        up.user_product_buy_count,
        up.user_product_last_order_in_window,
        up.user_product_avg_add_to_cart,
        up.user_product_orders_since_last_buy,
        uf.user_window_orders,
        uf.user_avg_days_between,
        uf.user_total_orders,
        CASE
            WHEN uf.user_window_orders > 0 THEN up.user_product_buy_count / uf.user_window_orders
            ELSE 0
        END AS user_product_window_reorder_ratio,
        gpf.item_total_sales,
        gpf.item_reorder_rate,
        CAST(COALESCE(tpl.label, 0) AS INT) AS label,
        current_timestamp() AS audit_transform_time
    FROM train_candidates tc
    INNER JOIN train_up_features up
        ON tc.user_id = up.user_id AND tc.product_id = up.product_id
    INNER JOIN train_user_features uf
        ON tc.user_id = uf.user_id
    LEFT JOIN global_product_features gpf
        ON tc.product_id = gpf.product_id
    LEFT JOIN train_positive_labels tpl
        ON tc.user_id = tpl.user_id AND tc.product_id = tpl.product_id
    """
    )
    print(f"✅ Created {CATALOG_NAME}.{GOLD_DB}.training_features")


def create_serving_features():
    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {CATALOG_NAME}.{GOLD_DB}.serving_features
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    AS
    WITH user_order_bounds AS (
        SELECT
            user_id,
            MAX(order_number) AS max_order_number
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_orders
        GROUP BY user_id
        HAVING MAX(order_number) >= {MIN_ORDERS_REQUIRED}
    ),
    global_product_features AS (
        SELECT
            fop.product_id,
            COUNT(fop.order_id)                  AS item_total_sales,
            AVG(CAST(fop.reordered AS DOUBLE))   AS item_reorder_rate
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        GROUP BY fop.product_id
    ),
    serve_feature_orders AS (
        SELECT
            fo.order_id,
            fo.user_id,
            fo.order_number,
            fo.days_since_prior_order,
            ub.max_order_number
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_orders fo
        INNER JOIN user_order_bounds ub ON fo.user_id = ub.user_id
        WHERE fo.order_number BETWEEN (ub.max_order_number - 3) AND (ub.max_order_number - 1)
    ),
    serve_candidates AS (
        SELECT DISTINCT fop.user_id, fop.product_id
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        INNER JOIN serve_feature_orders sfo ON fop.order_id = sfo.order_id
    ),
    serve_up_features AS (
        SELECT
            fop.user_id,
            fop.product_id,
            COUNT(fop.order_id)                                          AS user_product_buy_count,
            MAX(sfo.order_number)                                        AS user_product_last_order_in_window,
            AVG(fop.add_to_cart_order)                                   AS user_product_avg_add_to_cart,
            MAX(sfo.max_order_number) - 1 - MAX(sfo.order_number)        AS user_product_orders_since_last_buy
        FROM {CATALOG_NAME}.{SILVER_DB}.fact_order_products fop
        INNER JOIN serve_feature_orders sfo ON fop.order_id = sfo.order_id
        GROUP BY fop.user_id, fop.product_id
    ),
    serve_user_features AS (
        SELECT
            user_id,
            COUNT(DISTINCT order_id)         AS user_window_orders,
            AVG(days_since_prior_order)      AS user_avg_days_between,
            MAX(max_order_number)            AS user_total_orders
        FROM serve_feature_orders
        GROUP BY user_id
    )
    SELECT
        sc.user_id,
        sc.product_id,
        up.user_product_buy_count,
        up.user_product_last_order_in_window,
        up.user_product_avg_add_to_cart,
        up.user_product_orders_since_last_buy,
        uf.user_window_orders,
        uf.user_avg_days_between,
        uf.user_total_orders,
        CASE
            WHEN uf.user_window_orders > 0 THEN up.user_product_buy_count / uf.user_window_orders
            ELSE 0
        END AS user_product_window_reorder_ratio,
        gpf.item_total_sales,
        gpf.item_reorder_rate,
        current_timestamp() AS audit_transform_time
    FROM serve_candidates sc
    INNER JOIN serve_up_features up
        ON sc.user_id = up.user_id AND sc.product_id = up.product_id
    INNER JOIN serve_user_features uf
        ON sc.user_id = uf.user_id
    LEFT JOIN global_product_features gpf
        ON sc.product_id = gpf.product_id
    """
    )
    print(f"✅ Created {CATALOG_NAME}.{GOLD_DB}.serving_features")


def verify_gold_tables():
    print("\n" + "=" * 50)
    print("Gold Layer Tables Verification")
    print("=" * 50)

    tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{GOLD_DB}").collect()
    print(f"\nTables in {CATALOG_NAME}.{GOLD_DB}:")
    for table in tables:
        table_name = table["tableName"]
        count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {CATALOG_NAME}.{GOLD_DB}.{table_name}"
        ).collect()[0]["cnt"]
        print(f"  - {table_name}: {count:,} rows")

    print("\n✅ Gold layer verification complete!")


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("Silver to Gold ETL Job Starting")
    print("=" * 50)
    print(f"Bucket:    {BUCKET_NAME}")
    print(f"Catalog:   {CATALOG_NAME}")
    print(f"Silver DB: {SILVER_DB}")
    print(f"Gold DB:   {GOLD_DB}")
    print(f"Min orders required (cold start filter): {MIN_ORDERS_REQUIRED}")
    print("=" * 50 + "\n")

    create_gold_database()

    print("\n--- Creating Gold Tables ---")
    create_training_features()
    create_serving_features()

    verify_gold_tables()

    print("\n" + "=" * 50)
    print("Silver to Gold ETL Job Completed Successfully!")
    print("=" * 50)
