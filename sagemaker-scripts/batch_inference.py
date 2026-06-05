import argparse
import awswrangler as wr
import mlflow
import mlflow.xgboost
import pandas as pd
from datetime import date

FEATURE_COLUMNS = [
    "user_product_buy_count",
    "user_product_last_order_in_window",
    "user_product_avg_add_to_cart",
    "user_product_orders_since_last_buy",
    "user_window_orders",
    "user_avg_days_between",
    "user_total_orders",
    "user_product_window_reorder_ratio",
    "item_total_sales",
    "item_reorder_rate",
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", default="instacart-aws-data-ml-eng-project")
    parser.add_argument("--model-name", default="instacart-reorder-model")
    parser.add_argument("--mlflow-tracking-uri", default="arn:aws:sagemaker:ap-southeast-2:606476261726:mlflow-tracking-server/instacart-mlflow")
    parser.add_argument("--top-n", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()

    athena_output = f"s3://{args.bucket_name}/athena-results/"

    mlflow.set_tracking_uri(args.mlflow_tracking_uri)

    print("Loading Champion model from MLflow...")
    client = mlflow.tracking.MlflowClient()
    champion = client.get_model_version_by_alias(args.model_name, "Champion")
    model_version = int(champion.version)
    loaded_model = mlflow.xgboost.load_model(f"models:/{args.model_name}@Champion")
    print(f"Loaded model '{args.model_name}' version {model_version} (run_id={champion.run_id})")

    print("Loading serving features from Athena...")
    serving_df = wr.athena.read_sql_query(
        f"SELECT user_id, product_id, {', '.join(FEATURE_COLUMNS)} FROM gold.serving_features",
        database="gold",
        s3_output=athena_output,
    )
    print(f"Scoring {len(serving_df):,} user-product pairs")

    serving_df["reorder_probability"] = loaded_model.predict_proba(serving_df[FEATURE_COLUMNS])[:, 1]
    serving_df["model_version"] = model_version
    serving_df["prediction_date"] = str(date.today())

    predictions_df = serving_df[["user_id", "product_id", "reorder_probability", "model_version", "prediction_date"]]
    print(f"Score range: {predictions_df['reorder_probability'].min():.4f} – {predictions_df['reorder_probability'].max():.4f}")

    print("Writing predictions_audit...")
    wr.athena.to_iceberg(
        df=predictions_df,
        database="gold",
        table="predictions_audit",
        temp_path=f"{athena_output}temp/",
        table_location=f"s3://{args.bucket_name}/gold/predictions_audit/",
        keep_files=False,
        partition_cols=["prediction_date"],
        mode="append",
    )
    print(f"✅ predictions_audit: {len(predictions_df):,} rows written (partition={date.today()})")

    print("Writing predictions_top_n...")
    top_n_df = (
        predictions_df
        .sort_values("reorder_probability", ascending=False)
        .groupby("user_id")
        .head(args.top_n)
        .reset_index(drop=True)
    )
    wr.athena.to_iceberg(
        df=top_n_df,
        database="gold",
        table="predictions_top_n",
        temp_path=f"{athena_output}temp/",
        table_location=f"s3://{args.bucket_name}/gold/predictions_top_n/",
        keep_files=False,
        mode="overwrite",
    )
    print(f"✅ predictions_top_n: {len(top_n_df):,} rows written (top {args.top_n} per user)")


if __name__ == "__main__":
    main()
