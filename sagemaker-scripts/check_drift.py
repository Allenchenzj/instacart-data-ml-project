import argparse
import boto3
import awswrangler as wr
import numpy as np
from datetime import datetime, timezone

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
    parser.add_argument("--psi-threshold", type=float, default=0.1)
    parser.add_argument("--sagemaker-role", default="arn:aws:iam::606476261726:role/instacart-sagemaker-role")
    parser.add_argument("--region", default="ap-southeast-2")
    return parser.parse_args()


def compute_psi(baseline: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    eps = 1e-6
    bin_edges = np.linspace(
        min(baseline.min(), current.min()),
        max(baseline.max(), current.max()),
        n_bins + 1,
    )
    baseline_pct = np.histogram(baseline, bins=bin_edges)[0] / len(baseline) + eps
    current_pct  = np.histogram(current,  bins=bin_edges)[0] / len(current)  + eps
    return float(np.sum((current_pct - baseline_pct) * np.log(current_pct / baseline_pct)))


def main():
    args = parse_args()
    athena_output = f"s3://{args.bucket_name}/athena-results/"
    cols = ", ".join(FEATURE_COLUMNS)

    print("Loading training_features (baseline)...")
    training_df = wr.athena.read_sql_query(
        f"SELECT {cols} FROM gold.training_features",
        database="gold",
        s3_output=athena_output,
    )
    print(f"Baseline samples: {len(training_df):,}")

    print("Loading serving_features (current)...")
    serving_df = wr.athena.read_sql_query(
        f"SELECT {cols} FROM gold.serving_features",
        database="gold",
        s3_output=athena_output,
    )
    print(f"Current samples: {len(serving_df):,}")

    print(f"\nComputing PSI (threshold={args.psi_threshold})...")
    drift_results = []
    drift_detected = False

    for col in FEATURE_COLUMNS:
        baseline = training_df[col].dropna().values
        current  = serving_df[col].dropna().values

        if len(baseline) == 0 or len(current) == 0:
            print(f"  SKIP  {col} (empty column)")
            continue

        psi = compute_psi(baseline, current)
        drift_results.append({"column": col, "psi": psi})

        if psi > args.psi_threshold:
            drift_detected = True
            print(f"  DRIFT {col}: PSI={psi:.4f}")
        else:
            print(f"  OK    {col}: PSI={psi:.4f}")

    max_psi = max(r["psi"] for r in drift_results) if drift_results else 0.0
    print(f"\nMax PSI: {max_psi:.4f} | Drift detected: {drift_detected}")

    if not drift_detected:
        print("No significant drift. Retrain not required.")
        return

    print("\nDrift detected. Submitting SageMaker Training Job...")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    job_name = f"instacart-reorder-training-drift-{timestamp}"

    sm = boto3.client("sagemaker", region_name=args.region)
    sm.create_training_job(
        TrainingJobName=job_name,
        RoleArn=args.sagemaker_role,
        AlgorithmSpecification={
            "TrainingImage": "683313688378.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3",
            "TrainingInputMode": "File",
        },
        HyperParameters={
            "bucket-name": args.bucket_name,
            "model-name": "instacart-reorder-model",
            "mlflow-tracking-uri": "arn:aws:sagemaker:ap-southeast-2:606476261726:mlflow-tracking-server/instacart-mlflow",
            "pr-auc-gate": "0.35",
            "sample-users": "10000",
            "sagemaker_program": "train.py",
            "sagemaker_submit_directory": f"s3://{args.bucket_name}/scripts/",
        },
        OutputDataConfig={"S3OutputPath": f"s3://{args.bucket_name}/sagemaker-output/"},
        ResourceConfig={
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 20,
        },
        StoppingCondition={"MaxRuntimeInSeconds": 3600},
    )
    print(f"✅ Retrain job submitted: {job_name}")


if __name__ == "__main__":
    main()
