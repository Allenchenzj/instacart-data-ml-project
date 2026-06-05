import argparse
import awswrangler as wr
import mlflow
import mlflow.xgboost
import xgboost as xgb
import pandas as pd
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score

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
    parser.add_argument("--pr-auc-gate", type=float, default=0.35)
    parser.add_argument("--sample-users", type=int, default=10000)
    return parser.parse_args()


def main():
    args = parse_args()

    athena_output = f"s3://{args.bucket_name}/athena-results/"

    mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    mlflow.set_experiment("instacart-reorder-training")

    print("Loading training data from Athena...")
    df = wr.athena.read_sql_query(
        f"SELECT user_id, product_id, {', '.join(FEATURE_COLUMNS)}, label FROM gold.training_features",
        database="gold",
        s3_output=athena_output,
    )
    print(f"Full dataset: {len(df):,} rows, {df['user_id'].nunique():,} users")

    sampled_users = df["user_id"].drop_duplicates().sample(n=args.sample_users, random_state=42)
    df = df[df["user_id"].isin(sampled_users)].reset_index(drop=True)
    print(f"After sampling {args.sample_users} users: {len(df):,} rows")
    print(f"Label distribution: {df['label'].value_counts().to_dict()}")

    splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    train_idx, test_idx = next(splitter.split(df, groups=df["user_id"]))

    X_train = df.iloc[train_idx][FEATURE_COLUMNS].values
    y_train = df.iloc[train_idx]["label"].values
    X_test  = df.iloc[test_idx][FEATURE_COLUMNS].values
    y_test  = df.iloc[test_idx]["label"].values

    scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
    print(f"Train: {len(X_train):,} samples | Test: {len(X_test):,} samples")
    print(f"scale_pos_weight: {scale_pos_weight:.2f}")

    with mlflow.start_run() as run:
        model = xgb.XGBClassifier(
            objective="binary:logistic",
            eval_metric="aucpr",
            max_depth=6,
            learning_rate=0.1,
            n_estimators=200,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            tree_method="hist",
            random_state=42,
            scale_pos_weight=scale_pos_weight,
        )
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        y_pred       = (y_pred_proba >= 0.5).astype(int)
        pr_auc    = average_precision_score(y_test, y_pred_proba)
        f1        = f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall    = recall_score(y_test, y_pred)

        mlflow.log_params({
            "max_depth": 6,
            "learning_rate": 0.1,
            "n_estimators": 200,
            "sample_users": args.sample_users,
            "scale_pos_weight": round(scale_pos_weight, 2),
            "test_size": 0.2,
        })
        mlflow.log_metrics({"pr_auc": pr_auc, "f1": f1, "precision": precision, "recall": recall})

        print(f"PR-AUC={pr_auc:.4f}  F1={f1:.4f}  Precision={precision:.4f}  Recall={recall:.4f}")

        if pr_auc >= args.pr_auc_gate:
            print(f"✅ PASSED gate ({pr_auc:.4f} >= {args.pr_auc_gate})")
            mlflow.xgboost.log_model(
                model,
                name="model",
                input_example=pd.DataFrame(X_test[:5], columns=FEATURE_COLUMNS),
                registered_model_name=args.model_name,
            )
            client = mlflow.tracking.MlflowClient()
            versions = client.search_model_versions(f"name='{args.model_name}' and run_id='{run.info.run_id}'")
            latest_version = versions[0].version
            client.set_registered_model_alias(args.model_name, "Champion", latest_version)
            print(f"Model v{latest_version} registered as '{args.model_name}' with alias 'Champion'")
        else:
            print(f"❌ FAILED gate ({pr_auc:.4f} < {args.pr_auc_gate}). Model NOT registered.")
            mlflow.xgboost.log_model(model, name="model_failed_gate")


if __name__ == "__main__":
    main()
