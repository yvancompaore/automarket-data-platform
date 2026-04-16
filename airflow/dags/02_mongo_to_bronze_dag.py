"""
DAG 02 — MongoDB → Parquet Bronze (local, sans Spark ni AWS)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="02_mongo_to_bronze",
    description="Export MongoDB → Parquet Bronze (local)",
    default_args=default_args,
    schedule_interval="30 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "mongodb"],
) as dag:

    def _export_collection(collection: str, **ctx):
        import sys, os, json
        import pandas as pd
        from pathlib import Path
        from pymongo import MongoClient

        mongo_uri = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017")
        mongo_db  = os.getenv("MONGO_DB", "automarket_raw")
        data_dir  = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
        bronze    = data_dir / "bronze"
        bronze.mkdir(parents=True, exist_ok=True)

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        docs = list(db[collection].find({}, {"_id": 0, "_raw": 0}))
        client.close()

        if not docs:
            print(f"{collection} vide, skip")
            return 0

        df = pd.DataFrame(docs)
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (list, dict)) else x
                )

        out = bronze / f"{collection}.parquet"
        df.to_parquet(out, index=False)
        print(f"✓ {collection}: {len(df)} docs → {out}")
        return len(df)

    start = EmptyOperator(task_id="start")
    export_parts    = PythonOperator(task_id="export_parts_catalog",
                                     python_callable=_export_collection,
                                     op_kwargs={"collection": "parts_catalog"})
    export_listings = PythonOperator(task_id="export_market_listings",
                                     python_callable=_export_collection,
                                     op_kwargs={"collection": "market_listings"})
    end = EmptyOperator(task_id="end")

    start >> [export_parts, export_listings] >> end
