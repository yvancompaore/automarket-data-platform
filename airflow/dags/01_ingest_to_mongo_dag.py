"""
DAG 01 — Ingestion sources → MongoDB (local, sans AWS)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_ingest_to_mongo",
    description="Ingestion → MongoDB Raw Landing Zone",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "mongodb"],
) as dag:

    def _load_parts(**ctx):
        import sys; sys.path.insert(0, "/opt/airflow")
        import os
        from ingestion.mongo_loader import get_mongo_client, load_parts_catalog
        client = get_mongo_client()
        db = client[os.getenv("MONGO_DB", "automarket_raw")]
        stats = load_parts_catalog(db, n=5000)
        client.close()
        ctx["ti"].xcom_push(key="stats", value=stats)

    def _load_listings(**ctx):
        import sys; sys.path.insert(0, "/opt/airflow")
        import os
        from ingestion.mongo_loader import get_mongo_client, load_market_listings
        client = get_mongo_client()
        db = client[os.getenv("MONGO_DB", "automarket_raw")]
        stats = load_market_listings(db, n=2000)
        client.close()
        ctx["ti"].xcom_push(key="stats", value=stats)

    def _summary(**ctx):
        ti = ctx["ti"]
        p = ti.xcom_pull("load_parts",    key="stats") or {}
        l = ti.xcom_pull("load_listings", key="stats") or {}
        print(f"Parts: {p.get('total',0)} | Listings: {l.get('total',0)}")

    start = EmptyOperator(task_id="start")
    load_parts    = PythonOperator(task_id="load_parts",    python_callable=_load_parts)
    load_listings = PythonOperator(task_id="load_listings", python_callable=_load_listings)
    summary       = PythonOperator(task_id="summary", python_callable=_summary,
                                   trigger_rule=TriggerRule.ALL_DONE)
    end = EmptyOperator(task_id="end")

    start >> [load_parts, load_listings] >> summary >> end
