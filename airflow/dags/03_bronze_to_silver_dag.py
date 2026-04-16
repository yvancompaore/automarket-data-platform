"""
DAG 03 — Bronze → Silver (nettoyage Pandas, sans PySpark ni AWS)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="03_bronze_to_silver",
    description="Nettoyage Bronze → Silver Parquet (local)",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "transform"],
) as dag:

    def _check_bronze(**ctx):
        import os
        from pathlib import Path
        data_dir = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
        ok = (data_dir / "bronze" / "parts_catalog.parquet").exists()
        print(f"Bronze disponible : {ok}")
        return ok

    def _clean_parts(**ctx):
        import os, pandas as pd
        from pathlib import Path

        data_dir = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
        df = pd.read_parquet(data_dir / "bronze" / "parts_catalog.parquet")
        print(f"Lu {len(df)} lignes")

        df = df.dropna(subset=["part_id", "part_number"])
        df = df[df["part_number"].str.strip() != ""]
        df["part_number"] = df["part_number"].str.strip().str.upper()
        df["brand"]       = df["brand"].str.strip().str.title()
        df["source"]      = df["source"].str.strip().str.lower()
        df["availability"]= df["availability"].str.strip().str.lower()

        df["price_eur"] = pd.to_numeric(df["price_eur"], errors="coerce")
        df.loc[(df["price_eur"] <= 0) | (df["price_eur"] > 10000), "price_eur"] = None

        df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")
        df = df.sort_values("ingested_at", ascending=False)
        df = df.drop_duplicates(subset=["_fingerprint"], keep="first")
        df["is_available"]   = df["availability"] == "in_stock"
        df["ingested_year"]  = df["ingested_at"].dt.year
        df["ingested_month"] = df["ingested_at"].dt.month

        out = data_dir / "silver" / "parts_catalog.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
        print(f"✓ Silver parts: {len(df)} lignes → {out}")

    def _clean_listings(**ctx):
        import os, pandas as pd
        from pathlib import Path

        data_dir = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
        df = pd.read_parquet(data_dir / "bronze" / "market_listings.parquet")

        df = df.dropna(subset=["listing_id"]).drop_duplicates(subset=["listing_id"])
        df["asking_price_eur"]     = pd.to_numeric(df["asking_price_eur"], errors="coerce")
        df["negotiated_price_eur"] = pd.to_numeric(df["negotiated_price_eur"], errors="coerce")
        df = df[df["asking_price_eur"] > 0]
        df["vehicle_make"] = df["vehicle_make"].str.strip().str.title()
        df["brand"]        = df["brand"].str.strip().str.title()
        df["condition"]    = df["condition"].str.strip().str.lower()
        df["seller_type"]  = df["seller_type"].str.strip().str.lower()
        df["discount_rate"] = (
            (df["asking_price_eur"] - df["negotiated_price_eur"]) / df["asking_price_eur"]
        ).round(4)
        df["is_fast_sale"] = df["days_to_sell"].apply(
            lambda x: True if pd.notna(x) and x <= 7 else False
        )
        df["ingested_at"]    = pd.to_datetime(df["ingested_at"], errors="coerce")
        df["ingested_year"]  = df["ingested_at"].dt.year
        df["ingested_month"] = df["ingested_at"].dt.month

        out = data_dir / "silver" / "market_listings.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
        print(f"✓ Silver listings: {len(df)} lignes → {out}")

    start         = EmptyOperator(task_id="start")
    check_bronze  = ShortCircuitOperator(task_id="check_bronze", python_callable=_check_bronze)
    clean_parts   = PythonOperator(task_id="clean_parts",    python_callable=_clean_parts)
    clean_listings= PythonOperator(task_id="clean_listings", python_callable=_clean_listings)
    end           = EmptyOperator(task_id="end")

    start >> check_bronze >> [clean_parts, clean_listings] >> end
