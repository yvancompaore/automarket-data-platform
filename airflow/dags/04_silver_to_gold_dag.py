"""
DAG 04 — Silver → Gold (DuckDB, sans dbt ni AWS)
Construit les marts analytiques directement en SQL via DuckDB.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="04_silver_to_gold",
    description="Construction des marts Gold via DuckDB (local)",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "duckdb", "analytics"],
) as dag:

    def _build_gold(**ctx):
        import os, duckdb
        from pathlib import Path

        data_dir = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
        silver_parts    = str(data_dir / "silver" / "parts_catalog.parquet")
        silver_listings = str(data_dir / "silver" / "market_listings.parquet")
        gold_dir = data_dir / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(str(gold_dir / "automarket.duckdb"))

        # ── mart_market_pricing ──────────────────────────────
        con.execute(f"""
        CREATE OR REPLACE TABLE mart_market_pricing AS
        WITH catalog_prices AS (
            SELECT category, brand, brand_type,
                COUNT(*)                     AS nb_references,
                COUNT(CASE WHEN is_available THEN 1 END) AS nb_available,
                ROUND(AVG(price_eur), 2)     AS catalog_avg_price,
                ROUND(MEDIAN(price_eur), 2)  AS catalog_median_price,
                ROUND(MIN(price_eur), 2)     AS catalog_min_price,
                ROUND(MAX(price_eur), 2)     AS catalog_max_price,
                ROUND(AVG(quality_score), 3) AS avg_quality_score
            FROM read_parquet('{silver_parts}')
            WHERE price_eur IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        market_prices AS (
            SELECT category, brand,
                COUNT(*)                            AS nb_listings,
                ROUND(AVG(asking_price_eur), 2)     AS market_avg_asking,
                ROUND(MEDIAN(asking_price_eur), 2)  AS market_median_price,
                ROUND(AVG(discount_rate) * 100, 2)  AS avg_discount_pct,
                ROUND(AVG(days_to_sell), 1)         AS avg_days_to_sell,
                COUNT(CASE WHEN is_fast_sale THEN 1 END) AS nb_fast_sales
            FROM read_parquet('{silver_listings}')
            GROUP BY 1, 2
        )
        SELECT
            COALESCE(cp.category, mp.category)  AS category,
            COALESCE(cp.brand, mp.brand)        AS brand,
            cp.brand_type,
            COALESCE(cp.nb_references, 0)       AS nb_catalog_references,
            COALESCE(cp.nb_available, 0)        AS nb_catalog_available,
            cp.catalog_avg_price,
            cp.catalog_median_price,
            cp.catalog_min_price,
            cp.catalog_max_price,
            cp.avg_quality_score,
            COALESCE(mp.nb_listings, 0)         AS nb_market_listings,
            mp.market_avg_asking,
            mp.market_median_price,
            mp.avg_discount_pct,
            mp.avg_days_to_sell,
            COALESCE(mp.nb_fast_sales, 0)       AS nb_fast_sales,
            CASE
                WHEN cp.catalog_median_price > 0 AND mp.market_median_price > 0
                THEN ROUND((mp.market_median_price - cp.catalog_median_price)
                           / cp.catalog_median_price * 100, 2)
            END AS market_vs_catalog_pct,
            CASE
                WHEN mp.avg_days_to_sell <= 7  THEN 'haute'
                WHEN mp.avg_days_to_sell <= 21 THEN 'moyenne'
                ELSE 'faible'
            END AS market_liquidity,
            CURRENT_TIMESTAMP AS updated_at
        FROM catalog_prices cp
        FULL OUTER JOIN market_prices mp ON cp.category = mp.category AND cp.brand = mp.brand
        ORDER BY nb_catalog_references DESC
        """)
        n = con.execute("SELECT COUNT(*) FROM mart_market_pricing").fetchone()[0]
        print(f"✓ mart_market_pricing : {n} lignes")

        # ── mart_catalog_coverage ────────────────────────────
        con.execute(f"""
        CREATE OR REPLACE TABLE mart_catalog_coverage AS
        WITH parts_by_vehicle AS (
            SELECT vehicle_make, vehicle_model, vehicle_year, category,
                COUNT(DISTINCT part_number)         AS nb_distinct_parts,
                COUNT(*)                            AS nb_listings,
                ROUND(AVG(asking_price_eur), 2)     AS avg_price,
                COUNT(DISTINCT brand)               AS nb_brands
            FROM read_parquet('{silver_listings}')
            GROUP BY 1, 2, 3, 4
        )
        SELECT vehicle_make, vehicle_model,
            COUNT(DISTINCT vehicle_year)                        AS years_covered,
            COUNT(DISTINCT category)                            AS categories_covered,
            SUM(nb_distinct_parts)                              AS total_parts,
            SUM(nb_listings)                                    AS total_listings,
            ROUND(AVG(avg_price), 2)                            AS global_avg_price,
            ROUND(COUNT(DISTINCT category) * 100.0 / 7, 1)     AS coverage_score_pct,
            CASE
                WHEN COUNT(DISTINCT category) * 100.0 / 7 >= 85 THEN 'couverture complète'
                WHEN COUNT(DISTINCT category) * 100.0 / 7 >= 60 THEN 'bonne couverture'
                WHEN COUNT(DISTINCT category) * 100.0 / 7 >= 40 THEN 'couverture partielle'
                ELSE 'couverture faible'
            END AS coverage_level,
            CURRENT_TIMESTAMP AS updated_at
        FROM parts_by_vehicle
        GROUP BY 1, 2
        ORDER BY total_parts DESC
        """)
        n = con.execute("SELECT COUNT(*) FROM mart_catalog_coverage").fetchone()[0]
        print(f"✓ mart_catalog_coverage : {n} lignes")

        # ── mart_supplier_performance ────────────────────────
        con.execute(f"""
        CREATE OR REPLACE TABLE mart_supplier_performance AS
        SELECT
            supplier, brand_type,
            COUNT(DISTINCT part_number)     AS nb_unique_refs,
            COUNT(DISTINCT category)        AS nb_categories,
            COUNT(DISTINCT brand)           AS nb_brands,
            ROUND(AVG(price_eur), 2)        AS avg_price,
            ROUND(AVG(quality_score), 3)    AS avg_quality_score,
            COUNT(CASE WHEN is_available THEN 1 END) AS nb_available_refs,
            ROUND(COUNT(CASE WHEN is_available THEN 1 END) * 100.0 / NULLIF(COUNT(*),0), 1) AS availability_rate_pct,
            CASE
                WHEN AVG(quality_score) >= 0.8
                 AND COUNT(CASE WHEN is_available THEN 1 END)*100.0/NULLIF(COUNT(*),0) >= 80
                    THEN 'Tier 1 — Stratégique'
                WHEN AVG(quality_score) >= 0.6 THEN 'Tier 2 — Fiable'
                WHEN AVG(quality_score) >= 0.4 THEN 'Tier 3 — À surveiller'
                ELSE 'Tier 4 — Risqué'
            END AS supplier_tier,
            ROUND(AVG(quality_score) * 100, 1) AS supplier_score,
            CURRENT_TIMESTAMP AS updated_at
        FROM read_parquet('{silver_parts}')
        WHERE supplier IS NOT NULL
        GROUP BY 1, 2
        ORDER BY supplier_score DESC
        """)
        n = con.execute("SELECT COUNT(*) FROM mart_supplier_performance").fetchone()[0]
        print(f"✓ mart_supplier_performance : {n} lignes")

        # Export Parquet pour le dashboard
        for mart in ["mart_market_pricing", "mart_catalog_coverage", "mart_supplier_performance"]:
            out = gold_dir / f"{mart}.parquet"
            con.execute(f"COPY {mart} TO '{out}' (FORMAT PARQUET)")

        con.close()
        print("=== Gold layer construit ✓ ===")

    def _validate(**ctx):
        import os, duckdb
        from pathlib import Path
        data_dir = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
        con = duckdb.connect(str(data_dir / "gold" / "automarket.duckdb"), read_only=True)
        issues = []
        # Test : pas de prix négatif
        neg = con.execute("SELECT COUNT(*) FROM mart_market_pricing WHERE catalog_median_price < 0").fetchone()[0]
        if neg > 0:
            issues.append(f"Prix négatifs : {neg}")
        # Test : couverture coverage_score_pct entre 0 et 100
        oob = con.execute("SELECT COUNT(*) FROM mart_catalog_coverage WHERE coverage_score_pct < 0 OR coverage_score_pct > 100").fetchone()[0]
        if oob > 0:
            issues.append(f"coverage_score hors bornes : {oob}")
        con.close()
        if issues:
            raise ValueError(f"Validation échouée : {issues}")
        print("✓ Toutes les validations passent")

    start    = EmptyOperator(task_id="start")
    build    = PythonOperator(task_id="build_gold",  python_callable=_build_gold)
    validate = PythonOperator(task_id="validate",    python_callable=_validate)
    end      = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> build >> validate >> end
