"""
Pipeline local complet — sans AWS S3
MongoDB → Parquet local (bronze/silver) → DuckDB (gold) → dashboard

Usage : python scripts/run_pipeline_local.py
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Chemins locaux ───────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "data"
BRONZE_DIR  = DATA_DIR / "bronze"
SILVER_DIR  = DATA_DIR / "silver"
GOLD_DIR    = DATA_DIR / "gold"

for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "automarket_raw")


# ─── Étape 1 : Ingestion MongoDB ─────────────────────────────

def step_ingest(parts_count: int = 2000, listings_count: int = 500):
    logger.info("=== ÉTAPE 1 : Ingestion → MongoDB ===")
    from ingestion.mongo_loader import get_mongo_client, load_parts_catalog, load_market_listings
    from pymongo import MongoClient

    client = get_mongo_client()
    db = client[MONGO_DB]

    r1 = load_parts_catalog(db, n=parts_count)
    r2 = load_market_listings(db, n=listings_count)

    logger.info(f"Parts: {r1['inserted']} inserts | Listings: {r2['inserted']} inserts")
    client.close()
    return r1, r2


# ─── Étape 2 : MongoDB → Parquet Bronze ──────────────────────

def step_mongo_to_bronze():
    logger.info("=== ÉTAPE 2 : MongoDB → Parquet Bronze ===")
    import pandas as pd
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    for collection in ["parts_catalog", "market_listings"]:
        docs = list(db[collection].find({}, {"_id": 0, "_raw": 0}))
        if not docs:
            logger.warning(f"{collection} : vide, skip")
            continue

        df = pd.DataFrame(docs)

        # Sérialiser les colonnes liste/dict en JSON string pour Parquet
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    import json
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                    )
                except Exception:
                    pass

        out = BRONZE_DIR / f"{collection}.parquet"
        df.to_parquet(out, index=False)
        logger.info(f"✓ {collection}: {len(df)} docs → {out}")

    client.close()


# ─── Étape 3 : Bronze → Silver (nettoyage Python) ────────────

def step_bronze_to_silver():
    logger.info("=== ÉTAPE 3 : Bronze → Silver (nettoyage) ===")
    import pandas as pd
    import numpy as np

    # ── Parts catalog ──
    bronze_parts = BRONZE_DIR / "parts_catalog.parquet"
    if bronze_parts.exists():
        df = pd.read_parquet(bronze_parts)
        logger.info(f"Parts Bronze lu : {len(df)} lignes")

        # Nettoyage
        df = df.dropna(subset=["part_id", "part_number"])
        df = df[df["part_number"].str.strip() != ""]
        df["part_number"] = df["part_number"].str.strip().str.upper()
        df["brand"]        = df["brand"].str.strip().str.title()
        df["source"]       = df["source"].str.strip().str.lower()
        df["availability"] = df["availability"].str.strip().str.lower()

        # Prix hors bornes → null
        df["price_eur"] = pd.to_numeric(df["price_eur"], errors="coerce")
        df.loc[(df["price_eur"] <= 0) | (df["price_eur"] > 10000), "price_eur"] = None

        # Déduplication : garder dernière version par fingerprint
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")
        df = df.sort_values("ingested_at", ascending=False)
        df = df.drop_duplicates(subset=["_fingerprint"], keep="first")

        # Colonnes dérivées
        df["is_available"] = df["availability"] == "in_stock"
        df["ingested_year"]  = df["ingested_at"].dt.year
        df["ingested_month"] = df["ingested_at"].dt.month

        out = SILVER_DIR / "parts_catalog.parquet"
        df.to_parquet(out, index=False)
        logger.info(f"✓ Parts Silver : {len(df)} lignes → {out}")
    else:
        logger.warning("Bronze parts_catalog non trouvé")

    # ── Market listings ──
    bronze_listings = BRONZE_DIR / "market_listings.parquet"
    if bronze_listings.exists():
        df = pd.read_parquet(bronze_listings)
        logger.info(f"Listings Bronze lu : {len(df)} lignes")

        df = df.dropna(subset=["listing_id"])
        df = df.drop_duplicates(subset=["listing_id"])
        df["asking_price_eur"]      = pd.to_numeric(df["asking_price_eur"], errors="coerce")
        df["negotiated_price_eur"]  = pd.to_numeric(df["negotiated_price_eur"], errors="coerce")
        df = df[df["asking_price_eur"] > 0]
        df["vehicle_make"]  = df["vehicle_make"].str.strip().str.title()
        df["brand"]         = df["brand"].str.strip().str.title()
        df["category"]      = df["category"].str.strip()
        df["condition"]     = df["condition"].str.strip().str.lower()
        df["seller_type"]   = df["seller_type"].str.strip().str.lower()

        df["discount_rate"] = (
            (df["asking_price_eur"] - df["negotiated_price_eur"])
            / df["asking_price_eur"]
        ).round(4)
        df["is_fast_sale"] = df["days_to_sell"].apply(
            lambda x: True if pd.notna(x) and x <= 7 else False
        )
        df["ingested_at"]    = pd.to_datetime(df["ingested_at"], errors="coerce")
        df["ingested_year"]  = df["ingested_at"].dt.year
        df["ingested_month"] = df["ingested_at"].dt.month

        out = SILVER_DIR / "market_listings.parquet"
        df.to_parquet(out, index=False)
        logger.info(f"✓ Listings Silver : {len(df)} lignes → {out}")
    else:
        logger.warning("Bronze market_listings non trouvé")


# ─── Étape 4 : Silver → Gold (DuckDB) ────────────────────────

def step_silver_to_gold():
    logger.info("=== ÉTAPE 4 : Silver → Gold (DuckDB) ===")
    import duckdb

    con = duckdb.connect(str(GOLD_DIR / "automarket.duckdb"))
    silver_parts    = str(SILVER_DIR / "parts_catalog.parquet")
    silver_listings = str(SILVER_DIR / "market_listings.parquet")

    # ── mart_market_pricing ──
    con.execute(f"""
    CREATE OR REPLACE TABLE mart_market_pricing AS

    WITH catalog_prices AS (
        SELECT
            category,
            brand,
            brand_type,
            COUNT(*)                        AS nb_references,
            COUNT(CASE WHEN is_available THEN 1 END) AS nb_available,
            ROUND(AVG(price_eur), 2)        AS catalog_avg_price,
            ROUND(MEDIAN(price_eur), 2)     AS catalog_median_price,
            ROUND(MIN(price_eur), 2)        AS catalog_min_price,
            ROUND(MAX(price_eur), 2)        AS catalog_max_price,
            ROUND(AVG(quality_score), 3)    AS avg_quality_score
        FROM read_parquet('{silver_parts}')
        WHERE price_eur IS NOT NULL
        GROUP BY 1, 2, 3
    ),

    market_prices AS (
        SELECT
            category,
            brand,
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
        COALESCE(cp.category, mp.category)      AS category,
        COALESCE(cp.brand, mp.brand)            AS brand,
        cp.brand_type,
        COALESCE(cp.nb_references, 0)           AS nb_catalog_references,
        COALESCE(cp.nb_available, 0)            AS nb_catalog_available,
        cp.catalog_avg_price,
        cp.catalog_median_price,
        cp.catalog_min_price,
        cp.catalog_max_price,
        cp.avg_quality_score,
        COALESCE(mp.nb_listings, 0)             AS nb_market_listings,
        mp.market_avg_asking,
        mp.market_median_price,
        mp.avg_discount_pct,
        mp.avg_days_to_sell,
        COALESCE(mp.nb_fast_sales, 0)           AS nb_fast_sales,
        CASE
            WHEN cp.catalog_median_price > 0 AND mp.market_median_price > 0
            THEN ROUND((mp.market_median_price - cp.catalog_median_price)
                       / cp.catalog_median_price * 100, 2)
        END                                     AS market_vs_catalog_pct,
        CASE
            WHEN mp.avg_days_to_sell <= 7  THEN 'haute'
            WHEN mp.avg_days_to_sell <= 21 THEN 'moyenne'
            ELSE 'faible'
        END                                     AS market_liquidity,
        CURRENT_TIMESTAMP                       AS updated_at
    FROM catalog_prices cp
    FULL OUTER JOIN market_prices mp
        ON cp.category = mp.category AND cp.brand = mp.brand
    ORDER BY nb_catalog_references DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM mart_market_pricing").fetchone()[0]
    logger.info(f"✓ mart_market_pricing : {count} lignes")

    # ── mart_catalog_coverage ──
    con.execute(f"""
    CREATE OR REPLACE TABLE mart_catalog_coverage AS

    WITH parts_by_vehicle AS (
        SELECT
            vehicle_make,
            vehicle_model,
            vehicle_year,
            category,
            COUNT(DISTINCT part_number)                 AS nb_distinct_parts,
            COUNT(*)                                    AS nb_listings,
            ROUND(AVG(asking_price_eur), 2)             AS avg_price,
            COUNT(DISTINCT brand)                       AS nb_brands
        FROM read_parquet('{silver_listings}')
        GROUP BY 1, 2, 3, 4
    ),

    vehicle_totals AS (
        SELECT
            vehicle_make,
            vehicle_model,
            COUNT(DISTINCT vehicle_year)                AS years_covered,
            COUNT(DISTINCT category)                    AS categories_covered,
            SUM(nb_distinct_parts)                      AS total_parts,
            SUM(nb_listings)                            AS total_listings,
            ROUND(AVG(avg_price), 2)                    AS global_avg_price,
            ROUND(COUNT(DISTINCT category) * 100.0 / 7, 1) AS coverage_score_pct
        FROM parts_by_vehicle
        GROUP BY 1, 2
    )

    SELECT
        vehicle_make,
        vehicle_model,
        years_covered,
        categories_covered,
        total_parts,
        total_listings,
        global_avg_price,
        coverage_score_pct,
        CASE
            WHEN coverage_score_pct >= 85 THEN 'couverture complète'
            WHEN coverage_score_pct >= 60 THEN 'bonne couverture'
            WHEN coverage_score_pct >= 40 THEN 'couverture partielle'
            ELSE 'couverture faible'
        END                                             AS coverage_level,
        CURRENT_TIMESTAMP                               AS updated_at
    FROM vehicle_totals
    ORDER BY total_parts DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM mart_catalog_coverage").fetchone()[0]
    logger.info(f"✓ mart_catalog_coverage : {count} lignes")

    # ── mart_supplier_performance ──
    con.execute(f"""
    CREATE OR REPLACE TABLE mart_supplier_performance AS

    SELECT
        supplier,
        brand_type,
        COUNT(DISTINCT part_number)                         AS nb_unique_refs,
        COUNT(DISTINCT category)                            AS nb_categories,
        COUNT(DISTINCT brand)                               AS nb_brands,
        ROUND(AVG(price_eur), 2)                            AS avg_price,
        ROUND(AVG(quality_score), 3)                        AS avg_quality_score,
        COUNT(CASE WHEN is_available THEN 1 END)            AS nb_available_refs,
        ROUND(COUNT(CASE WHEN is_available THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                     AS availability_rate_pct,
        CASE
            WHEN AVG(quality_score) >= 0.8 AND
                 COUNT(CASE WHEN is_available THEN 1 END) * 100.0 / NULLIF(COUNT(*),0) >= 80
                 THEN 'Tier 1 — Stratégique'
            WHEN AVG(quality_score) >= 0.6
                 THEN 'Tier 2 — Fiable'
            WHEN AVG(quality_score) >= 0.4
                 THEN 'Tier 3 — À surveiller'
            ELSE      'Tier 4 — Risqué'
        END                                                 AS supplier_tier,
        ROUND(AVG(quality_score) * 100, 1)                 AS supplier_score,
        CURRENT_TIMESTAMP                                   AS updated_at
    FROM read_parquet('{silver_parts}')
    WHERE supplier IS NOT NULL
    GROUP BY 1, 2
    ORDER BY supplier_score DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM mart_supplier_performance").fetchone()[0]
    logger.info(f"✓ mart_supplier_performance : {count} lignes")

    # Export aussi en Parquet pour le dashboard
    for mart in ["mart_market_pricing", "mart_catalog_coverage", "mart_supplier_performance"]:
        out = GOLD_DIR / f"{mart}.parquet"
        con.execute(f"COPY {mart} TO '{out}' (FORMAT PARQUET)")
        logger.info(f"  → {out}")

    con.close()


# ─── Run complet ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ingest", action="store_true", help="Skip MongoDB ingestion")
    parser.add_argument("--parts",    type=int, default=2000)
    parser.add_argument("--listings", type=int, default=500)
    args = parser.parse_args()

    if not args.skip_ingest:
        step_ingest(args.parts, args.listings)

    step_mongo_to_bronze()
    step_bronze_to_silver()
    step_silver_to_gold()

    logger.info("")
    logger.info("=== Pipeline local terminé ✓ ===")
    logger.info(f"  Bronze : {BRONZE_DIR}")
    logger.info(f"  Silver : {SILVER_DIR}")
    logger.info(f"  Gold   : {GOLD_DIR}")
    logger.info("")
    logger.info("  Lancer le dashboard : streamlit run dashboard/app_local.py")
