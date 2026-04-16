"""
PySpark Job : Bronze → Silver
Nettoyage, déduplication, typage strict, écriture en Parquet partitionné.
Lit depuis S3 Bronze (JSON brut depuis MongoDB export),
écrit vers S3 Silver (Parquet, schéma imposé).
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType,
    IntegerType, StringType, StructField, StructType, TimestampType,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# ─── Schémas imposés (Silver Layer) ──────────────────────────

SCHEMA_PARTS_CATALOG = StructType([
    StructField("part_id",            StringType(),  nullable=False),
    StructField("part_number",        StringType(),  nullable=False),
    StructField("brand",              StringType(),  nullable=True),
    StructField("brand_type",         StringType(),  nullable=True),
    StructField("category",           StringType(),  nullable=True),
    StructField("sub_category",       StringType(),  nullable=True),
    StructField("description",        StringType(),  nullable=True),
    StructField("price_eur",          DoubleType(),  nullable=True),
    StructField("stock_quantity",     IntegerType(), nullable=True),
    StructField("availability",       StringType(),  nullable=True),
    StructField("supplier",           StringType(),  nullable=True),
    StructField("source",             StringType(),  nullable=True),
    StructField("quality_score",      DoubleType(),  nullable=True),
    StructField("_fingerprint",       StringType(),  nullable=True),
    StructField("ingested_at",        StringType(),  nullable=True),
])

SCHEMA_RECALLS = StructType([
    StructField("recall_id",          StringType(),  nullable=False),
    StructField("manufacturer",       StringType(),  nullable=True),
    StructField("make",               StringType(),  nullable=True),
    StructField("model",              StringType(),  nullable=True),
    StructField("model_year",         IntegerType(), nullable=True),
    StructField("component",          StringType(),  nullable=True),
    StructField("summary",            StringType(),  nullable=True),
    StructField("remedy",             StringType(),  nullable=True),
    StructField("potentially_affected", IntegerType(), nullable=True),
    StructField("source",             StringType(),  nullable=True),
    StructField("ingested_at",        StringType(),  nullable=True),
])

SCHEMA_LISTINGS = StructType([
    StructField("listing_id",         StringType(),  nullable=False),
    StructField("part_number",        StringType(),  nullable=True),
    StructField("brand",              StringType(),  nullable=True),
    StructField("category",           StringType(),  nullable=True),
    StructField("vehicle_make",       StringType(),  nullable=True),
    StructField("vehicle_model",      StringType(),  nullable=True),
    StructField("vehicle_year",       IntegerType(), nullable=True),
    StructField("asking_price_eur",   DoubleType(),  nullable=True),
    StructField("negotiated_price_eur", DoubleType(), nullable=True),
    StructField("seller_type",        StringType(),  nullable=True),
    StructField("seller_region",      StringType(),  nullable=True),
    StructField("condition",          StringType(),  nullable=True),
    StructField("days_to_sell",       IntegerType(), nullable=True),
    StructField("source",             StringType(),  nullable=True),
    StructField("ingested_at",        StringType(),  nullable=True),
])


def build_spark_session(app_name: str, s3_endpoint: str = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if s3_endpoint:
        # Configuration pour LocalStack / MinIO
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.565")
        )

    return builder.getOrCreate()


# ─── Transformations Parts Catalog ───────────────────────────

def clean_parts_catalog(df: DataFrame) -> DataFrame:
    """
    Nettoyage du catalogue pièces :
    - Suppression des lignes sans part_id ou part_number
    - Normalisation des chaînes (strip, uppercase pour codes)
    - Nettoyage des prix aberrants (< 0 ou > 10 000€)
    - Déduplication par fingerprint (garder le plus récent)
    - Ajout de colonnes dérivées
    """
    df = (
        df
        # Filtre qualité minimale
        .filter(F.col("part_id").isNotNull() & F.col("part_number").isNotNull())
        .filter(F.col("part_number") != "")
        # Normalisation
        .withColumn("part_number", F.trim(F.upper(F.col("part_number"))))
        .withColumn("brand",       F.trim(F.initcap(F.col("brand"))))
        .withColumn("category",    F.trim(F.col("category")))
        .withColumn("source",      F.trim(F.lower(F.col("source"))))
        .withColumn("availability",F.trim(F.lower(F.col("availability"))))
        # Prix : remplacer les valeurs hors bornes par null
        .withColumn(
            "price_eur",
            F.when((F.col("price_eur") > 0) & (F.col("price_eur") < 10000), F.col("price_eur"))
             .otherwise(F.lit(None).cast(DoubleType()))
        )
        # Stock : pas de négatif
        .withColumn(
            "stock_quantity",
            F.when(F.col("stock_quantity") >= 0, F.col("stock_quantity")).otherwise(F.lit(0))
        )
        # Quality score : borner entre 0 et 1
        .withColumn(
            "quality_score",
            F.greatest(F.least(F.col("quality_score"), F.lit(1.0)), F.lit(0.0))
        )
        # Timestamp normalisé
        .withColumn("ingested_at_ts", F.to_timestamp("ingested_at"))
        # Colonnes de partitionnement
        .withColumn("ingested_year",  F.year("ingested_at_ts"))
        .withColumn("ingested_month", F.month("ingested_at_ts"))
    )

    # Déduplication : par fingerprint, garder le record le plus récent
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("_fingerprint")
        .orderBy(F.desc("ingested_at_ts"))
    )
    df = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "ingested_at_ts")
    )

    logger.info(f"parts_catalog après nettoyage : {df.count()} lignes")
    return df


def clean_vehicle_recalls(df: DataFrame) -> DataFrame:
    """Nettoyage des rappels véhicules."""
    df = (
        df
        .filter(F.col("recall_id").isNotNull())
        .dropDuplicates(["recall_id"])
        .withColumn("make",         F.trim(F.upper(F.col("make"))))
        .withColumn("model",        F.trim(F.initcap(F.col("model"))))
        .withColumn("manufacturer", F.trim(F.col("manufacturer")))
        .withColumn("component",    F.trim(F.col("component")))
        .withColumn(
            "model_year",
            F.when(
                (F.col("model_year") >= 1980) & (F.col("model_year") <= 2025),
                F.col("model_year")
            ).otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn("ingested_at_ts", F.to_timestamp("ingested_at"))
        .withColumn("ingested_year",  F.year("ingested_at_ts"))
        .withColumn("ingested_month", F.month("ingested_at_ts"))
        .drop("ingested_at_ts", "_raw")
    )
    logger.info(f"vehicle_recalls après nettoyage : {df.count()} lignes")
    return df


def clean_market_listings(df: DataFrame) -> DataFrame:
    """Nettoyage des annonces marketplace."""
    df = (
        df
        .filter(F.col("listing_id").isNotNull())
        .dropDuplicates(["listing_id"])
        .filter(F.col("asking_price_eur") > 0)
        .withColumn("vehicle_make",  F.trim(F.initcap(F.col("vehicle_make"))))
        .withColumn("vehicle_model", F.trim(F.col("vehicle_model")))
        .withColumn("brand",         F.trim(F.initcap(F.col("brand"))))
        .withColumn("category",      F.trim(F.col("category")))
        .withColumn("condition",     F.trim(F.lower(F.col("condition"))))
        .withColumn("seller_type",   F.trim(F.lower(F.col("seller_type"))))
        # Discount rate
        .withColumn(
            "discount_rate",
            F.round(
                1.0 - (F.col("negotiated_price_eur") / F.col("asking_price_eur")),
                4
            )
        )
        .withColumn("ingested_at_ts", F.to_timestamp("ingested_at"))
        .withColumn("ingested_year",  F.year("ingested_at_ts"))
        .withColumn("ingested_month", F.month("ingested_at_ts"))
        .drop("ingested_at_ts")
    )
    logger.info(f"market_listings après nettoyage : {df.count()} lignes")
    return df


# ─── Main ────────────────────────────────────────────────────

def run(
    bronze_bucket: str,
    silver_bucket: str,
    s3_endpoint: str,
    datasets: list[str],
    run_date: str,
) -> None:
    logger.info(f"=== Spark Bronze→Silver | datasets={datasets} | date={run_date} ===")

    spark = build_spark_session("automarket-bronze-to-silver", s3_endpoint)
    spark.sparkContext.setLogLevel("WARN")

    protocol = "s3a"

    for dataset in datasets:
        logger.info(f"--- Traitement : {dataset} ---")

        bronze_path = f"{protocol}://{bronze_bucket}/{dataset}/"
        silver_path = f"{protocol}://{silver_bucket}/{dataset}/"

        try:
            raw_df = spark.read.json(bronze_path)
            logger.info(f"Lu {raw_df.count()} lignes depuis {bronze_path}")

            if dataset == "parts_catalog":
                clean_df = clean_parts_catalog(raw_df)
                partition_cols = ["ingested_year", "ingested_month"]
            elif dataset == "vehicle_recalls":
                clean_df = clean_vehicle_recalls(raw_df)
                partition_cols = ["ingested_year", "ingested_month"]
            elif dataset == "market_listings":
                clean_df = clean_market_listings(raw_df)
                partition_cols = ["ingested_year", "ingested_month"]
            else:
                logger.warning(f"Dataset inconnu : {dataset}, skip.")
                continue

            (
                clean_df
                .write
                .mode("overwrite")
                .partitionBy(*partition_cols)
                .parquet(silver_path)
            )
            logger.info(f"Écrit vers {silver_path} ✓")

        except Exception as e:
            logger.error(f"Erreur sur {dataset}: {e}")
            raise

    spark.stop()
    logger.info("=== Job Spark terminé ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze to Silver PySpark job")
    parser.add_argument("--bronze-bucket", default=os.getenv("S3_BUCKET_BRONZE", "automarket-bronze"))
    parser.add_argument("--silver-bucket", default=os.getenv("S3_BUCKET_SILVER", "automarket-silver"))
    parser.add_argument("--s3-endpoint",   default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"))
    parser.add_argument("--datasets",      nargs="+", default=["parts_catalog", "vehicle_recalls", "market_listings"])
    parser.add_argument("--run-date",      default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    run(
        bronze_bucket=args.bronze_bucket,
        silver_bucket=args.silver_bucket,
        s3_endpoint=args.s3_endpoint,
        datasets=args.datasets,
        run_date=args.run_date,
    )
