"""
PySpark Job : MongoDB → S3 Bronze
Exporte les collections MongoDB vers S3 Bronze en JSON (partitionné par date).
Utilise le connecteur MongoDB Spark.
"""

import argparse
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

COLLECTIONS = ["parts_catalog", "vehicle_recalls", "market_listings"]


def build_spark_session(app_name: str, s3_endpoint: str, mongo_uri: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.565")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def export_collection(
    spark: SparkSession,
    database: str,
    collection: str,
    bronze_bucket: str,
    run_date: str,
) -> int:
    """Exporte une collection MongoDB vers S3 Bronze."""
    logger.info(f"Export {database}.{collection} → S3 Bronze...")

    df = (
        spark.read
        .format("mongodb")
        .option("database", database)
        .option("collection", collection)
        .load()
    )

    # Supprimer l'ObjectId MongoDB (non sérialisable proprement)
    if "_id" in df.columns:
        df = df.drop("_id")

    # Ajouter partition date
    year  = run_date[:4]
    month = run_date[5:7]
    day   = run_date[8:10]

    df = (
        df
        .withColumn("export_year",  F.lit(int(year)))
        .withColumn("export_month", F.lit(int(month)))
        .withColumn("export_day",   F.lit(int(day)))
    )

    output_path = f"s3a://{bronze_bucket}/{collection}/year={year}/month={month}/day={day}/"

    (
        df.write
        .mode("overwrite")
        .json(output_path)
    )

    count = df.count()
    logger.info(f"Export {collection} : {count} docs → {output_path}")
    return count


def run(
    mongo_uri: str,
    mongo_db: str,
    bronze_bucket: str,
    s3_endpoint: str,
    collections: list[str],
    run_date: str,
) -> None:
    logger.info(f"=== Export MongoDB → S3 Bronze | {run_date} ===")

    spark = build_spark_session("automarket-mongo-to-s3", s3_endpoint, mongo_uri)
    spark.sparkContext.setLogLevel("WARN")

    total = 0
    for collection in collections:
        count = export_collection(spark, mongo_db, collection, bronze_bucket, run_date)
        total += count

    logger.info(f"=== Export terminé | {total} docs exportés ===")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB to S3 Bronze export")
    parser.add_argument("--mongo-uri",     default=os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017"))
    parser.add_argument("--mongo-db",      default=os.getenv("MONGO_DB", "automarket_raw"))
    parser.add_argument("--bronze-bucket", default=os.getenv("S3_BUCKET_BRONZE", "automarket-bronze"))
    parser.add_argument("--s3-endpoint",   default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"))
    parser.add_argument("--collections",   nargs="+", default=COLLECTIONS)
    parser.add_argument("--run-date",      default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    run(
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        bronze_bucket=args.bronze_bucket,
        s3_endpoint=args.s3_endpoint,
        collections=args.collections,
        run_date=args.run_date,
    )
