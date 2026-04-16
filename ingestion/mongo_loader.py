"""
MongoDB Loader — Raw Landing Zone
Charge les données brutes depuis les connecteurs vers MongoDB.
MongoDB joue le rôle de couche d'ingestion flexible (schéma libre)
avant le versement vers S3 Bronze.
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Iterator

import pymongo
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from ingestion.connectors.nhtsa_connector import stream_recalls
from ingestion.connectors.synthetic_parts_generator import (
    generate_market_listings,
    generate_parts_catalog,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "automarket_raw")
BATCH_SIZE = 500


def get_mongo_client() -> MongoClient:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")  # Validation connexion
    logger.info(f"Connecté à MongoDB : {MONGO_URI}")
    return client


def upsert_batch(collection, documents: list[dict], id_field: str) -> dict:
    """
    Upsert en batch avec gestion des doublons.
    Retourne les stats d'écriture.
    """
    if not documents:
        return {"inserted": 0, "updated": 0, "errors": 0}

    operations = [
        UpdateOne(
            {id_field: doc[id_field]},
            {"$set": doc, "$setOnInsert": {"first_seen_at": datetime.utcnow()}},
            upsert=True,
        )
        for doc in documents
        if doc.get(id_field)
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        return {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "errors": 0,
        }
    except BulkWriteError as e:
        errors = len(e.details.get("writeErrors", []))
        logger.warning(f"BulkWrite partiel : {errors} erreurs sur {len(operations)}")
        return {
            "inserted": e.details.get("nUpserted", 0),
            "updated": e.details.get("nModified", 0),
            "errors": errors,
        }


def load_in_batches(
    collection,
    generator: Iterator[dict],
    id_field: str,
    collection_name: str,
) -> dict:
    """Charge un générateur en batches vers une collection MongoDB."""
    batch = []
    total_stats = {"inserted": 0, "updated": 0, "errors": 0, "total": 0}

    for doc in generator:
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            stats = upsert_batch(collection, batch, id_field)
            for k in ["inserted", "updated", "errors"]:
                total_stats[k] += stats[k]
            total_stats["total"] += len(batch)
            logger.info(
                f"{collection_name} | batch {total_stats['total']} docs "
                f"| +{stats['inserted']} inserts | ~{stats['updated']} updates"
            )
            batch = []

    # Dernier batch
    if batch:
        stats = upsert_batch(collection, batch, id_field)
        for k in ["inserted", "updated", "errors"]:
            total_stats[k] += stats[k]
        total_stats["total"] += len(batch)

    return total_stats


def load_parts_catalog(db, n: int = 5000) -> dict:
    """Charge le catalogue pièces synthétique dans MongoDB."""
    logger.info(f"Chargement catalogue pièces ({n} docs)...")
    collection = db["parts_catalog"]
    return load_in_batches(
        collection,
        generate_parts_catalog(n),
        id_field="part_id",
        collection_name="parts_catalog",
    )


def load_market_listings(db, n: int = 2000) -> dict:
    """Charge les annonces marketplace dans MongoDB."""
    logger.info(f"Chargement annonces marketplace ({n} docs)...")
    collection = db["market_listings"]
    return load_in_batches(
        collection,
        generate_market_listings(n),
        id_field="listing_id",
        collection_name="market_listings",
    )


def load_vehicle_recalls(
    db,
    makes: list[str] = None,
    model_years: list[int] = None,
) -> dict:
    """Charge les rappels NHTSA depuis l'API dans MongoDB."""
    logger.info("Chargement rappels véhicules NHTSA...")
    collection = db["vehicle_recalls"]
    return load_in_batches(
        collection,
        stream_recalls(makes=makes, model_years=model_years),
        id_field="recall_id",
        collection_name="vehicle_recalls",
    )


def log_pipeline_run(db, run_info: dict) -> None:
    """Trace chaque exécution du pipeline pour auditabilité."""
    db["pipeline_runs"].insert_one({
        **run_info,
        "recorded_at": datetime.utcnow(),
    })


def run(
    load_parts: bool = True,
    load_listings: bool = True,
    load_recalls: bool = False,  # False par défaut (appel API externe)
    parts_count: int = 5000,
    listings_count: int = 2000,
) -> None:
    run_id = f"ingestion_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"=== Pipeline ingestion démarré | run_id={run_id} ===")

    client = get_mongo_client()
    db = client[MONGO_DB]

    results = {}

    if load_parts:
        results["parts_catalog"] = load_parts_catalog(db, n=parts_count)

    if load_listings:
        results["market_listings"] = load_market_listings(db, n=listings_count)

    if load_recalls:
        results["vehicle_recalls"] = load_vehicle_recalls(
            db,
            makes=["TOYOTA", "VOLKSWAGEN", "RENAULT"],
            model_years=[2020, 2021, 2022, 2023],
        )

    total_docs = sum(r.get("total", 0) for r in results.values())
    logger.info(f"=== Ingestion terminée | {total_docs} docs traités ===")
    logger.info(json.dumps(results, indent=2))

    log_pipeline_run(db, {
        "run_id": run_id,
        "dag_id": "ingestion",
        "status": "success",
        "results": results,
        "total_docs": total_docs,
    })

    client.close()


if __name__ == "__main__":
    run(load_parts=True, load_listings=True, load_recalls=False)
