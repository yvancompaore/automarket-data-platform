"""
NHTSA API Connector
API publique US : rappels véhicules, plaintes, notes de sécurité
Docs : https://api.nhtsa.gov/
"""

import logging
import time
from datetime import datetime
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

NHTSA_BASE_URL = "https://api.nhtsa.gov"
REQUEST_DELAY = 0.3  # secondes entre requêtes (respect rate limit)

# Constructeurs les plus présents en Europe / marché mondial
TARGET_MAKES = [
    "TOYOTA", "VOLKSWAGEN", "RENAULT", "PEUGEOT", "CITROEN",
    "BMW", "MERCEDES-BENZ", "AUDI", "FORD", "HYUNDAI",
    "KIA", "NISSAN", "HONDA", "FIAT", "OPEL",
]

TARGET_MODEL_YEARS = list(range(2015, 2025))


def get_makes() -> list[dict]:
    """Récupère la liste de tous les constructeurs."""
    url = f"{NHTSA_BASE_URL}/products/vehicle/makes"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("results", [])


def get_models_for_make(make: str) -> list[dict]:
    """Récupère les modèles d'un constructeur."""
    url = f"{NHTSA_BASE_URL}/products/vehicle/models"
    resp = requests.get(url, params={"make": make}, timeout=15)
    resp.raise_for_status()
    return resp.json().get("results", [])


def get_recalls_for_make_model_year(
    make: str, model: str, model_year: int
) -> list[dict]:
    """Récupère les rappels pour une combinaison make/model/year."""
    url = f"{NHTSA_BASE_URL}/recalls/recallsByVehicle"
    try:
        resp = requests.get(
            url,
            params={"make": make, "model": model, "modelYear": model_year},
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return [_enrich_recall(r, make, model, model_year) for r in results]
    except requests.RequestException as e:
        logger.warning(f"Erreur recall {make}/{model}/{model_year}: {e}")
        return []


def get_complaints_for_make_model_year(
    make: str, model: str, model_year: int
) -> list[dict]:
    """Récupère les plaintes consommateurs pour une combinaison make/model/year."""
    url = f"{NHTSA_BASE_URL}/complaints/complaintsByVehicle"
    try:
        resp = requests.get(
            url,
            params={"make": make, "model": model, "modelYear": model_year},
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return [_enrich_complaint(r, make, model, model_year) for r in results]
    except requests.RequestException as e:
        logger.warning(f"Erreur complaint {make}/{model}/{model_year}: {e}")
        return []


def stream_recalls(
    makes: list[str] = None,
    model_years: list[int] = None,
    max_models_per_make: int = 5,
) -> Iterator[dict]:
    """
    Générateur : stream les rappels pour les constructeurs et années cibles.
    Utilisation : for recall in stream_recalls(): ...
    """
    makes = makes or TARGET_MAKES
    model_years = model_years or TARGET_MODEL_YEARS

    for make in makes:
        models = get_models_for_make(make)[:max_models_per_make]
        for model_info in models:
            model = model_info.get("model", "")
            for year in model_years:
                recalls = get_recalls_for_make_model_year(make, model, year)
                for recall in recalls:
                    yield recall
                time.sleep(REQUEST_DELAY)


def _enrich_recall(raw: dict, make: str, model: str, model_year: int) -> dict:
    """Normalise et enrichit un rappel brut."""
    return {
        "recall_id": raw.get("NHTSACampaignNumber", ""),
        "manufacturer": raw.get("Manufacturer", make),
        "make": make,
        "model": model,
        "model_year": model_year,
        "component": raw.get("Component", ""),
        "summary": raw.get("Summary", ""),
        "consequence": raw.get("Conséquence", raw.get("Consequence", "")),
        "remedy": raw.get("Remedy", ""),
        "report_received_date": raw.get("ReportReceivedDate", ""),
        "record_creation_date": raw.get("RecordCreationDate", ""),
        "potentially_affected": raw.get("PotentialNumberOfUnitsAffected", 0),
        "source": "nhtsa_api",
        "ingested_at": datetime.utcnow().isoformat(),
        "_raw": raw,
    }


def _enrich_complaint(raw: dict, make: str, model: str, model_year: int) -> dict:
    """Normalise et enrichit une plainte brute."""
    return {
        "complaint_id": str(raw.get("odiNumber", "")),
        "make": make,
        "model": model,
        "model_year": model_year,
        "component": raw.get("components", ""),
        "description": raw.get("complaint", ""),
        "crash": raw.get("crash", False),
        "fire": raw.get("fire", False),
        "injuries": raw.get("numberOfInjuries", 0),
        "deaths": raw.get("numberOfDeaths", 0),
        "date_of_incident": raw.get("dateOfIncident", ""),
        "date_added": raw.get("dateAdded", ""),
        "source": "nhtsa_api",
        "ingested_at": datetime.utcnow().isoformat(),
        "_raw": raw,
    }
