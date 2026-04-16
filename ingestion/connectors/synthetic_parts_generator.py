"""
Générateur de données synthétiques pour le catalogue pièces auto.
Simule la diversité et les incohérences d'un vrai catalogue multi-sources
(comme Parts.IO chez Kadensis) : doublons, prix variables, références multiples.
"""

import hashlib
import random
import uuid
from datetime import datetime, timedelta
from typing import Iterator

# ─── Référentiels métier ──────────────────────────────────────

BRANDS_OEM = ["Bosch", "Valeo", "Denso", "Continental", "Delphi", "ZF", "Magna"]
BRANDS_COMPAT = ["Febi", "SKF", "FAG", "NGK", "Mann", "Hella", "Beru", "Sachs"]
BRANDS_TIRES = ["Michelin", "Bridgestone", "Continental", "Pirelli", "Goodyear", "Hankook"]

CATEGORIES = {
    "Freinage": ["Plaquettes", "Disques", "Étriers", "Maître-cylindre", "Flexible"],
    "Moteur": ["Filtre à huile", "Filtre à air", "Courroie distribution", "Joint cache culbuteur", "Bougie"],
    "Suspension": ["Amortisseur avant", "Amortisseur arrière", "Triangle", "Rotule", "Silent bloc"],
    "Électricité": ["Alternateur", "Démarreur", "Batterie", "Faisceau", "Capteur"],
    "Refroidissement": ["Radiateur", "Thermostat", "Pompe à eau", "Durite", "Vase expansion"],
    "Pneus": ["Été 205/55R16", "Hiver 205/55R16", "4x4 235/65R17", "Été 195/65R15"],
    "Transmission": ["Embrayage", "Boîte de vitesses", "Arbre de transmission", "Joints homocinétiques"],
}

VEHICLE_MAKES = {
    "Renault": ["Clio", "Megane", "Kadjar", "Captur", "Trafic", "Master"],
    "Peugeot": ["208", "308", "3008", "5008", "Partner", "Boxer"],
    "Volkswagen": ["Golf", "Polo", "Passat", "Tiguan", "Transporter", "Crafter"],
    "Toyota": ["Yaris", "Corolla", "RAV4", "C-HR", "Proace", "HiLux"],
    "BMW": ["Serie 1", "Serie 3", "X1", "X3", "X5"],
    "Mercedes-Benz": ["Classe A", "Classe C", "Vito", "Sprinter", "GLC"],
    "Ford": ["Fiesta", "Focus", "Kuga", "Transit", "Ranger"],
    "Citroën": ["C3", "C4", "Berlingo", "Jumpy", "Jumper"],
    "Audi": ["A3", "A4", "Q3", "Q5", "A6"],
    "Hyundai": ["i20", "i30", "Tucson", "Kona", "H1"],
}

SUPPLIERS = [
    "AutoPro Distribution", "MécaPieces", "PartnerAuto",
    "EuroParts SAS", "DistribAuto", "FastParts FR",
    "PiècesExpres", "GarageConnect", "AutoStock Pro",
]

SOURCES = ["supplier_api", "catalog_csv", "marketplace_scraper", "edi_feed"]

MODEL_YEARS = list(range(2010, 2025))


def _generate_part_number(brand: str, category: str, idx: int) -> str:
    """Génère un numéro de référence réaliste."""
    prefix = brand[:3].upper()
    cat_code = category[:2].upper()
    return f"{prefix}-{cat_code}-{idx:06d}"


def _generate_price(category: str, brand_type: str) -> float:
    """Génère un prix cohérent avec la catégorie et le type de marque."""
    base_prices = {
        "Freinage": (25, 180),
        "Moteur": (8, 220),
        "Suspension": (30, 350),
        "Électricité": (40, 600),
        "Refroidissement": (15, 250),
        "Pneus": (60, 280),
        "Transmission": (80, 900),
    }
    low, high = base_prices.get(category, (10, 200))
    price = random.uniform(low, high)
    if brand_type == "OEM":
        price *= random.uniform(1.2, 1.8)  # OEM plus cher
    return round(price, 2)


def generate_parts_catalog(n: int = 5000) -> Iterator[dict]:
    """
    Génère n références du catalogue avec des incohérences intentionnelles
    (doublons cross-sources, variations de prix, descriptions différentes).
    """
    all_brands = BRANDS_OEM + BRANDS_COMPAT + BRANDS_TIRES
    part_pool = []

    # Créer un pool de pièces de base (qui seront dupliquées cross-sources)
    base_count = n // 2
    for i in range(base_count):
        category = random.choice(list(CATEGORIES.keys()))
        sub_category = random.choice(CATEGORIES[category])
        brand = random.choice(all_brands)
        brand_type = "OEM" if brand in BRANDS_OEM else "COMPAT"
        make = random.choice(list(VEHICLE_MAKES.keys()))
        models = random.sample(VEHICLE_MAKES[make], k=random.randint(1, 3))
        years = random.sample(MODEL_YEARS, k=random.randint(2, 6))
        part_number = _generate_part_number(brand, category, i)

        part_pool.append({
            "_base_part_number": part_number,
            "category": category,
            "sub_category": sub_category,
            "brand": brand,
            "brand_type": brand_type,
            "make": make,
            "models": models,
            "years": sorted(years),
        })

    # Générer les documents avec multi-sources (doublons intentionnels)
    generated = 0
    for base in part_pool:
        num_sources = random.choices([1, 2, 3], weights=[40, 40, 20])[0]
        sources = random.sample(SOURCES, k=min(num_sources, len(SOURCES)))

        for source in sources:
            if generated >= n:
                return

            supplier = random.choice(SUPPLIERS)
            base_price = _generate_price(base["category"], base["brand_type"])

            # Variation de prix selon la source (réaliste)
            price_variation = random.uniform(0.85, 1.15)
            price = round(base_price * price_variation, 2)

            doc = {
                "part_id": str(uuid.uuid4()),
                "part_number": base["_base_part_number"],
                # Légère variation du numéro selon source (cas réels)
                "part_number_source": base["_base_part_number"]
                if random.random() > 0.1
                else base["_base_part_number"].replace("-", " "),
                "brand": base["brand"],
                "brand_type": base["brand_type"],
                "category": base["category"],
                "sub_category": base["sub_category"],
                "description": f"{base['brand']} {base['sub_category']} pour {base['make']}",
                "price_eur": price,
                "currency": "EUR",
                "stock_quantity": random.randint(0, 500),
                "availability": random.choices(
                    ["in_stock", "on_order", "discontinued"],
                    weights=[65, 25, 10]
                )[0],
                "vehicle_compatibility": [
                    {
                        "make": base["make"],
                        "model": model,
                        "year_from": min(base["years"]),
                        "year_to": max(base["years"]),
                    }
                    for model in base["models"]
                ],
                "supplier": supplier,
                "source": source,
                "quality_score": round(random.uniform(0.5, 1.0), 2),
                "ingested_at": (
                    datetime.utcnow() - timedelta(days=random.randint(0, 30))
                ).isoformat(),
                "last_updated": datetime.utcnow().isoformat(),
                # Fingerprint pour déduplication
                "_fingerprint": hashlib.md5(
                    f"{base['_base_part_number']}:{base['brand']}".encode()
                ).hexdigest(),
            }
            yield doc
            generated += 1


def generate_market_listings(n: int = 2000) -> Iterator[dict]:
    """
    Génère des annonces de marché (simulation marketplace Partakus).
    Représente les prix réels pratiqués par les garagistes / distributeurs.
    """
    categories = list(CATEGORIES.keys())

    for i in range(n):
        category = random.choice(categories)
        make = random.choice(list(VEHICLE_MAKES.keys()))
        model = random.choice(VEHICLE_MAKES[make])
        year = random.choice(MODEL_YEARS)
        brand = random.choice(BRANDS_OEM + BRANDS_COMPAT)
        price = _generate_price(category, "OEM" if brand in BRANDS_OEM else "COMPAT")

        listing_date = datetime.utcnow() - timedelta(days=random.randint(0, 90))

        yield {
            "listing_id": str(uuid.uuid4()),
            "part_number": _generate_part_number(brand, category, i),
            "brand": brand,
            "category": category,
            "vehicle_make": make,
            "vehicle_model": model,
            "vehicle_year": year,
            "asking_price_eur": price,
            "negotiated_price_eur": round(price * random.uniform(0.88, 1.0), 2),
            "seller_type": random.choice(["garage", "distributor", "individual"]),
            "seller_region": random.choice([
                "Île-de-France", "Auvergne-Rhône-Alpes", "PACA",
                "Occitanie", "Nouvelle-Aquitaine", "Hauts-de-France"
            ]),
            "condition": random.choices(
                ["new", "reconditioned", "used"],
                weights=[70, 20, 10]
            )[0],
            "days_to_sell": random.randint(1, 60) if random.random() > 0.3 else None,
            "listing_date": listing_date.isoformat(),
            "source": "partakus_marketplace",
            "ingested_at": datetime.utcnow().isoformat(),
        }
