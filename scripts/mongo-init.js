// Initialisation MongoDB - Collections et index pour automarket_raw

db = db.getSiblingDB("automarket_raw");

// Collections
db.createCollection("parts_catalog");
db.createCollection("vehicle_recalls");
db.createCollection("market_listings");
db.createCollection("pipeline_runs");

// Index pour parts_catalog
// part_number N'est PAS unique : même référence peut venir de plusieurs sources
db.parts_catalog.createIndex({ "part_number": 1 });
// Compound unique : un part_id (UUID) est unique
db.parts_catalog.createIndex({ "part_id": 1 }, { unique: true });
db.parts_catalog.createIndex({ "brand": 1, "category": 1 });
db.parts_catalog.createIndex({ "vehicle_compatibility.make": 1 });
db.parts_catalog.createIndex({ "ingested_at": 1 });
db.parts_catalog.createIndex({ "source": 1 });

// Index pour vehicle_recalls
db.vehicle_recalls.createIndex({ "recall_id": 1 }, { unique: true });
db.vehicle_recalls.createIndex({ "manufacturer": 1, "model_year": 1 });
db.vehicle_recalls.createIndex({ "component": 1 });

// Index pour market_listings
db.market_listings.createIndex({ "listing_id": 1 }, { unique: true });
db.market_listings.createIndex({ "part_number": 1 });
db.market_listings.createIndex({ "price": 1 });
db.market_listings.createIndex({ "scraped_at": 1 });

// Index pour pipeline_runs (tracking)
db.pipeline_runs.createIndex({ "run_id": 1 }, { unique: true });
db.pipeline_runs.createIndex({ "dag_id": 1, "status": 1 });

print("MongoDB init terminé - Collections et index créés");
