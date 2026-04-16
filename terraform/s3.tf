# ============================================================
# S3 Data Lake - Architecture Medallion (Bronze / Silver / Gold)
# ============================================================

# ─── Bronze : données brutes telles qu'ingérées ─────────────
resource "aws_s3_bucket" "bronze" {
  bucket = "${local.bucket_prefix}-bronze"
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "expire-old-raw-data"
    status = "Enabled"
    filter { prefix = "" }
    expiration {
      days = 90
    }
  }
}

# Prefixes logiques (partitionnement)
resource "aws_s3_object" "bronze_prefixes" {
  for_each = toset([
    "parts_catalog/",
    "vehicle_recalls/",
    "market_listings/",
  ])
  bucket  = aws_s3_bucket.bronze.id
  key     = each.value
  content = ""
}

# ─── Silver : données nettoyées et typées (Parquet) ──────────
resource "aws_s3_bucket" "silver" {
  bucket = "${local.bucket_prefix}-silver"
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "silver_prefixes" {
  for_each = toset([
    "parts_catalog/",
    "vehicle_recalls/",
    "market_listings/",
  ])
  bucket  = aws_s3_bucket.silver.id
  key     = each.value
  content = ""
}

# ─── Gold : data marts prêts à l'analyse ─────────────────────
resource "aws_s3_bucket" "gold" {
  bucket = "${local.bucket_prefix}-gold"
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "gold_prefixes" {
  for_each = toset([
    "mart_market_pricing/",
    "mart_catalog_coverage/",
    "mart_supplier_performance/",
    "mart_recall_impact/",
  ])
  bucket  = aws_s3_bucket.gold.id
  key     = each.value
  content = ""
}

# ─── Outputs ─────────────────────────────────────────────────
output "bronze_bucket" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket" { value = aws_s3_bucket.silver.bucket }
output "gold_bucket"   { value = aws_s3_bucket.gold.bucket }
