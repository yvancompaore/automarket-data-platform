# ============================================================
# AWS Glue - Data Catalog (métastore pour dbt + Athena)
# ============================================================

resource "aws_glue_catalog_database" "automarket_dw" {
  name        = var.glue_database_name
  description = "Data warehouse AutoMarket - après-vente automobile"
}

# Table Silver : parts_catalog
resource "aws_glue_catalog_table" "silver_parts_catalog" {
  name          = "silver_parts_catalog"
  database_name = aws_glue_catalog_database.automarket_dw.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"  = "parquet"
    "typeOfData"      = "file"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_prefix}-silver/parts_catalog/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "part_id"
      type = "string"
    }
    columns {
      name = "part_number"
      type = "string"
    }
    columns {
      name = "brand"
      type = "string"
    }
    columns {
      name = "category"
      type = "string"
    }
    columns {
      name = "price_eur"
      type = "double"
    }
    columns {
      name = "vehicle_make"
      type = "string"
    }
    columns {
      name = "vehicle_model"
      type = "string"
    }
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "ingested_date"
      type = "date"
    }
  }

  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }
}

output "glue_database" { value = aws_glue_catalog_database.automarket_dw.name }
