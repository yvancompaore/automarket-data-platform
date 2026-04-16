# ============================================================
# IAM - Rôles et politiques pour le pipeline data
# ============================================================

# ─── Rôle pour Airflow ───────────────────────────────────────
resource "aws_iam_role" "airflow_role" {
  name = "${var.project_name}-airflow-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "airflow.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "airflow_s3_policy" {
  name = "${var.project_name}-airflow-s3"
  role = aws_iam_role.airflow_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,   "${aws_s3_bucket.gold.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = ["*"]
      }
    ]
  })
}

# ─── Rôle pour Spark (EMR ou EKS) ───────────────────────────
resource "aws_iam_role" "spark_role" {
  name = "${var.project_name}-spark-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "spark_s3_policy" {
  name = "${var.project_name}-spark-s3"
  role = aws_iam_role.spark_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*",
        aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
        aws_s3_bucket.gold.arn,   "${aws_s3_bucket.gold.arn}/*",
      ]
    }]
  })
}

output "airflow_role_arn" { value = aws_iam_role.airflow_role.arn }
output "spark_role_arn"   { value = aws_iam_role.spark_role.arn }
