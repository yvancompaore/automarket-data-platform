terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Pour prod: backend S3
  # backend "s3" {
  #   bucket = "automarket-terraform-state"
  #   key    = "automarket/terraform.tfstate"
  #   region = "eu-west-1"
  # }
}

provider "aws" {
  region = var.aws_region

  # LocalStack override pour dev
  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      s3     = var.localstack_endpoint
      glue   = var.localstack_endpoint
      iam    = var.localstack_endpoint
      sts    = var.localstack_endpoint
    }
  }

  # Credentials fictifs pour LocalStack
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack

  default_tags {
    tags = local.common_tags
  }
}
