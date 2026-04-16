variable "environment" {
  description = "Environnement de déploiement"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Nom du projet"
  type        = string
  default     = "automarket"
}

variable "localstack_endpoint" {
  description = "Endpoint LocalStack pour dev local"
  type        = string
  default     = "http://localhost:4566"
}

variable "use_localstack" {
  description = "Utiliser LocalStack au lieu d'AWS réel"
  type        = bool
  default     = true
}

variable "glue_database_name" {
  description = "Nom de la Glue Database"
  type        = string
  default     = "automarket_dw"
}

locals {
  bucket_prefix = "${var.project_name}-${var.environment}"
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Owner       = "data-engineering"
  }
}
