variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
  default     = "clickstream"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "pg_admin_username" {
  description = "PostgreSQL admin username"
  type        = string
  default     = "pgadmin"
}

variable "pg_admin_password" {
  description = "PostgreSQL admin password"
  type        = string
  sensitive   = true
}

variable "databricks_sku" {
  description = "Databricks SKU"
  type        = string
  default     = "standard"
}