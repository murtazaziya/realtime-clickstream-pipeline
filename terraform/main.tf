terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# ── Resource Group ───────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location
}

# ── Storage: ADLS Gen2 (Data Lake) ──────────────────────────
resource "azurerm_storage_account" "datalake" {
  name                     = "sa${var.project_name}${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Enables ADLS Gen2
}

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed" {
  name                  = "processed"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# ── Event Hubs (Kafka-compatible) ───────────────────────────
resource "azurerm_eventhub_namespace" "kafka" {
  name                = "evhbns-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "clickstream" {
  name                = "clickstream-events"
  namespace_name      = azurerm_eventhub_namespace.kafka.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1
}

resource "azurerm_eventhub_consumer_group" "spark" {
  name                = "spark-consumer"
  namespace_name      = azurerm_eventhub_namespace.kafka.name
  eventhub_name       = azurerm_eventhub.clickstream.name
  resource_group_name = azurerm_resource_group.main.name
}

# ── Azure PostgreSQL Flexible Server ────────────────────────
resource "azurerm_postgresql_flexible_server" "main" {
  name                   = "psql-${var.project_name}-${var.environment}"
  resource_group_name    = azurerm_resource_group.main.name
  location               = azurerm_resource_group.main.location
  version                = "14"
  administrator_login    = var.pg_admin_username
  administrator_password = var.pg_admin_password
  storage_mb             = 32768
  sku_name               = "B_Standard_B1ms"
  zone                   = "1"
  backup_retention_days  = 7
}

resource "azurerm_postgresql_flexible_server_database" "clickstream" {
  name      = "clickstream_db"
  server_id = azurerm_postgresql_flexible_server.main.id
  collation = "en_US.utf8"
  charset   = "utf8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_all" {
  name             = "allow-all-dev"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

# ── Azure Databricks ─────────────────────────────────────────
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku
}