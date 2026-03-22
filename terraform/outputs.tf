output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "datalake_storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "eventhub_namespace_name" {
  value = azurerm_eventhub_namespace.kafka.name
}

output "eventhub_connection_string" {
  value     = azurerm_eventhub_namespace.kafka.default_primary_connection_string
  sensitive = true
}

output "postgresql_host" {
  value = azurerm_postgresql_flexible_server.main.fqdn
}

output "postgresql_database" {
  value = azurerm_postgresql_flexible_server_database.clickstream.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}