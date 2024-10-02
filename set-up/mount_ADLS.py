# Databricks notebook source
# MAGIC %md
# MAGIC ## Gain access to storage account via key vault

# COMMAND ----------

scope = "formula1kv778-scope"
kv = "formula1kv778"
storage_account = "formula1dl778"
tenant_id = dbutils.secrets.get(scope=scope, key="formula1app-tenant-id")
client_secret = dbutils.secrets.get(scope=scope, key="formula1app-client-secret")
client_id = dbutils.secrets.get(scope=scope, key="formula1app-client-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

mounted_folders = [file.name.replace("/", "") for file in dbutils.fs.ls("/mnt")]
    
containers = ["raw", "presentation", "processed"]
for container in containers:
  if container in mounted_folders:
    continue
  dbutils.fs.mount(
    source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{container}",
    extra_configs = configs)