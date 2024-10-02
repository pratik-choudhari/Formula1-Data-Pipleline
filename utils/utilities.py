# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

def add_date(df: DataFrame, source_date: str) -> DataFrame:
    return df.withColumn("source_date", lit(source_date)) \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

def update_delta_table(df: DataFrame, db_name: str, table_name: str, merge_condition: str, partition_col: str) -> None:
    table_name = f"{db_name}.{table_name}"
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning.enabled", "true")
    if spark.catalog.tableExists(table_name):
        delta_obj = DeltaTable.forName(spark, table_name)
        delta_obj.alias("t").merge(df.alias("s"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(table_name)

# COMMAND ----------

spark.catalog.listTables()