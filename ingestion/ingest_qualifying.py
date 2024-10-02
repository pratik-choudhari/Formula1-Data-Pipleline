# Databricks notebook source
# MAGIC %run "../configuration"

# COMMAND ----------

# MAGIC %run "../utils/utilities"

# COMMAND ----------

dbutils.widgets.text("source_date", "")
source_date = dbutils.widgets.get("source_date")

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t

# COMMAND ----------

qualifying_schema = ("qualifyId INT, raceId INT, driverId INT, constructorId INT, "
                     "number INT, position INT, q1 STRING, q2 STRING, q3 STRING")
qualifying_df = spark.read.json(f"{raw_folder}/{source_date}/qualifying", 
                             multiLine=True, 
                             schema=qualifying_schema)

# COMMAND ----------

qualifying_new_df = qualifying_df.withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("qualifyId", "qualify_id")

qualifying_new_df = add_date(qualifying_new_df, source_date)

# COMMAND ----------

update_delta_table(qualifying_new_df, "f1_processed", "qualifying", "t.qualify_id = s.qualify_id", "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")