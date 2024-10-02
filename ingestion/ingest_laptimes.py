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

laps_schema = "raceId INT, driverId INT, lap INT, position INT, time STRING, milliseconds INT"
laps_df = spark.read.csv(f"{raw_folder}/{source_date}/lap_times", 
                             schema=laps_schema)

# COMMAND ----------

laps_new_df = laps_df.withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id")

laps_new_df = add_date(laps_new_df, source_date)

# COMMAND ----------

update_delta_table(laps_new_df, "f1_processed", "lap_times", 
                   "t.race_id = s.race_id AND t.driver_id = s.driver_id AND t.lap = s.lap", "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")