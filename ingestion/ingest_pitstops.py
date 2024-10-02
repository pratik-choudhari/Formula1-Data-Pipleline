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

pitstops_schema = ("raceId INT, driverId INT, stop INT, lap INT, time STRING, "
                  "duration DOUBLE, milliseconds INT")
pitstops_df = spark.read.json(f"{raw_folder}/{source_date}/pit_stops.json", 
                             multiLine=True, 
                             schema=pitstops_schema)

# COMMAND ----------

pitstops_new_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id")

pitstops_new_df = add_date(pitstops_new_df, source_date)

# COMMAND ----------

update_delta_table(pitstops_new_df, "f1_processed", "pit_stops", 
                   "t.race_id = s.race_id AND t.stop = s.stop AND t.driver_id = s.driver_id", "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")