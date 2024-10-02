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

results_schema = ("resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, "
                  "position INT, positionText STRING, positionOrder INT, points DOUBLE, laps INT, "
                  "time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, "
                  "fastestLapSpeed STRING, statusId INT")
results_df = spark.read.json(f"{raw_folder}/{source_date}/results.json", 
                          schema=results_schema)

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                .withColumnRenamed("raceId", "race_id") \
                .withColumnRenamed("driverId", "driver_id")

results_renamed_df = add_date(results_renamed_df, source_date)

results_new_df = results_renamed_df.drop("statusId")

results_dedupe_df = results_new_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

update_delta_table(results_dedupe_df, "f1_processed", "results", "t.result_id = s.result_id", "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")