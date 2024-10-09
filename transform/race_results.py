# Databricks notebook source
# MAGIC %run "../utils/utilities"

# COMMAND ----------

dbutils.widgets.text("source_date", "")
source_date = dbutils.widgets.get("source_date")

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t

# COMMAND ----------

# Full refresh data, load data without filtering
races_df = spark.sql("SELECT * FROM f1_processed.races")
circuits_df = spark.sql("SELECT * FROM f1_processed.circuits")
drivers_df = spark.sql("SELECT * FROM f1_processed.drivers")
constructors_df = spark.sql("SELECT * FROM f1_processed.constructors")

# Filter result(incremental load data) by source date
results_df = spark.sql(f"SELECT * FROM f1_processed.results WHERE source_date = '{source_date}'")

# COMMAND ----------

races_df_renamed = races_df.withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

circuits_df_renamed = circuits_df.withColumnRenamed("location", "circuit_location")

drivers_df_renamed = drivers_df.withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

results_df_renamed = results_df.withColumnRenamed("time", "race_time")

constructors_df_renamed = constructors_df.withColumnRenamed("name", "team")

# COMMAND ----------

races_df_selected = races_df_renamed.select(f.col("race_id"), f.col("circuit_id"), f.col("race_year"), 
                        f.col("race_name"), f.col("race_date"))

circuits_df_selected = circuits_df_renamed.select(f.col("circuit_id"), f.col("circuit_location"))

drivers_df_selected = drivers_df_renamed.select(f.col("driver_id"), f.col("driver_name"), f.col("driver_number"),
                        f.col("driver_nationality"))

results_df_selected = results_df_renamed.select(f.col("race_id"), f.col("result_id"), f.col("driver_id"), f.col("position"),
                        f.col("grid"), f.col("fastestLap"), f.col("race_time"), f.col("points"), f.col("constructorId"))

# COMMAND ----------

race_circuit_join = races_df_selected.join(circuits_df_selected, 
                                           races_df_selected.circuit_id == circuits_df_selected.circuit_id)

race_results = results_df_selected.join(race_circuit_join, races_df_selected.race_id == results_df_selected.race_id) \
    .join(drivers_df_selected, results_df_selected.driver_id == drivers_df_selected.driver_id) \
    .join(constructors_df_renamed, results_df_selected.constructorId == constructors_df_renamed.constructor_id)

# COMMAND ----------

proj_race_results = race_results.select(f.col("races.race_id"), f.col("race_year"), f.col("race_name"), f.col("race_date"), 
                                    f.col("circuit_location"), f.col("driver_name"), f.col("driver_number"),
                                    f.col("driver_nationality"), f.col("team"), f.col("grid"), f.col("fastestLap"),
                                    f.col("race_time"), f.col("points"), f.col("position"))

final_race_results = add_date(proj_race_results, source_date)

# COMMAND ----------

update_delta_table(final_race_results, "f1_presentation", "race_results", 
                   "t.race_id = s.race_id AND t.driver_name = s.driver_name", "race_id")
