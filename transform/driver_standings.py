# Databricks notebook source
# MAGIC %run "../utils/utilities"

# COMMAND ----------

dbutils.widgets.text("source_date", "")
source_date = dbutils.widgets.get("source_date")

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

# COMMAND ----------

race_year_list = spark.sql(f"SELECT DISTINCT race_year FROM f1_presentation.race_results WHERE source_date = '{source_date}'").collect()
race_year_list = [str(elem.race_year) for elem in race_year_list]

# COMMAND ----------

query = f"SELECT * FROM f1_presentation.race_results WHERE race_year IN ({','.join(race_year_list)})"
race_results_df = spark.sql(query)

# COMMAND ----------

driver_standings_df = race_results_df \
          .groupBy("race_year", "driver_name", "driver_nationality") \
          .agg(f.sum("points").alias("total_points"),
               f.count(f.when(f.col("position") == 1, True)).alias("wins"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(f.desc("total_points"), f.desc("wins"))
final_df = driver_standings_df.withColumn("rank", f.rank().over(driver_rank_spec))

# COMMAND ----------

update_delta_table(final_df, 'f1_presentation', 'driver_standings', 
                   "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year", 'race_year')
