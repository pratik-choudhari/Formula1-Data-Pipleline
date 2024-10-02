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

races_schema = t.StructType(fields=[t.StructField("raceId", t.IntegerType(), False),
                                   t.StructField("year", t.IntegerType(), True),
                                   t.StructField("round", t.IntegerType(), True),
                                   t.StructField("circuitId", t.IntegerType(), True),
                                   t.StructField("name", t.StringType(), True),
                                   t.StructField("date", t.DateType(), True),
                                   t.StructField("time", t.StringType(), True),
                                   t.StructField("url", t.StringType(), True)])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder}/{source_date}/races.csv", 
                          schema=races_schema, 
                          header=True)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumn("race_timestamp", f.to_timestamp(f.concat(f.col("date"), f.lit(" "), f.col("time"))))

races_new_df = add_date(races_renamed_df, source_date)

races_new_df = races_new_df.select(*['race_id', 'race_year', 'round', 'circuit_id', 'name', 
                                     'url', 'ingestion_date', 'race_timestamp', 'source_date'])

# COMMAND ----------

races_new_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("success")