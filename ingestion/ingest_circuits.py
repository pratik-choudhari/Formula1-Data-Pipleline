# Databricks notebook source
spark.sql("SELECT current_user()").collect()

# COMMAND ----------

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

circuits_schema = t.StructType(fields=[t.StructField("circuitId", t.IntegerType(), False),
                                       t.StructField("circuitRef", t.StringType(), True),
                                       t.StructField("name", t.StringType(), True),
                                       t.StructField("location", t.StringType(), True),
                                       t.StructField("country", t.StringType(), True),
                                       t.StructField("lat", t.DoubleType(), True),
                                       t.StructField("lng", t.DoubleType(), True),
                                       t.StructField("alt", t.DoubleType(), True),
                                       t.StructField("url", t.StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder}/{source_date}/circuits.csv", 
                             schema=circuits_schema, 
                             header=True)

# COMMAND ----------

circuits_renamed_df = circuits_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

circuits_new_df = add_date(circuits_renamed_df, source_date)

# COMMAND ----------

circuits_new_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("success")