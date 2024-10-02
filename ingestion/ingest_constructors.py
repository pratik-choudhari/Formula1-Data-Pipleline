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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructors_df = spark.read.json(f"{raw_folder}/{source_date}/constructors.json", 
                          schema=constructors_schema)

# COMMAND ----------

constructors_renamed_df = constructors_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref")

constructors_dropped_df = constructors_renamed_df.drop("url")

constructors_new_df = add_date(constructors_dropped_df, source_date)

# COMMAND ----------

constructors_new_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")