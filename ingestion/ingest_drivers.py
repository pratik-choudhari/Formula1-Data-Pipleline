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

name_schema = t.StructType(fields=[t.StructField("forename", t.StringType(), False),
                                   t.StructField("surname", t.StringType(), False)])

drivers_schema = t.StructType(fields=[t.StructField("driverId", t.IntegerType(), False),
                                   t.StructField("driverRef", t.StringType(), True),
                                   t.StructField("number", t.IntegerType(), True),
                                   t.StructField("code", t.StringType(), True),
                                   t.StructField("name", name_schema),
                                   t.StructField("dob", t.DateType(), True),
                                   t.StructField("nationality", t.StringType(), True),
                                   t.StructField("url", t.StringType(), True)])
drivers_df = spark.read.json(f"{raw_folder}/{source_date}/drivers.json", 
                          schema=drivers_schema)

# COMMAND ----------

drivers_new_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("driverRef", "driver_ref") \
        .withColumn("name", f.concat(f.col("name.forename"), f.lit(" "), f.col("name.surname")))

drivers_new_df = add_date(drivers_new_df, source_date)

# COMMAND ----------

drivers_new_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("success")