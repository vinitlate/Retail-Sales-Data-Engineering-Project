# Databricks notebook source
filepath = "/Volumes/globalretail/bronze_layer/product_catalog/products.json"
df= spark.read.json(filepath)
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new = df.withColumn("ingestion_timestamp", current_timestamp())
display(df_new)

# COMMAND ----------

spark.sql("use catalog globalretail")
spark.sql("use schema bronze_layer")
df_new.write.format("delta").mode("append").saveAsTable("bronze_product_data")

# COMMAND ----------

spark.sql("select * from globalretail.bronze_layer.bronze_product_data").display()

# COMMAND ----------

import datetime

archive_folder = "/Volumes/globalretail/bronze_layer/product_catalog/archive/"
archive_filepath = archive_folder + " " + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
dbutils.fs.mv(filepath, archive_filepath)