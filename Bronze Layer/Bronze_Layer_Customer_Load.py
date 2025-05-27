# Databricks notebook source
filepath = "/Volumes/globalretail/bronze_layer/customer_data/customer.csv"
df= spark.read.csv(filepath, header=True, inferSchema=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new = df.withColumn("ingestion_timestamp", current_timestamp())
display(df_new)

# COMMAND ----------

spark.sql("use catalog globalretail")
spark.sql("use schema bronze_layer")
df_new.write.format("delta").mode("append").saveAsTable("bronze_customer_data")

# COMMAND ----------

spark.sql("select * from globalretail.bronze_layer.bronze_customer_data").display()

# COMMAND ----------

import datetime

archive_folder = "/Volumes/globalretail/bronze_layer/customer_data/archive/"
archive_filepath = archive_folder + " " + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
dbutils.fs.mv(filepath, archive_filepath)