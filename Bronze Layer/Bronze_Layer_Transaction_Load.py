# Databricks notebook source
filepath = "/Volumes/globalretail/bronze_layer/transactions/transaction.snappy.parquet"
df= spark.read.parquet(filepath)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

new_df = df.withColumn("transaction_date", to_timestamp(col("transaction_date")))
display(new_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = new_df.withColumn("ingestion_timestamp", current_timestamp())
display(final_df)

# COMMAND ----------

spark.sql("use catalog globalretail")
spark.sql("use schema bronze_layer")
final_df.write.format("delta").mode("append").saveAsTable("bronze_transaction_data")

# COMMAND ----------

spark.sql("select * from globalretail.bronze_layer.bronze_transaction_data").display()

# COMMAND ----------

import datetime

archive_folder = "/Volumes/globalretail/bronze_layer/transactions/archive/"
archive_filepath = archive_folder + " " + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
dbutils.fs.mv(filepath, archive_filepath)