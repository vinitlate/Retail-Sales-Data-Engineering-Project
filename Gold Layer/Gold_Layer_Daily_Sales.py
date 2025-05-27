# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
          create or replace table gold_daily_sales as
          select
                transaction_date,
                sum(total_amount) as daily_total_sales
            from
                globalretail.silver_layer.silver_orders
          group by
                transaction_date
          """)

# COMMAND ----------

