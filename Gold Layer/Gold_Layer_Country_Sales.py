# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
        CREATE OR REPLACE TABLE gold_country_sales AS
        SELECT
            c.country,
            COUNT(o.transaction_id) AS total_orders,
            SUM(o.total_amount) AS total_sales,
            AVG(o.total_amount) AS avg_order_value
        FROM
            globalretail.silver_layer.silver_customers c
        JOIN
            globalretail.silver_layer.silver_orders o
            ON c.customer_id = o.customer_id
        GROUP BY
            c.country
          """)