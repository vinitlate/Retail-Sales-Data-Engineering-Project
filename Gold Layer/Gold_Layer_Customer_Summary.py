# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
        CREATE OR REPLACE TABLE gold_customer_summary AS
        SELECT
            c.customer_id,
            c.name,
            c.country,
            COUNT(o.transaction_id) AS total_orders,
            SUM(o.total_amount) AS lifetime_value,
            AVG(o.total_amount) AS avg_order_value,
            MAX(o.transaction_date) AS last_order_date,
            c.customer_segment
        FROM
            globalretail.silver_layer.silver_customers c
        JOIN
            globalretail.silver_layer.silver_orders o
            ON c.customer_id = o.customer_id
        GROUP BY
        c.customer_id, c.name, c.country, c.customer_segment;
          """)