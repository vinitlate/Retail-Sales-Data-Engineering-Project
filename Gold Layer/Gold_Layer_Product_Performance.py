# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
        CREATE OR REPLACE TABLE gold_product_performance AS
        SELECT
            p.product_id,
            p.name AS product_name,
            p.category,
            p.brand,
            SUM(o.quantity) AS total_units_sold,
            SUM(o.total_amount) AS total_revenue,
            AVG(p.rating) AS avg_rating,
            p.price_category
        FROM
            globalretail.silver_layer.silver_orders o
        JOIN
            globalretail.silver_layer.silver_products p
            ON o.product_id = p.product_id
        GROUP BY
            p.product_id, p.name, p.category, p.brand, p.price_category;
          """)