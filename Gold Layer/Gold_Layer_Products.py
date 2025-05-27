# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
        CREATE OR REPLACE TABLE gold_products AS
        SELECT
            product_id,
            name,
            category,
            brand,
            price,
            stock_quantity,
            rating,
            is_active,
            price_category,
            stock_status,
            last_updated
        FROM
            globalretail.silver_layer.silver_products;
          """)