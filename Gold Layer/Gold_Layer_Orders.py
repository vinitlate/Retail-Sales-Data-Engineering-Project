# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
        CREATE OR REPLACE TABLE gold_orders AS
        SELECT
        o.transaction_id,
        o.customer_id,
        c.name AS customer_name,
        c.country AS customer_country,
        o.product_id,
        p.name AS product_name,
        p.category AS product_category,
        o.quantity,
        o.total_amount,
        o.transaction_date,
        o.payment_method,
        o.store_type,
        o.order_status,
        o.last_updated
        FROM
            globalretail.silver_layer.silver_orders o
        LEFT JOIN
            globalretail.silver_layer.silver_customers c ON o.customer_id = c.customer_id
        LEFT JOIN
            globalretail.silver_layer.silver_products p ON o.product_id = p.product_id
          """)