# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
          create or replace table gold_category_sales as
          select 
                p.category as product_category,
                sum(o.total_amount) as category_total_sales
            from 
                globalretail.silver_layer.silver_orders o
            join
                globalretail.silver_layer.silver_products p
                on o.product_id = p.product_id
            group by 
                p.category
          """)