# Databricks notebook source
dbutils.widgets.text("sfdc_id", "sfdc_id")
dbutils.widgets.text("rate_discount", "0.0")

# COMMAND ----------

# MAGIC %run ./init/setup $rate_discount=$rate_discount $table_prefix=$table_prefix
