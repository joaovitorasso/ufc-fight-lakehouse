# Databricks notebook source
# UFC Lakehouse - RUN ALL
#
# 0) setup deps + sys.path
# 1) bronze scrape -> delta + create bronze tables
# 2) silver normalize -> tables
# 3) gold aggregates -> tables


# COMMAND ----------

# MAGIC %run ./00_setup_databricks

# COMMAND ----------

# MAGIC %run ./01_bronze_run_all

# COMMAND ----------

# MAGIC %run ./11_build_silver

# COMMAND ----------

# MAGIC %run ./12_build_gold
