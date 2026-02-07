# Databricks notebook source
# UFC Lakehouse - Bronze (scrape -> delta)
#
# Rode primeiro:
#   %run ./00_setup_databricks
#
# Saídas:
#   dbfs:/FileStore/ufc/bronze/delta/{events,fights,fighters}
#   Tabelas externas (metastore):
#     ufc.bronze_events / ufc.bronze_fights / ufc.bronze_fighters

# COMMAND ----------

# MAGIC %run ./00_setup_databricks

# COMMAND ----------

from datetime import date
from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id
from src.ufc.pipelines.events_pipeline import run_events_pipeline
from src.ufc.pipelines.fights_pipeline import run_fights_pipeline
from src.ufc.pipelines.fighters_pipeline import run_fighters_pipeline

BRONZE_ROOT = "dbfs:/Volumes/ufc_fight/default/ufc_lakehouse/ufc/bronze"
INGESTION_DATE = str(date.today())
RUN_ID = make_run_id()

cfg = UFCConfig(bronze_root=BRONZE_ROOT)

print("BRONZE_ROOT:", BRONZE_ROOT)
print("INGESTION_DATE:", INGESTION_DATE)
print("RUN_ID:", RUN_ID)

# COMMAND ----------

n_events = run_events_pipeline(spark, cfg, ingestion_date=INGESTION_DATE, run_id=RUN_ID)
print("[events] rows:", n_events)

# COMMAND ----------

n_fights = run_fights_pipeline(spark, cfg, ingestion_date=INGESTION_DATE, run_id=RUN_ID)
print("[fights] rows:", n_fights)

# COMMAND ----------

n_fighters = run_fighters_pipeline(spark, cfg, ingestion_date=INGESTION_DATE, run_id=RUN_ID)
print("[fighters] rows:", n_fighters)

# COMMAND ----------

# Cria tabelas externas apontando para os paths Delta
spark.sql("CREATE DATABASE IF NOT EXISTS ufc")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ufc.bronze_events
USING DELTA
LOCATION '{BRONZE_ROOT}/delta/events'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ufc.bronze_fights
USING DELTA
LOCATION '{BRONZE_ROOT}/delta/fights'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ufc.bronze_fighters
USING DELTA
LOCATION '{BRONZE_ROOT}/delta/fighters'
""")

display(spark.table("ufc.bronze_fights").limit(10))
