# Databricks notebook source
import os
import sys
from datetime import date


def _current_user() -> str | None:
    try:
        return (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .tags()
            .apply("user")
        )
    except Exception:
        return None


USER = _current_user() or "<seu_email@empresa.com>"
_candidates = [
    f"/Workspace/Users/{USER}/ufc-fight-lakehouse",
    f"/Workspace/Users/{USER}/ufc-fights-lakehouse",
]
PROJECT_ROOT = next((p for p in _candidates if os.path.exists(p)), _candidates[0])
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Storage: por padrao use Unity Catalog Volume (evita /FileStore)
BRONZE_ROOT = os.environ.get(
    "UFC_BRONZE_ROOT",
    "dbfs:/Volumes/ufc_fight/default/ufc_lakehouse/ufc/bronze",
)

# 3) Uma data de ingestão consistente
INGESTION_DATE = str(date.today())

print("PROJECT_ROOT:", PROJECT_ROOT)
print("BRONZE_ROOT:", BRONZE_ROOT)
print("INGESTION_DATE:", INGESTION_DATE)

# COMMAND ----------

from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id
from src.ufc.pipelines.fights_pipeline import run_fights_pipeline

cfg = UFCConfig(bronze_root=BRONZE_ROOT)
run_id = make_run_id()

run_fights_pipeline(
    spark,
    cfg,
    ingestion_date=INGESTION_DATE,
    run_id=run_id,
    only_new_events=True
)
print("fights done. run_id:", run_id)