# Databricks notebook source
# UFC Lakehouse - Setup (Databricks)
# 1) Instala dependências do projeto
# 2) Descobre automaticamente a raiz do projeto no Workspace
# 3) Adiciona a raiz ao sys.path para permitir: `from src...`

# COMMAND ----------
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------
import sys

# Descobre o caminho do notebook no Workspace (ex.: /Users/<email>/ufc-fights-lakehouse/notebooks/00_setup_databricks)
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nb_path = ctx.notebookPath().get()

# Projeto = pasta acima de /notebooks
project_rel = nb_path.rsplit("/notebooks/", 1)[0]
PROJECT_ROOT = "/Workspace" + project_rel  # workspace files mount

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print("Notebook path:", nb_path)
print("PROJECT_ROOT:", PROJECT_ROOT)
print("sys.path[0]:", sys.path[0])

# COMMAND ----------
# Se o seu DBR exigir restart após %pip, descomente:
# dbutils.library.restartPython()
