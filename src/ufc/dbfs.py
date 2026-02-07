from __future__ import annotations

"""DBFS helpers.

Why this exists:
  - In many Databricks workspaces the *Public DBFS root* (e.g. /FileStore) is disabled.
  - Also, using the local FUSE mount (/dbfs/...) can raise intermittent I/O errors,
    especially with Unity Catalog Volumes (/Volumes/...).

So for reading/writing small driver-side files (like checkpoints), prefer dbutils.fs
with paths like "dbfs:/Volumes/...".

These helpers keep local/dev compatibility by falling back to the local filesystem
when dbutils is not available.
"""

from typing import Optional


def _get_dbutils():
    """Return a DBUtils instance if running on Databricks, else None."""
    try:
        # Databricks notebooks expose DBUtils via pyspark.dbutils
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None


def assert_dbfs(path: str) -> str:
    if not path.startswith("dbfs:/"):
        raise ValueError(f"Esperava caminho dbfs:/..., veio: {path}")
    return path


def dbfs_to_local(dbfs_path: str) -> str:
    """Best-effort conversion for local filesystem access.

    IMPORTANT:
      - Do NOT use this for Unity Catalog Volumes (dbfs:/Volumes/...).
        On some workspaces the /dbfs FUSE mount can raise Errno 5.
      - Use dbutils.fs helpers below instead.
    """
    assert_dbfs(dbfs_path)
    if dbfs_path.startswith("dbfs:/Volumes/"):
        raise ValueError(
            "Nao converta dbfs:/Volumes/... para /dbfs/...; use dbutils.fs (put/head/ls)."
        )
    return "/dbfs/" + dbfs_path[len("dbfs:/"):]


def mkdirs(path: str) -> None:
    assert_dbfs(path)
    dbu = _get_dbutils()
    if dbu is not None:
        dbu.fs.mkdirs(path)
        return
    # local fallback
    import os

    os.makedirs(dbfs_to_local(path), exist_ok=True)


def exists(path: str) -> bool:
    assert_dbfs(path)
    dbu = _get_dbutils()
    if dbu is not None:
        try:
            dbu.fs.ls(path)
            return True
        except Exception:
            return False
    # local fallback
    import os

    try:
        return os.path.exists(dbfs_to_local(path))
    except Exception:
        return False


def read_text(path: str, default: str = "", max_bytes: int = 10_000_000) -> str:
    assert_dbfs(path)
    dbu = _get_dbutils()
    if dbu is not None:
        try:
            return dbu.fs.head(path, max_bytes)
        except Exception:
            return default
    # local fallback
    try:
        with open(dbfs_to_local(path), "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return default


def write_text(path: str, content: str, overwrite: bool = True) -> None:
    assert_dbfs(path)
    dbu = _get_dbutils()
    if dbu is not None:
        dbu.fs.put(path, content, overwrite=overwrite)
        return
    # local fallback
    local = dbfs_to_local(path)
    import os

    os.makedirs(os.path.dirname(local), exist_ok=True)
    if (not overwrite) and os.path.exists(local):
        raise FileExistsError(local)
    with open(local, "w", encoding="utf-8") as f:
        f.write(content)
