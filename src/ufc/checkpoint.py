from __future__ import annotations

import json
from typing import Set

from . import dbfs

def checkpoint_path(bronze_root: str, name: str) -> str:
    return f"{bronze_root}/_checkpoints/{name}.json"

def load_checkpoint_set(bronze_root: str, name: str) -> Set[str]:
    path_dbfs = checkpoint_path(bronze_root, name)
    if not dbfs.exists(path_dbfs):
        return set()

    raw = dbfs.read_text(path_dbfs, default="")
    if not raw.strip():
        return set()

    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return {str(x) for x in data}
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return {str(x) for x in data["items"]}
    except Exception:
        return set()

    return set()

def save_checkpoint_set(bronze_root: str, name: str, items: Set[str]):
    path_dbfs = checkpoint_path(bronze_root, name)
    # garante o diretório
    dbfs.mkdirs(f"{bronze_root}/_checkpoints")
    dbfs.write_text(
        path_dbfs,
        json.dumps(sorted(list(items)), ensure_ascii=False, indent=2),
        overwrite=True,
    )