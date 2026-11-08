"""Configuration loader.  Reads config.json from the repo root."""
from __future__ import annotations
import json
import os
from pathlib import Path


_DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "config.json"


def load(path: str | None = None) -> dict:
    """Load and return the config dict.

    Search order:
      1. *path* argument (absolute or relative to cwd)
      2. env var ``FALCON_CONFIG``
      3. ``<repo_root>/config/config.json``
    """
    cfg_path = Path(path or os.getenv("FALCON_CONFIG", str(_DEFAULT_CONFIG)))
    if not cfg_path.is_absolute():
        cfg_path = Path.cwd() / cfg_path
    with open(cfg_path) as fh:
        return json.load(fh)
