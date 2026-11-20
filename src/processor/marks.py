"""Progress marks ("how far have I read?") stored as small JSON files under CHECKPOINT_DIR.

Silver uses this to remember the last bronze_loaded_at it took from payment_events_bronze.
Java analogy: a tiny key-value store backed by one file per key, like Properties.store().
"""

import json
import os
from datetime import datetime
from pathlib import Path


def _mark_path(name):
    return Path(os.getenv("CHECKPOINT_DIR", "./checkpoints")) / "marks" / f"{name}.json"


def read_mark(name):
    """Return the saved datetime for `name`, or None if nothing has been saved yet."""
    path = _mark_path(name)
    if not path.exists():
        return None
    return datetime.fromisoformat(json.loads(path.read_text(encoding="utf-8"))["value"])


def save_mark(name, value):
    """Save `value` (a timezone-aware datetime) for `name`.

    Writes to a temp file, then renames it over the old one, so a crash mid-write can never
    leave a half-written mark (os.replace is atomic, like Files.move(..., ATOMIC_MOVE)).
    """
    path = _mark_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"value": value.isoformat()}), encoding="utf-8")
    os.replace(tmp, path)
