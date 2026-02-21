import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict
from .config import BRONZE_DIR
from .logger import get_logger

log = get_logger("bronze_writer")

def write_bronze(events: List[Dict]) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    ingest_dt = datetime.now(timezone.utc)
    ingest_date = ingest_dt.strftime("%Y-%m-%d")
    ingest_ts = ingest_dt.strftime("%Y-%m-%dT%H%M%SZ")

    out_dir = BRONZE_DIR / f"ingest_date={ingest_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"events_{ingest_ts}.jsonl"

    with out_path.open("w", encoding="utf-8") as f:
        for ev in events:
            record = {
                "ingested_at": ingest_dt.isoformat().replace("+00:00", "Z"),
                "event": ev,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    log.info(f"Bronze written: {out_path} (rows={len(events)})")
    return out_path
