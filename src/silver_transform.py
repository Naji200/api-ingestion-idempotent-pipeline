import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple
from .config import SILVER_DIR
from .logger import get_logger

log = get_logger("silver_transform")

def _flatten_events(events: List[Dict]) -> pd.DataFrame:
    rows = []
    for ev in events:
        rows.append({
            "id": ev.get("id"),
            "type": ev.get("type"),
            "actor_login": (ev.get("actor") or {}).get("login"),
            "repo_name": (ev.get("repo") or {}).get("name"),
            "created_at": ev.get("created_at"),
            "payload": ev.get("payload"),
        })
    return pd.DataFrame(rows)

def _quality_checks(df: pd.DataFrame):
    # 1) id not null
    if df["id"].isna().any():
        raise ValueError("Quality check failed: id has nulls.")

    # 2) created_at parsed
    if df["created_at_ts"].isna().any():
        raise ValueError("Quality check failed: created_at could not be parsed for some rows.")

    # 3) no duplicates by id
    if df["id"].duplicated().any():
        raise ValueError("Quality check failed: duplicate ids in silver dataframe.")

def transform_to_silver(events: List[Dict]) -> Tuple[int, int]:
    """
    Returns (written_rows, unique_dates)
    """
    if not events:
        log.info("No events to transform.")
        return 0, 0

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    df = _flatten_events(events)

    # Parse timestamps
    df["created_at_ts"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["event_date"] = df["created_at_ts"].dt.strftime("%Y-%m-%d")

    # Drop invalid
    before = len(df)
    df = df.dropna(subset=["id", "created_at_ts", "event_date"])
    dropped = before - len(df)

    # Dedup
    df = df.drop_duplicates(subset=["id"], keep="last")

    # Quality checks
    _quality_checks(df)

    # Write partitioned parquet
    written = 0
    dates = sorted(df["event_date"].unique().tolist())

    for d in dates:
        part = df[df["event_date"] == d].copy()
        out_dir = SILVER_DIR / f"date={d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # file name is deterministic-ish per run but not required
        out_path = out_dir / f"part-{pd.Timestamp.utcnow().strftime('%Y%m%dT%H%M%S')}.parquet"

        # Write parquet
        part.drop(columns=["event_date"]).to_parquet(out_path, index=False)
        written += len(part)

    log.info(f"Silver written rows={written}, dates={len(dates)}, dropped_invalid={dropped}")
    return written, len(dates)