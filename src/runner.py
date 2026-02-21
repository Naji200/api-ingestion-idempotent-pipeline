import json
from datetime import datetime, timezone
import pandas as pd

from .logger import get_logger
from .config import CHECKPOINT_PATH, STATE_DIR
from .api_client import fetch_github_events
from .bronze_writer import write_bronze
from .silver_transform import transform_to_silver

log = get_logger("runner")

def load_checkpoint() -> str:
    if not CHECKPOINT_PATH.exists():
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        data = {"github_events": {"last_created_at": "1970-01-01T00:00:00Z"}}
        CHECKPOINT_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return data["github_events"]["last_created_at"]

    data = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    return data.get("github_events", {}).get("last_created_at", "1970-01-01T00:00:00Z")

def save_checkpoint(new_last_created_at: str):
    data = {"github_events": {"last_created_at": new_last_created_at}}
    CHECKPOINT_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")

def main():
    last_created_at = load_checkpoint()
    log.info(f"Loaded checkpoint last_created_at={last_created_at}")

    # 1) Fetch raw events (we fetch latest pages then filter)
    raw_events = fetch_github_events(max_pages=3)
    log.info(f"Fetched raw events={len(raw_events)}")

    # 2) Filter events newer than checkpoint
    last_ts = pd.to_datetime(last_created_at, utc=True, errors="coerce")
    if pd.isna(last_ts):
        last_ts = pd.Timestamp("1970-01-01", tz="UTC")

    filtered = []
    for ev in raw_events:
        ts = pd.to_datetime(ev.get("created_at"), utc=True, errors="coerce")
        if pd.notna(ts) and ts > last_ts:
            filtered.append(ev)

    log.info(f"Filtered new events={len(filtered)} (after checkpoint)")

    if not filtered:
        log.info("No new events. Exiting without updating checkpoint.")
        return

    # 3) Write bronze
    bronze_path = write_bronze(filtered)

    # 4) Transform to silver + quality checks
    written_rows, n_dates = transform_to_silver(filtered)

    # 5) Update checkpoint ONLY after success
    newest_ts = max(pd.to_datetime(ev["created_at"], utc=True) for ev in filtered)
    new_checkpoint = newest_ts.isoformat().replace("+00:00", "Z")

    save_checkpoint(new_checkpoint)
    log.info(f"SUCCESS ✅ bronze={bronze_path.name} silver_rows={written_rows} dates={n_dates}")
    log.info(f"Checkpoint updated to {new_checkpoint}")

if __name__ == "__main__":
    main()