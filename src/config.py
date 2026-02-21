from pathlib import Path

# API
GITHUB_EVENTS_URL = "https://api.github.com/events"
PER_PAGE = 100  # max allowed by GitHub is typically 100 for many endpoints

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze" / "events"
SILVER_DIR = DATA_DIR / "silver" / "events"
STATE_DIR = PROJECT_ROOT / "state"
CHECKPOINT_PATH = STATE_DIR / "checkpoints.json"