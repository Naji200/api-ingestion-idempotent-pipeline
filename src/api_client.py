import time
import requests
from typing import List, Dict, Optional
from .logger import get_logger
from .config import GITHUB_EVENTS_URL, PER_PAGE

log = get_logger("api_client")

def _sleep_if_rate_limited(resp: requests.Response):
    # GitHub rate limit headers:
    # X-RateLimit-Remaining, X-RateLimit-Reset (epoch seconds)
    remaining = resp.headers.get("X-RateLimit-Remaining")
    reset = resp.headers.get("X-RateLimit-Reset")

    if remaining == "0" and reset:
        reset_ts = int(reset)
        now = int(time.time())
        wait = max(reset_ts - now, 1)
        log.warning(f"Rate limited. Sleeping {wait}s...")
        time.sleep(wait)

def fetch_github_events(max_pages: int = 3, session: Optional[requests.Session] = None) -> List[Dict]:
    """
    GitHub /events is 'latest public events'. It doesn't support a true 'since' query parameter.
    So we fetch pages, then later we filter using checkpoint (created_at).
    """
    s = session or requests.Session()
    all_events: List[Dict] = []

    url = GITHUB_EVENTS_URL
    params = {"per_page": PER_PAGE}

    for page in range(1, max_pages + 1):
        log.info(f"Fetching page {page}...")
        retries = 3

        while retries > 0:
            resp = s.get(url, params=params, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                if not isinstance(data, list):
                    raise ValueError("Unexpected API response format (expected list).")

                all_events.extend(data)

                _sleep_if_rate_limited(resp)

                # pagination: GitHub uses Link header
                link = resp.headers.get("Link", "")
                next_url = None
                for part in link.split(","):
                    if 'rel="next"' in part:
                        next_url = part.split(";")[0].strip().strip("<>")
                        break

                if not next_url:
                    log.info("No next page link found. Stopping.")
                    return all_events

                url = next_url
                params = None  # next_url already includes query params
                break

            elif resp.status_code in (429, 403):
                _sleep_if_rate_limited(resp)
                retries -= 1
                time.sleep(2)

            else:
                retries -= 1
                log.warning(f"API error {resp.status_code}: {resp.text[:200]}")
                time.sleep(2)

        if retries == 0:
            raise RuntimeError("Failed to fetch after retries.")

    return all_events