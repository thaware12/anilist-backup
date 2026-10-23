import json
import sys
import time
import requests
from pathlib import Path

API_URL = "https://graphql.anilist.co"
STATE_FILE = Path("state.json")

# Fetch 80 pages (4,000 items) per 5-minute Action run so every run
# finishes in ~3 minutes and commits its progress cleanly.
MAX_PAGES_PER_RUN = 80

MEDIA_FIELDS = """
      id
      idMal
      type
      format
      status
      updatedAt
      title {
        romaji
        english
        native
      }
      episodes
      chapters
      volumes
      averageScore
      popularity
      genres
"""

# Used during Phase 1 (Initial Full Backup): always stays on page 1 using id_greater
SEED_QUERY = f"""
query ($lastId: Int) {{
  Page(page: 1, perPage: 50) {{
    pageInfo {{
      hasNextPage
    }}
    media(id_greater: $lastId, sort: ID) {{
      {MEDIA_FIELDS}
    }}
  }}
}}
"""

# Used during Phase 2 (5-Minute Delta Updates): fetches recently modified media
DELTA_QUERY = f"""
query ($page: Int) {{
  Page(page: $page, perPage: 50) {{
    pageInfo {{
      hasNextPage
    }}
    media(sort: UPDATED_AT_DESC) {{
      {MEDIA_FIELDS}
    }}
  }}
}}
"""

# HTTP codes indicating AniList or Cloudflare is down/rate-limiting (do not fail workflow)
UPSTREAM_OUTAGE_CODES = {403, 408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}


def load_state() -> dict:
    default_state = {
        "seed_complete": False,
        "seed_last_id": 0,
        "last_updated_at": 0,
    }
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            default_state.update(saved)
        except Exception as err:
            print(f"[WARN] Could not parse state.json, starting fresh: {err}")
    return default_state


def save_progress(state: dict, dirty_shards: dict[Path, dict]) -> None:
    """Flush all modified shard files and state.json to disk."""
    for shard_path, shard_data in dirty_shards.items():
        shard_path.parent.mkdir(parents=True, exist_ok=True)
        shard_path.write_text(
            json.dumps(shard_data, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    dirty_shards.clear()
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def store_item_in_memory(item: dict, shard_cache: dict[Path, dict], dirty_shards: dict[Path, dict]) -> None:
    media_type = (item.get("type") or "ANIME").lower()
    shard_id = item["id"] // 1000
    shard_path = Path(f"data/{media_type}/shard_{shard_id:04d}.json")

    if shard_path not in shard_cache:
        if shard_path.exists():
            shard_cache[shard_path] = json.loads(shard_path.read_text(encoding="utf-8"))
        else:
            shard_cache[shard_path] = {}

    shard_cache[shard_path][str(item["id"])] = item
    dirty_shards[shard_path] = shard_cache[shard_path]


def graphql_request(query: str, variables: dict, state: dict, dirty_shards: dict[Path, dict]) -> dict:
    headers = {
        "User-Agent": "anilist-backup/1.0 (https://github.com/thaware12/anilist-backup)",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        response = requests.post(
            API_URL,
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=20,
        )
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_err:
        print(f"[WARN] Network/Cloudflare unreachable: {net_err}. Saving progress and exiting cleanly.")
        save_progress(state, dirty_shards)
        sys.exit(0)

    if response.status_code in UPSTREAM_OUTAGE_CODES:
        print(f"[WARN] Upstream HTTP {response.status_code} (AniList/Cloudflare issue). Saving progress and exiting cleanly.")
        save_progress(state, dirty_shards)
        sys.exit(0)

    if not response.ok:
        # Log the exact GraphQL error message before raising so debugging is easy
        print(f"[ERROR] HTTP {response.status_code} Body: {response.text}")
        save_progress(state, dirty_shards)
        response.raise_for_status()

    try:
        payload = response.json()
    except ValueError:
        print("[WARN] Non-JSON response received from upstream proxy. Saving progress and exiting cleanly.")
        save_progress(state, dirty_shards)
        sys.exit(0)

    if "errors" in payload:
        errors = payload["errors"]
        for err in errors:
            if err.get("status") in UPSTREAM_OUTAGE_CODES or "temporarily disabled" in err.get("message", ""):
                print(f"[WARN] AniList API degraded: {err}. Saving progress and exiting cleanly.")
                save_progress(state, dirty_shards)
                sys.exit(0)
        save_progress(state, dirty_shards)
        raise RuntimeError(f"GraphQL Schema/Query Error: {errors}")

    return payload["data"]["Page"]


def run_initial_seed(state: dict, shard_cache: dict, dirty_shards: dict) -> None:
    # Record the current latest updatedAt watermark before starting the seed
    if state["last_updated_at"] == 0:
        first_delta = graphql_request(DELTA_QUERY, {"page": 1}, state, dirty_shards)
        if first_delta["media"]:
            state["last_updated_at"] = first_delta["media"][0]["updatedAt"]
        time.sleep(2.1)

    pages_fetched = 0
    while pages_fetched < MAX_PAGES_PER_RUN:
        page_data = graphql_request(
            SEED_QUERY,
            {"lastId": state["seed_last_id"]},
            state,
            dirty_shards,
        )
        media_list = page_data["media"]
        if not media_list:
            state["seed_complete"] = True
            print("[OK] Initial full backup is 100% complete!")
            break

        for item in media_list:
            store_item_in_memory(item, shard_cache, dirty_shards)

        state["seed_last_id"] = media_list[-1]["id"]
        pages_fetched += 1
        print(f"[SEED] Batch {pages_fetched}/{MAX_PAGES_PER_RUN} — saved up to ID {state['seed_last_id']}")

        if not page_data["pageInfo"]["hasNextPage"]:
            state["seed_complete"] = True
            print("[OK] Initial full backup is 100% complete!")
            break

        time.sleep(2.1)


def run_incremental_sync(state: dict, shard_cache: dict, dirty_shards: dict) -> None:
    last_seen = state["last_updated_at"]
    newest_timestamp = last_seen

    # Never exceed page 100 (AniList's 5,000-entry pagination cap)
    for page in range(1, 100 + 1):
        page_data = graphql_request(DELTA_QUERY, {"page": page}, state, dirty_shards)
        media_list = page_data["media"]
        if not media_list:
            break

        if page == 1:
            newest_timestamp = max(newest_timestamp, media_list[0]["updatedAt"])

        stop_pagination = False
        for item in media_list:
            if item["updatedAt"] <= last_seen:
                stop_pagination = True
                break
            store_item_in_memory(item, shard_cache, dirty_shards)

        print(f"[DELTA] Processed page {page}")
        if stop_pagination or not page_data["pageInfo"]["hasNextPage"]:
            break

        time.sleep(2.1)

    state["last_updated_at"] = newest_timestamp
    print(f"[OK] Incremental sync complete. New watermark: {newest_timestamp}")


def main():
    state = load_state()
    shard_cache: dict[Path, dict] = {}
    dirty_shards: dict[Path, dict] = {}

    if not state["seed_complete"]:
        print(f"[INFO] Running initial seed starting after ID {state['seed_last_id']}...")
        run_initial_seed(state, shard_cache, dirty_shards)
    else:
        print(f"[INFO] Running incremental update since timestamp {state['last_updated_at']}...")
        run_incremental_sync(state, shard_cache, dirty_shards)

    save_progress(state, dirty_shards)


if __name__ == "__main__":
    main()
