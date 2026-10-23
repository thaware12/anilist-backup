import json
import sys
import time
import requests
from pathlib import Path

API_URL = "https://graphql.anilist.co"
STATE_FILE = Path("state.json")

# Max HTTP requests per 5-minute GitHub Action run (~3 minutes runtime)
MAX_REQUESTS_PER_RUN = 75
# Number of IDs to check per window (max 10 pages of 50 items)
ID_WINDOW_SIZE = 500

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

# 1. Valid AniList query to get the highest ID & latest updatedAt timestamp
WATERMARK_QUERY = """
query {
  newestId: Page(page: 1, perPage: 1) {
    media(sort: ID_DESC) {
      id
    }
  }
  newestUpdate: Page(page: 1, perPage: 1) {
    media(sort: UPDATED_AT_DESC) {
      updatedAt
    }
  }
}
"""

# 2. Valid AniList query using `id_in: [Int]` (officially supported on Page.media)
SEED_QUERY = f"""
query ($page: Int, $ids: [Int]) {{
  Page(page: $page, perPage: 50) {{
    pageInfo {{
      hasNextPage
    }}
    media(id_in: $ids, sort: ID) {{
      {MEDIA_FIELDS}
    }}
  }}
}}
"""

# 3. Valid AniList query for 5-minute incremental updates
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

UPSTREAM_OUTAGE_CODES = {403, 408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}


def load_state() -> dict:
    default_state = {
        "seed_complete": False,
        "seed_last_id": 0,
        "max_target_id": 0,
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
            timeout=25,
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

    return payload["data"]


def run_initial_seed(state: dict, shard_cache: dict, dirty_shards: dict) -> None:
    requests_made = 0

    # Initialize max_target_id and last_updated_at on the very first run
    if state["max_target_id"] == 0 or state["last_updated_at"] == 0:
        wm_data = graphql_request(WATERMARK_QUERY, {}, state, dirty_shards)
        requests_made += 1
        if wm_data["newestId"]["media"]:
            state["max_target_id"] = wm_data["newestId"]["media"][0]["id"]
        if wm_data["newestUpdate"]["media"]:
            state["last_updated_at"] = wm_data["newestUpdate"]["media"][0]["updatedAt"]
        print(f"[INFO] Target Max ID: {state['max_target_id']}, Initial Watermark: {state['last_updated_at']}")
        time.sleep(2.1)

    while requests_made < MAX_REQUESTS_PER_RUN:
        start_id = state["seed_last_id"] + 1
        if start_id > state["max_target_id"]:
            state["seed_complete"] = True
            print("[OK] Initial full backup is 100% complete!")
            break

        end_id = min(start_id + ID_WINDOW_SIZE - 1, state["max_target_id"])
        id_list = list(range(start_id, end_id + 1))

        page = 1
        saved_in_window = 0
        while True:
            data = graphql_request(
                SEED_QUERY,
                {"page": page, "ids": id_list},
                state,
                dirty_shards,
            )
            requests_made += 1
            page_data = data["Page"]
            media_list = page_data["media"]

            for item in media_list:
                store_item_in_memory(item, shard_cache, dirty_shards)
                saved_in_window += 1

            if not page_data["pageInfo"]["hasNextPage"] or requests_made >= MAX_REQUESTS_PER_RUN:
                break

            page += 1
            time.sleep(2.1)

        # Only advance seed_last_id to end_id if we finished all pages in this window
        if not page_data["pageInfo"]["hasNextPage"]:
            state["seed_last_id"] = end_id
        elif media_list:
            state["seed_last_id"] = media_list[-1]["id"]

        print(
            f"[SEED] Window {start_id}..{end_id}: saved {saved_in_window} items "
            f"({requests_made}/{MAX_REQUESTS_PER_RUN} requests used)"
        )
        time.sleep(2.1)


def run_incremental_sync(state: dict, shard_cache: dict, dirty_shards: dict) -> None:
    last_seen = state["last_updated_at"]
    newest_timestamp = last_seen

    for page in range(1, 50 + 1):
        data = graphql_request(DELTA_QUERY, {"page": page}, state, dirty_shards)
        page_data = data["Page"]
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
