import json
import sys
import time
import requests
from pathlib import Path

API_URL = "https://graphql.anilist.co"
STATE_FILE = Path("state.json")

QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      hasNextPage
    }
    media(sort: UPDATED_AT_DESC) {
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
    }
  }
}
"""

# HTTP codes that indicate AniList or Cloudflare is down/throttling
UPSTREAM_OUTAGE_CODES = {403, 408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

def fetch_page(page: int) -> dict:
    headers = {
        "User-Agent": "anilist-backup/1.0 (https://github.com/thaware12/anilist-backup)",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        response = requests.post(
            API_URL,
            json={"query": QUERY, "variables": {"page": page, "perPage": 50}},
            headers=headers,
            timeout=20,
        )
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_err:
        print(f"[WARN] Network/Cloudflare unreachable: {net_err}. Keeping existing backup.")
        sys.exit(0)  # Exit cleanly: no GitHub failure email

    if response.status_code in UPSTREAM_OUTAGE_CODES:
        print(f"[WARN] Upstream HTTP {response.status_code} (AniList/Cloudflare issue). Skipping run.")
        sys.exit(0)  # Exit cleanly: no GitHub failure email

    # Any other unexpected HTTP code (e.g., 400 Bad Request from broken GraphQL query)
    # will raise an exception and exit(1) so you get emailed!
    response.raise_for_status()

    # Check if Cloudflare/proxy returned HTML instead of JSON on a 200 OK
    try:
        payload = response.json()
    except ValueError:
        print("[WARN] Non-JSON response received from upstream proxy. Skipping run.")
        sys.exit(0)

    # Check for GraphQL-level errors
    if "errors" in payload:
        errors = payload["errors"]
        # AniList sometimes returns status 403/500 inside a 200 JSON body during outages
        for err in errors:
            if err.get("status") in UPSTREAM_OUTAGE_CODES or "temporarily disabled" in err.get("message", ""):
                print(f"[WARN] AniList API disabled/degraded: {err}. Skipping run.")
                sys.exit(0)
        # Real GraphQL syntax or schema error -> Fail loudly and send email!
        raise RuntimeError(f"GraphQL Schema/Query Error: {errors}")

    return payload["data"]["Page"]

def main():
    state = {"last_updated_at": 0}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))

    last_seen = state["last_updated_at"]
    newest_timestamp = last_seen
    page = 1

    while True:
        page_data = fetch_page(page)
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
            # Save item to sharded bucket (e.g., 1,000 IDs per file)
            shard_id = item["id"] // 1000
            shard_path = Path(f"data/{item['type'].lower()}/shard_{shard_id:04d}.json")
            shard_path.parent.mkdir(parents=True, exist_ok=True)
            
            shard_data = {}
            if shard_path.exists():
                shard_data = json.loads(shard_path.read_text(encoding="utf-8"))
            shard_data[str(item["id"])] = item
            shard_path.write_text(json.dumps(shard_data, indent=2, sort_keys=True), encoding="utf-8")

        if stop_pagination or not page_data["pageInfo"]["hasNextPage"]:
            break

        page += 1
        time.sleep(2.1)  # Stay safely under the 30 req/min degraded rate limit

    state["last_updated_at"] = newest_timestamp
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(f"[OK] Sync complete. New watermark: {newest_timestamp}")

if __name__ == "__main__":
    main()
