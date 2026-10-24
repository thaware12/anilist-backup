#!/usr/bin/env python3
"""AniList -> GitHub mirror (https://github.com/thaware12/anilist-backup).

Built by AI in collaboration with @thaware12. MIT licensed.

Phases (all state lives in state.json):
  1. Seed     - walk every ID 1..max in windows of 500 and store every entry.
  2. Delta    - newest-first walk of `UPDATED_AT_DESC` down to the watermark.
  3. Hot list - every 30 min re-read the entries people actually look at (trending,
                currently airing/releasing, all-time most popular) so their scores and
                popularity stay fresh. Scores change without `updatedAt` changing.
  4. Refresh  - rolling re-sweep of the whole ID range, so drifting stats everywhere
                else and anything the delta missed get healed.

Exit codes:
  0   success
  75  AniList / Cloudflare outage -> progress saved, run skipped (workflow stays green)
  1   real bug (or an outage lasting longer than OUTAGE_ESCALATE_HOURS) -> fail loudly
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
API_URL = "https://graphql.anilist.co"
USER_AGENT = "anilist-backup/2.0 (+https://github.com/thaware12/anilist-backup)"

DATA_DIR = Path("data")
STATE_FILE = Path("state.json")
STATUS_FILE = Path("status.json")  # shields.io "endpoint" badge payload

# Bump when the stored entry format changes: triggers an automatic re-seed
# (old data stays in place as a fallback until it is overwritten).
SCHEMA_VERSION = 2

MAX_REQUESTS_PER_RUN = 75      # request budget for one `python sync.py` while seeding
ID_WINDOW_SIZE = 500           # IDs per window (well below AniList's 5,000-result depth cap)
DEFAULT_PER_PAGE = 50
MIN_PER_PAGE = 5
REQUEST_INTERVAL = 2.1         # seconds between requests (~28/min, under the 30/min limit)
MAX_ATTEMPTS = 3               # per request, for transient errors
MAX_DELTA_PAGES = 100          # 100 x 50 = 5,000 = AniList's pagination depth cap
DELTA_OVERLAP_SECONDS = 120    # re-read this much history to survive clock/ordering jitter
REFRESH_REQUESTS_PER_RUN = 45  # rolling re-sweep budget per pass once seeded (0 disables)
HOT_INTERVAL_SECONDS = 1800    # how often the "hot list" is re-read (0 disables)
# (sort, extra filters, how many top entries)  ~20 requests per hot run
HOT_LISTS = (
    ("TRENDING_DESC", "", 200),                                     # trending right now
    ("POPULARITY_DESC", "type: ANIME, status: RELEASING", 400),     # currently airing anime
    ("POPULARITY_DESC", "type: MANGA, status: RELEASING", 200),     # currently releasing manga
    ("POPULARITY_DESC", "", 200),                                   # all-time most popular
)
HEARTBEAT_SECONDS = 3600       # when nothing changed, touch state/status at most hourly

OUTAGE_ESCALATE_HOURS = float(os.environ.get("OUTAGE_ESCALATE_HOURS", "6"))  # 0 = never
EXIT_OUTAGE = 75

INCLUDE_DESCRIPTION = True     # False -> noticeably smaller repo (changing it needs a re-seed)

OUTAGE_CODES = {403, 408, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 530}

_DESCRIPTION = "description" if INCLUDE_DESCRIPTION else ""
MEDIA_FIELDS = f"""
      id idMal type format status updatedAt
      title {{ romaji english native }}
      synonyms
      {_DESCRIPTION}
      startDate {{ year month day }}
      endDate {{ year month day }}
      season seasonYear
      episodes duration chapters volumes
      source countryOfOrigin isAdult isLicensed hashtag
      coverImage {{ extraLarge large medium color }}
      bannerImage
      trailer {{ id site }}
      genres
      tags {{ id name rank isMediaSpoiler }}
      studios(isMain: true) {{ nodes {{ id name }} }}
      averageScore meanScore popularity favourites
"""

WATERMARK_QUERY = """
query {
  newestId: Page(page: 1, perPage: 1) { media(sort: ID_DESC) { id } }
  newestUpdate: Page(page: 1, perPage: 1) { media(sort: UPDATED_AT_DESC) { updatedAt } }
}
"""

SEED_QUERY = f"""
query ($page: Int, $perPage: Int, $ids: [Int]) {{
  Page(page: $page, perPage: $perPage) {{
    pageInfo {{ hasNextPage }}
    media(id_in: $ids, sort: ID) {{ {MEDIA_FIELDS} }}
  }}
}}
"""

DELTA_QUERY = f"""
query ($page: Int, $perPage: Int) {{
  Page(page: $page, perPage: $perPage) {{
    pageInfo {{ hasNextPage }}
    media(sort: UPDATED_AT_DESC) {{ {MEDIA_FIELDS} }}
  }}
}}
"""

def hot_query(sort: str, filters: str) -> str:
    args = f"sort: {sort}" + (f", {filters}" if filters else "")
    return f"""
query ($page: Int, $perPage: Int) {{
  Page(page: $page, perPage: $perPage) {{
    pageInfo {{ hasNextPage }}
    media({args}) {{ {MEDIA_FIELDS} }}
  }}
}}
"""


DEFAULT_STATE = {
    "schema_version": SCHEMA_VERSION,
    "seed_complete": False,
    "seed_last_id": 0,
    "max_target_id": 0,
    "last_updated_at": 0,
    "refresh_last_id": 0,
    "refresh_cycles": 0,
    "hot_last_run": 0,
    "per_page": DEFAULT_PER_PAGE,
    "outage_since": 0,
    "last_success_at": 0,
}


class UpstreamOutage(Exception):
    """AniList or Cloudflare is down / blocking us: skip this run quietly."""


class ComplexityError(Exception):
    """AniList rejected the query as too complex: shrink perPage and retry."""


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def dumps(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def prune(value):
    """Drop nulls / empty lists / empty objects to keep the repo small."""
    if isinstance(value, dict):
        out = {}
        for key, inner in value.items():
            inner = prune(inner)
            if inner is None or inner == [] or inner == {}:
                continue
            out[key] = inner
        return out
    if isinstance(value, list):
        return [prune(x) for x in value]
    return value


def normalize(raw: dict) -> dict:
    item = dict(raw)
    studios = item.get("studios")
    if isinstance(studios, dict):
        item["studios"] = studios.get("nodes") or []
    return prune(item)


def shard_path(item: dict) -> Path:
    media_type = (item.get("type") or "ANIME").lower()
    return DATA_DIR / media_type / f"shard_{item['id'] // 1000:04d}.json"


def write_shard(path: Path, shard: dict) -> None:
    """Valid JSON, one entry per line, numeric key order -> small, readable git diffs."""
    rows = [f"{json.dumps(key)}:{dumps(shard[key])}" for key in sorted(shard, key=int)]
    atomic_write(path, "{\n" + ",\n".join(rows) + "\n}\n")


def load_state() -> dict:
    state = dict(DEFAULT_STATE)
    saved: dict = {}
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as err:  # noqa: BLE001
            print(f"[WARN] Could not parse state.json, starting fresh: {err}")
    state.update(saved)
    if saved and saved.get("schema_version") != SCHEMA_VERSION:
        print(f"[INFO] Schema upgrade -> v{SCHEMA_VERSION}: re-seeding every entry with the new fields "
              "(existing data stays online until overwritten).")
        state.update(seed_complete=False, seed_last_id=0, max_target_id=0, last_updated_at=0,
                     refresh_last_id=0)
    state["schema_version"] = SCHEMA_VERSION
    return state


# --------------------------------------------------------------------------- #
# Sync engine
# --------------------------------------------------------------------------- #
class Syncer:
    def __init__(self) -> None:
        self.state = load_state()
        self.shards: dict[Path, dict] = {}
        self.dirty: set[Path] = set()
        self.requests = 0
        self._last_request = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ----- HTTP ------------------------------------------------------------ #
    def _pace(self) -> None:
        wait = REQUEST_INTERVAL - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    @staticmethod
    def _retry_wait(attempt: int, retry_after: str | None = None) -> float:
        if retry_after:
            try:
                return min(max(float(retry_after), 5.0), 65.0)
            except ValueError:
                pass
        return 5.0 * (3 ** (attempt - 1))  # 5s, 15s, 45s

    def graphql(self, query: str, variables: dict) -> dict:
        problem = "unknown"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._pace()
            retry_after = None
            try:
                resp = self.session.post(API_URL, json={"query": query, "variables": variables},
                                         timeout=30)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                problem = f"network error ({exc.__class__.__name__})"
            else:
                self.requests += 1
                code = resp.status_code
                if code == 429:
                    problem = "HTTP 429 (rate limited)"
                    retry_after = resp.headers.get("Retry-After")
                elif code in OUTAGE_CODES or code >= 500:
                    problem = f"HTTP {code}"
                else:
                    try:
                        payload = resp.json()
                    except ValueError:
                        problem = f"HTTP {code} with a non-JSON body (proxy / Cloudflare page)"
                    else:
                        if payload.get("errors"):
                            self._raise_for_graphql_errors(payload["errors"])
                        if not resp.ok:
                            raise RuntimeError(f"HTTP {code}: {resp.text[:500]}")
                        if payload.get("data") is None:
                            raise RuntimeError(f"Response without data: {str(payload)[:500]}")
                        return payload["data"]
            if attempt < MAX_ATTEMPTS:
                wait = self._retry_wait(attempt, retry_after)
                print(f"[WARN] {problem}; retry {attempt}/{MAX_ATTEMPTS - 1} in {wait:.0f}s")
                time.sleep(wait)
        raise UpstreamOutage(problem)

    @staticmethod
    def _raise_for_graphql_errors(errors: list) -> None:
        for err in errors:
            if "complexity" in str(err.get("message", "")).lower():
                raise ComplexityError(str(err.get("message")))
        for err in errors:
            message = str(err.get("message", "")).lower()
            status = err.get("status")
            if (status in OUTAGE_CODES or status == 429 or "temporarily disabled" in message
                    or "too many requests" in message):
                raise UpstreamOutage(f"AniList API degraded: {err}")
        raise RuntimeError(f"GraphQL error: {errors}")

    def shrink_page_size(self, reason: Exception) -> None:
        current = self.state["per_page"]
        new = max(MIN_PER_PAGE, current // 2)
        if new == current:
            raise RuntimeError(f"Query too complex even at perPage={current}: {reason}")
        print(f"[WARN] Query too complex at perPage={current} ({reason}); using {new} from now on")
        self.state["per_page"] = new

    # ----- storage --------------------------------------------------------- #
    def store(self, raw: dict) -> None:
        item = normalize(raw)
        path = shard_path(item)
        if path not in self.shards:
            self.shards[path] = (json.loads(path.read_text(encoding="utf-8"))
                                 if path.exists() else {})
        shard = self.shards[path]
        key = str(item["id"])
        if shard.get(key) != item:  # unchanged entries never touch git
            shard[key] = item
            self.dirty.add(path)
        if item["id"] > self.state["max_target_id"]:
            self.state["max_target_id"] = item["id"]

    def save(self, success: bool = False) -> None:
        state = self.state
        changed = bool(self.dirty)
        for path in sorted(self.dirty):
            write_shard(path, self.shards[path])
        self.dirty.clear()

        now = int(time.time())
        if success:
            state["outage_since"] = 0
            if changed or now - state.get("last_success_at", 0) >= HEARTBEAT_SECONDS:
                state["last_success_at"] = now
                self.write_status(now)
        atomic_write(STATE_FILE, json.dumps(state, indent=2, sort_keys=True) + "\n")

    def write_status(self, now: int) -> None:
        state = self.state
        if state["seed_complete"]:
            message, color = "synced " + datetime.fromtimestamp(now, timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"), "brightgreen"
        else:
            pct = int(100 * state["seed_last_id"] / state["max_target_id"]) if state["max_target_id"] else 0
            message, color = f"seeding {pct}%", "yellow"
        atomic_write(STATUS_FILE, json.dumps(
            {"schemaVersion": 1, "label": "mirror", "message": message, "color": color}) + "\n")

    # ----- ID-window fetch (used by seed + refresh) ------------------------- #
    def sync_window(self, start_id: int, end_id: int, cap: int) -> int:
        """Fetch every entry with start_id <= id <= end_id.

        Returns the highest ID that is fully covered (end_id when finished,
        less if the request budget `cap` ran out; start_id - 1 = no progress).
        Results are ID-sorted, so everything <= the last ID seen is complete.
        """
        ids = list(range(start_id, end_id + 1))
        page, covered = 1, start_id - 1
        while self.requests < cap:
            try:
                data = self.graphql(SEED_QUERY, {"page": page, "perPage": self.state["per_page"],
                                                 "ids": ids})
            except ComplexityError as exc:
                self.shrink_page_size(exc)
                page, covered = 1, start_id - 1  # page numbering changed -> restart window
                continue
            page_data = data["Page"]
            for item in page_data["media"]:
                self.store(item)
                covered = max(covered, item["id"])
            if not page_data["pageInfo"]["hasNextPage"]:
                return end_id
            page += 1
        return covered

    # ----- phase 1: seed ---------------------------------------------------- #
    def init_watermarks(self) -> None:
        data = self.graphql(WATERMARK_QUERY, {})
        if data["newestId"]["media"]:
            self.state["max_target_id"] = data["newestId"]["media"][0]["id"]
        if data["newestUpdate"]["media"]:
            self.state["last_updated_at"] = data["newestUpdate"]["media"][0]["updatedAt"]
        print(f"[INFO] Target max ID: {self.state['max_target_id']}, "
              f"watermark: {self.state['last_updated_at']}")

    def run_seed(self) -> None:
        state = self.state
        cap = self.requests + MAX_REQUESTS_PER_RUN
        if state["max_target_id"] == 0 or state["last_updated_at"] == 0:
            self.init_watermarks()
        while self.requests < cap:
            start = state["seed_last_id"] + 1
            if start > state["max_target_id"]:
                state["seed_complete"] = True
                state["refresh_last_id"] = 0
                print("[OK] Initial full backup is 100% complete!")
                return
            end = min(start + ID_WINDOW_SIZE - 1, state["max_target_id"])
            done = self.sync_window(start, end, cap)
            if done <= state["seed_last_id"]:
                break
            state["seed_last_id"] = done
            pct = 100 * done / max(state["max_target_id"], 1)
            print(f"[SEED] {start}..{done} of {state['max_target_id']} ({pct:.1f}%), "
                  f"{self.requests} requests used")

    # ----- phase 2: delta --------------------------------------------------- #
    def run_delta(self) -> None:
        state = self.state
        last_seen = state["last_updated_at"]
        cutoff = last_seen - DELTA_OVERLAP_SECONDS
        newest = last_seen
        reached_cutoff = False
        pages = 0
        page = 1
        while pages < MAX_DELTA_PAGES and not reached_cutoff:
            try:
                data = self.graphql(DELTA_QUERY, {"page": page, "perPage": state["per_page"]})
            except ComplexityError as exc:
                self.shrink_page_size(exc)
                page = 1
                continue
            pages += 1
            page_data = data["Page"]
            media = page_data["media"]
            if page == 1 and media:
                newest = max(newest, media[0]["updatedAt"])
            for item in media:
                if item["updatedAt"] < cutoff:
                    reached_cutoff = True
                    break
                self.store(item)
            if not media or not page_data["pageInfo"]["hasNextPage"]:
                reached_cutoff = True
            page += 1

        if not reached_cutoff:
            # More than ~5,000 entries changed since the last run (long outage / downtime).
            # The delta cannot page any deeper, so the rolling refresh sweep heals the gap.
            print(f"[WARN] Delta hit the {MAX_DELTA_PAGES}-page depth cap before reaching the "
                  "watermark (a bulk update on AniList?); the rolling refresh sweep fills the gap.")
        state["last_updated_at"] = newest
        print(f"[DELTA] {pages} page(s) read, new watermark {newest}")

    # ----- phase 3: hot list (fresh scores for what people look at) ---------- #
    def run_hot(self) -> None:
        if HOT_INTERVAL_SECONDS <= 0:
            return
        state = self.state
        now = int(time.time())
        if now - state.get("hot_last_run", 0) < HOT_INTERVAL_SECONDS:
            return
        fetched = 0
        for sort, filters, top_n in HOT_LISTS:
            query = hot_query(sort, filters)
            page = 1
            while True:
                per_page = state["per_page"]
                if page > -(-top_n // per_page):  # ceil division: pages needed for top_n
                    break
                try:
                    data = self.graphql(query, {"page": page, "perPage": per_page})
                except ComplexityError as exc:
                    self.shrink_page_size(exc)
                    page = 1
                    continue
                for item in data["Page"]["media"]:
                    self.store(item)
                    fetched += 1
                if not data["Page"]["pageInfo"]["hasNextPage"]:
                    break
                page += 1
        state["hot_last_run"] = now
        print(f"[HOT] Refreshed {fetched} trending / airing / popular entries")

    # ----- phase 4: rolling refresh ----------------------------------------- #
    def run_refresh(self) -> None:
        if REFRESH_REQUESTS_PER_RUN <= 0:
            return
        state = self.state
        cap = self.requests + REFRESH_REQUESTS_PER_RUN
        while self.requests < cap:
            start = state["refresh_last_id"] + 1
            if start > state["max_target_id"]:
                state["refresh_last_id"] = 0
                state["refresh_cycles"] += 1
                print(f"[REFRESH] Full sweep #{state['refresh_cycles']} finished, starting over")
                return
            end = min(start + ID_WINDOW_SIZE - 1, state["max_target_id"])
            done = self.sync_window(start, end, cap)
            if done <= state["refresh_last_id"]:
                break
            state["refresh_last_id"] = done
        print(f"[REFRESH] Sweep cursor at ID {state['refresh_last_id']}")

    # ----- orchestration ----------------------------------------------------- #
    def run(self) -> None:
        if not self.state["seed_complete"]:
            print(f"[INFO] Seeding, starting after ID {self.state['seed_last_id']} "
                  f"(perPage={self.state['per_page']})")
            self.run_seed()
        else:
            print(f"[INFO] Incremental update since {self.state['last_updated_at']}")
            self.run_delta()
            self.run_hot()
            self.run_refresh()

    def on_outage(self, exc: Exception) -> int:
        state = self.state
        now = int(time.time())
        if not state.get("outage_since"):
            state["outage_since"] = now
        hours = (now - state["outage_since"]) / 3600
        print(f"[WARN] AniList/Cloudflare unavailable: {exc}. Outage for {hours:.1f}h. "
              "Progress saved; skipping this run.")
        escalate = OUTAGE_ESCALATE_HOURS > 0 and hours >= OUTAGE_ESCALATE_HOURS
        if escalate:
            state["outage_since"] = now  # re-arm: at most one loud failure per window
        self.save(success=False)
        if escalate:
            print(f"[ERROR] Outage lasted >= {OUTAGE_ESCALATE_HOURS}h - this is probably not a "
                  "transient AniList problem (blocked? changed API?). Failing loudly.")
            return 1
        return EXIT_OUTAGE


def main() -> int:
    syncer = Syncer()
    try:
        syncer.run()
    except UpstreamOutage as exc:
        return syncer.on_outage(exc)
    except BaseException:
        try:
            syncer.save()  # keep whatever was fetched before the bug hit
        except Exception as save_err:  # noqa: BLE001
            print(f"[WARN] Could not save progress after error: {save_err}")
        raise
    syncer.save(success=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
