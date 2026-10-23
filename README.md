# 🗄️ AniList Automated Backup (`anilist-backup`)

![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Built by AI](https://img.shields.io/badge/Built%20With-AI-blueviolet.svg)
![Sync Interval](https://img.shields.io/badge/Sync-Every%205%20Minutes-blue.svg)
[![Mirror status](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fthaware12%2Fanilist-backup%2Fmain%2Fstatus.json)](https://github.com/thaware12/anilist-backup/actions)

An automated, self-updating, public JSON mirror of the **AniList Anime & Manga catalog**, powered by GitHub Actions.

It is a fallback dataset for developers, websites, Discord bots and apps, so they can keep reading Anime and Manga metadata when the official AniList API (`graphql.anilist.co`) or Cloudflare is down.

> 🤖 **AI-generated project:** this repository (the sharded data layout, the sync script `sync.py`, the fault-tolerant GitHub Actions pipeline and this documentation) was **created by AI** in collaboration with [@thaware12](https://github.com/thaware12).

> 🔓 **Free & open (MIT License):** free for everyone to use, modify and redistribute, personal or commercial.

> ⚠️ Not affiliated with AniList. All metadata originates from the [AniList GraphQL API](https://github.com/AniList/ApiV2-GraphQL-Docs). Please respect AniList's terms and credit them in your project.

---

## ✨ How it works

1. **Seed:** walks every AniList ID in windows of 500 (`id_in`) at roughly 28 requests/min, under AniList's rate limit, and saves progress to `state.json` after every window. While seeding, `status.json` / the badge above shows `seeding NN%`.
2. **Delta (every ~5 min):** reads `media(sort: UPDATED_AT_DESC)` newest-first down to the saved watermark (with a 2-minute overlap so nothing at the boundary is missed) and rewrites only the entries that changed.
3. **Rolling refresh:** a few requests every cycle slowly re-sweep the whole ID range. This keeps drifting stats (scores, popularity) fresh and heals any gap if the delta ever falls behind (for example after a long outage).
4. **Outage-resilient:**
   - If AniList or Cloudflare is down (timeouts, `403`, `429`, `5xx`, Cloudflare `52x`, non-JSON proxy pages, "temporarily disabled"), the script retries with back-off, saves progress, exits with code `75`, and the workflow **skips the cycle and stays green**: no failure emails, the existing backup is untouched.
   - If an outage lasts longer than **6 hours** it is probably not a normal outage (blocked, API changed), so the run fails loudly, at most once per 6 hours. Set the `OUTAGE_ESCALATE_HOURS` environment variable to change this (`0` = never).
   - Real bugs (schema/query errors, bad JSON, code exceptions) exit `1`, **fail loudly**, and still commit whatever was fetched before the error.

**Scheduling:** a GitHub cron job fires every 5 minutes, but `sync.yml` runs one job as a ~50-minute loop (so seeding is fast and cron jitter doesn't matter). A `concurrency` group guarantees only one job runs at a time; the next cron run queues and takes over when the loop ends.

---

## 📂 Repository layout

```text
anilist-backup/
├── .github/workflows/sync.yml   # cron + continuous sync loop
├── data/
│   ├── anime/shard_0000.json    # anime IDs 0-999
│   ├── anime/shard_0001.json    # anime IDs 1000-1999
│   └── manga/shard_0030.json    # manga IDs 30000-30999 ...
├── state.json                   # progress + watermark (do not edit by hand)
├── status.json                  # drives the status badge
├── sync.py                      # the sync engine
└── README.md
```

Entries are grouped into **shards of 1,000 IDs**: `shard = floor(id / 1000)`, zero-padded to 4 digits.

- Anime ID `1` → `data/anime/shard_0000.json`, key `"1"`
- Anime ID `1535` → `data/anime/shard_0001.json`, key `"1535"`
- Manga ID `30002` → `data/manga/shard_0030.json`, key `"30002"`

Each shard file is valid JSON with **one entry per line**, which keeps git diffs small and readable. AniList anime and manga share one ID space, so an ID lives in exactly one of the two folders.

---

## 📊 What is backed up

Every entry is keyed by its AniList `id`. **Fields that are `null` or empty on AniList are omitted** to keep the repository small, so read optional fields with a default (`entry.get("episodes")`, `entry.title?.english`).

| Field | Description |
| :--- | :--- |
| `id`, `idMal`, `type` | AniList ID, MyAnimeList ID, `ANIME` or `MANGA` |
| `format`, `status` | e.g. `TV`, `MOVIE`, `MANGA`, `NOVEL`; `FINISHED`, `RELEASING`, ... |
| `title` | `{ romaji, english, native }` |
| `synonyms` | Alternative titles |
| `description` | Synopsis (AniList markup/HTML) |
| `startDate`, `endDate` | `{ year, month, day }` (parts can be missing) |
| `season`, `seasonYear` | Anime season |
| `episodes`, `duration` | Anime episode count and minutes per episode |
| `chapters`, `volumes` | Manga counts |
| `source`, `countryOfOrigin`, `isAdult`, `isLicensed`, `hashtag` | Misc metadata |
| `coverImage` | `{ extraLarge, large, medium, color }` image URLs |
| `bannerImage`, `trailer` | Banner URL, `{ id, site }` |
| `genres`, `tags` | Genre names; tags as `{ id, name, rank, isMediaSpoiler }` |
| `studios` | Main studio(s) as `[{ id, name }]` |
| `averageScore`, `meanScore`, `popularity`, `favourites` | Stats (refreshed by the rolling sweep, so they can lag the live site) |
| `updatedAt` | Unix time of the last AniList edit |

Not included: characters, staff, relations, reviews, recommendations and user lists. Deleted or merged entries are **not** removed from the backup.

### Example entry (abridged, illustrative values)

```json
"1": {"id":1,"idMal":1,"type":"ANIME","format":"TV","status":"FINISHED",
      "title":{"romaji":"Cowboy Bebop","english":"Cowboy Bebop","native":"カウボーイビバップ"},
      "episodes":26,"genres":["Action","Adventure","Drama","Sci-Fi"],"updatedAt":1729670000}
```

---

## 🚀 Using the backup

Use the free **jsDelivr CDN** for production traffic. Note that jsDelivr caches branch URLs (`@main`) for a while. For the freshest data use `raw.githubusercontent.com`, or purge the file via `https://purge.jsdelivr.net/gh/thaware12/anilist-backup@main/<path>`.

- Fast, cached: `https://cdn.jsdelivr.net/gh/thaware12/anilist-backup@main/data/anime/shard_0000.json`
- Freshest: `https://raw.githubusercontent.com/thaware12/anilist-backup/main/data/anime/shard_0000.json`

### JavaScript / TypeScript (automatic fallback)

```javascript
async function getMedia(id, type = "anime") {
  const shard = String(Math.floor(id / 1000)).padStart(4, "0");
  const backupUrl =
    `https://cdn.jsdelivr.net/gh/thaware12/anilist-backup@main/data/${type}/shard_${shard}.json`;

  try {
    const res = await fetch("https://graphql.anilist.co", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({
        query: `query ($id: Int) { Media(id: $id) { id idMal type format status
          title { romaji english native } episodes chapters averageScore genres } }`,
        variables: { id },
      }),
      signal: AbortSignal.timeout(5000),
    });
    const payload = await res.json();
    if (!res.ok || payload.errors) throw new Error(`AniList HTTP ${res.status}`);
    return { source: "anilist", data: payload.data.Media };
  } catch (err) {
    console.warn(`AniList unavailable (${err.message}), using backup`);
    const res = await fetch(backupUrl);
    if (!res.ok) throw new Error(`Backup shard ${shard} not found`);
    const shardData = await res.json();
    return { source: "backup", data: shardData[String(id)] ?? null };
  }
}

getMedia(1).then(console.log);
```

### Python

```python
import requests

def get_from_backup(media_id: int, media_type: str = "anime") -> dict | None:
    shard = f"{media_id // 1000:04d}"
    url = ("https://cdn.jsdelivr.net/gh/thaware12/anilist-backup@main/"
           f"data/{media_type}/shard_{shard}.json")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get(str(media_id))

print(get_from_backup(1, "anime"))
```

---

## 🛠️ Run your own mirror

1. Fork the repository and enable **Actions**. (Public repos get free Actions minutes.)
2. Run the **AniList Incremental Backup** workflow once manually. Seeding the full catalog takes a few hours, and the badge shows progress.
3. Optional tuning at the top of `sync.py`: `INCLUDE_DESCRIPTION` (smaller repo if `False`), `REFRESH_REQUESTS_PER_RUN`, `MAX_REQUESTS_PER_RUN`. After changing the stored fields, bump `SCHEMA_VERSION` to trigger a full re-seed.

---

## 📜 License & attribution

- **License:** [MIT](LICENSE).
- **Data source:** [AniList GraphQL API](https://github.com/AniList/ApiV2-GraphQL-Docs).
- **Authorship:** built by AI and maintained by [@thaware12](https://github.com/thaware12).
