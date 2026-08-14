# 🎬 Video Analyzer | HDR Detection

Video analyzer and library for HDR content with deep metadata extraction, flexible filtering, and data cleanup tooling.

See `CHANGELOG.md` for release notes and notable changes.

---

## 🚀 Key Features

- **HDR Detection:** Dolby Vision profiles, EL types (FEL/MEL), HDR10+, HDR10, HLG detection.
- **ARR Integration:** Right-click to queue Sonarr/Radarr search commands; separate status dots for each service with individual tooltips.
- **Metadata Enrichment:** Filename parsing, Kodi `.nfo` ingestion, and backfill tools.
- **Smart Filtering:** Multi‑select filters with counts, `All` + `Blanks` options, and advanced search tokens.
- **Media-type–aware ribbons:** When filtering on Movies or TV, ribbons and charts show totals for that type only; ribbon and badge clicks preserve the media type.
- **Charts:** Real‑time visualizations with **Totals / Filtered** toggle.
- **Manual Edits:** Edit titles, year, source, HDR info, and media type directly in the file modal.
- **Bulk Edit/Rescan:** Multi‑select rows and apply edits or rescan selected files.
- **Column Control:** Show/hide, resize, and drag‑reorder columns; **one wide table** in `#table-h-scroll`. **Freeze-pane headers:** thead stays pinned at the top of the table scrollport while rows scroll (Excel-like); checkbox/delete columns stay frozen left/right. The scrollport uses both axes (`overflow: auto`) and is sized to the remaining viewport height. The grid uses **`border-collapse: separate`** so sticky headers stay aligned with column widths. **Column widths are global:** `<colgroup>` and layout use **only** persisted `column_widths` (and per‑column **minimum defaults** when unset). Filtering and scrolling **do not** read `th.offsetWidth` or otherwise remeasure — widths change **only** when you drag a resize handle (or use reset‑to‑minimum). **Movies / TV** use **`display: none`** on irrelevant title columns. Saved **`column_widths` are unchanged**; **`<col>` widths are `0`** for collapsed columns (media or menu‑hidden) so the table grid matches what you see. After every colgroup sync, **inline `width` / `min-width` / `max-width` are stripped from all `th`/`td`** so only **`<colgroup>`** defines column widths. Resize uses **delegated capture** on `#video-table` (12px hit zone); **mouseup** snapshots all visible column widths from `<colgroup>` into `column_widths`. Drag reorder is blocked only while a resize gesture is active. **col-chk** / **col-del** stay 40px.
- **Scheduling:** Manual, daily, and interval scanning.
- **Exports:** CSV/JSON of All, Movies, TV, Filtered, or Current page.
- **Notifications:** Toasts for scan actions, backfill, settings saves, and more.
- **Scan Controls:** Split scan button with All/TV/Movie and per-folder targeting.
- **Missing File Tracking:** `missing` column and filter for files no longer on disk; optional "Remove Missing from DB" setting (delete vs mark).
- **Scan Report:** Shows scanned, new, removed, failures, and whether missing files were deleted or marked.
- **Duplicate Detection Suite:** Persistent duplicate keys/counts, duplicate groups modal, exact fingerprint rebuild option, and `Dup` badges in the main table.
- **Quality anomalies:** Dedicated `quality_anomaly` column and Yes/No filter for low-bitrate HD/UHD, legacy 4K codecs, and unusual frame rates.
- **Optional login:** `BASIC_AUTH` session login, Logout next to the version badge, and CSRF on mutating requests.
- **Scan resume:** Interrupted jobs keep pending paths; use **Resume** (scans do not auto-start after reboot).
- **Skip unchanged files:** Incremental scans still walk the library, then skip analysis when size and file mtime match the database.

---

## 🛠 Tech Stack

- **Frontend:** HTML5, CSS3, classic JavaScript (`app/static/js/*.js` + `app/static/css/app.css`; Chart.js)
- **Backend:** Python / Flask — package `app/video_analyzer/` with thin WSGI entry `app/analyzer.py` (`analyzer:app`)
- **Database:** SQLite

Layout (3.1+): domain modules under `video_analyzer/` own implementations (`config`, `state`, `db`, `queries`, `scan/pipeline`, `routes/*.py`). `core.py` is a compatibility barrel for older imports. Gunicorn still loads `analyzer:app`. SQLite schema changes go through `schema_migrations` (applied on startup).

---

## 📦 Installation

1. Copy env file and edit values:
   ```bash
   cp .env.example .env
   ```
2. Map media volumes in `docker-compose.yml` (and set `SCAN_PATHS` in `.env` to those container paths if you want fixed roots).
3. Start container:
   ```bash
   docker compose build --no-cache
   docker compose up -d
   ```
   Confirm the UI version badge matches the latest entry in `CHANGELOG.md` / `app/CHANGELOG.md` (e.g. **v3.1.6**). Sync the full repo to the NAS before building (not just `docker-compose.yml`). Prefer leaving `APP_VERSION` unset in `.env` so the changelog drives the badge.
4. Open: `http://localhost:6002` (or host IP)

The compose service uses `restart: unless-stopped` and a health check against `/api/health`. Python deps are pinned in `app/requirements.txt`. Images build for **amd64** and **arm64** (`dovi_tool` musl binaries).

### Running tests

```bash
cd app
pip install -r requirements.txt -r requirements-dev.txt
set VIDEO_ANALYZER_TESTING=1   # PowerShell: $env:VIDEO_ANALYZER_TESTING=1
pytest -q
```

CI runs the same suite via `.github/workflows/ci.yml` on push/PR.

### Authentication and CSRF

Optional login is controlled by environment variables (see `.env.example` and `docker-compose.yml`):

- `BASIC_AUTH=username:password` — enables the login page and a **Logout** control next to the version badge. Leave empty to keep the UI open on the LAN.
- `SECRET_KEY` — Flask session secret. When unset, the app writes a durable key to `/output/.flask_secret` (not derived from the password).

Session cookies are `HttpOnly` with `SameSite=Lax`. `Secure` is not forced, because LAN HTTP is normal.

Mutating requests (`POST` / `PUT` / `PATCH` / `DELETE`) require a CSRF token even when auth is off, so a drive-by page cannot trigger delete/restore/scan. The UI reads `<meta name="csrf-token">` (and `window.CSRF_TOKEN`) and sends `X-CSRF-Token`. Responses include the current token in the `X-CSRF-Token` header; the UI copies that into the meta tag and retries once if a POST gets `Invalid CSRF token`. The login form posts `csrf_token`. `GET` and `/api/health` are exempt.

### Deployment Scope

This app is designed for trusted LAN use. Enable `BASIC_AUTH` if the UI is reachable beyond your household, and do not publish it directly to the internet. If remote access is needed, place it behind a VPN or a secured reverse proxy.

### ARR Integration Environment Variables

To enable right-click ARR search/replace actions, set these environment variables in your compose service:

- `RADARR_URL` (example: `http://192.168.5.10:7878`)
- `RADARR_API_KEY`
- `SONARR_URL` (example: `http://192.168.5.10:8989`)
- `SONARR_API_KEY`

Notes:
- API keys are read from environment variables only (not stored in app settings DB).
- For TV rows, Sonarr lookup uses `tvdb_series_id` only (from `tvshow.nfo`) and `season`; triggers **SeasonSearch** for the season.
- For movie rows, Radarr lookup uses `tmdb_id` or `imdb_id`; triggers **MoviesSearch**.
- Two status dots (Sonarr and Radarr) next to the ARR menu option show connectivity per service; each dot has its own tooltip (checked when you open the context menu).

---

## 🖥 UI Guide

### Main Menu / Settings Panel
The top‑left menu contains:
- **Export format** toggle (CSV/JSON)
- **Database tools**: Backup, Restore, Optimize, Backfill Metadata, Clean DB
- **Health**: System status, database, scan status, uptime, app/tool versions, latency, and **Sonarr/Radarr connection status**
- **Scan folders**: Pick specific folders per volume, assign type, and mute
- **Filter presets** (save/load/delete)
- **Remove Missing from DB**: When enabled (default), files no longer on disk are deleted from the DB during scan. When disabled, they are marked `missing=1` so you can filter and delete them manually.
- **Duplicate Check While Scanning**: When enabled, scan batches also compute duplicate grouping keys/counts during scan saves. Keep this off for faster scans if you only run duplicate checks on demand.

### Health Modal
The Health button opens a modal showing:
- **Status** (healthy/degraded), **Database**, **Scan Status**, **Uptime**, **Version**, **Latency**
- Tool versions for **MediaInfo**, **FFmpeg**, **FFprobe**, **dovi_tool**, and **Python**
- **Sonarr** and **Radarr** connection status: green = connected (with version), yellow = not configured, red = connection failed (error message shown)

### Badges (Quick Filters)
| Badge | Meaning |
|-------|---------|
| DV P7 FEL | Dolby Vision P7 + Full EL |
| DV P7 MEL | Dolby Vision P7 + Minimal EL |
| DV P5 / P8.x / P10.x | Dolby Vision profiles |
| HDR10+ / HDR10 / HLG / SDR | Base HDR format |

Clicking badges applies a filter immediately. When you have **Movies** or **TV** selected, badge clicks keep that media type and add the chosen filter on top.

### Filters
- Multi‑select filters include **All** + **Blanks** and show counts.
- **Media Type** filter (Movie/TV).
- **Missing** filter (Yes/No) for files no longer on disk.
- **Anomaly** filter (Yes/No) for quality flags (low bitrate, legacy 4K codec, unusual FPS).
- Resolution, volumes, codecs, formats, source, container, edition, etc.

### Advanced Search
Supports tokens and quoted values:
- `source:"UHD Bluray"`
- `res:2160p`
- `year:2020`
- `type:tv`
- `category:dovi`
- `status:failed`

Tokens supported: `year`, `source`, `format`, `codec`, `res`, `category`, `volume`, `container`, `edition`, `type`, `media_type`, `status`, `hybrid`, `3d`, `missing`.

### Charts
Charts can toggle between:
- **Totals** (entire library)
- **Filtered** (current filters)

The toggle is in the **lower‑left** of the chart panel.

### Column Management
- Show/hide in column menu
- Resize by dragging column edge
- Reorder by dragging column headers
- Order and widths are persisted
- Freeze-pane header: sort/filter headers stay visible while scrolling rows inside the table viewport
- Sticky left/right columns keep checkbox and delete visible (including in the frozen header)
- `Dup` column shows duplicate count badge (`xN`) and can open duplicate-group details directly

### Details Modal (Manual Edits)
Editable fields saved on modal close:
- Type, Show Title, Episode Title, Movie Title
- Season, Episode, Year
- Source, Source Format
- Main HDR (category), Secondary HDR

### Bulk Edit / Rescan
- Use **Bulk Edit/Rescan** button next to Search (shown when rows are selected).
- Ctrl+Click (Windows) or Cmd+Click (macOS) to multi‑select rows.
- Apply edits across selected rows or rescan selected files.
- Per‑field **Clear** toggles allow blanking specific fields.
- Bulk rescan shows a busy overlay while rescanning.

### Row Right‑Click Menu (ARR)
- Right-click one or more selected rows and choose **Search/Replace: Sonarr | Radarr**.
- **Two status dots** (Sonarr | Radarr) show connectivity per service; each has its own tooltip. Green = reachable, red = connection failed.
- The app queues search commands in Radarr (movies) or Sonarr (TV) for matching library items.
- Mixed selection is supported; each row routes to Sonarr or Radarr by `media_type` (with ID fallback).
- **Fallbacks:** If direct ID lookup returns empty (e.g. NFO has wrong tvdbId or Sonarr lookup is down), the app fetches all series/movies and matches by `tvdb_series_id`/`tmdb_id`/`imdb_id`, or by show/movie title when IDs don't match.
- **Series vs Episode IDs:** TV rows store `tvdb_series_id` (from `tvshow.nfo`, e.g. 73940 for 'Allo 'Allo) and `tvdb_episode_id` (from episode NFO, e.g. 133064). Same for imdb, tmdb, trakt, rotten, metacritic. Movies continue to use `tvdb_id`, `imdb_id`, etc. After adding these columns, a **full rescan** is required to populate them from NFOs.

### Scan Folders
- **Folders** button in the main menu opens a folder picker.
- Choose a volume, browse directories, and add folders to the scan list.
- **Type** selector per folder (Auto/TV/Movie) for targeted scans.
- **Mute** keeps a folder in the list but skips it during scans.
- Any folder containing an empty `.scanignore` file is skipped (including all subfolders).
- If no scan folders are configured, scans default to all mounted volumes.

Example:
```
/media/Movies/.scanignore
```
Any folder with `.scanignore` is skipped.

### Scan Ignore Rules
The **IGNORE** setting is a comma-separated, case-insensitive list. Rules apply while crawling a scan:

| Rule | Matches | Example |
|------|---------|---------|
| `name` | Files whose filename contains `name` | `sample,trailer,.extras` |
| `*.pattern` | Files matching a filename glob | `*.trickplay,*.nfo` |
| `/name` | Folders whose name is exactly `name` | `/extras,/.extras` |
| `/*.pattern` | Folders matching a folder-name glob | `/*.trickplay,/.cache*` |
| `%name` | Files and folders containing `name` | `%sample,%extras` |
| `%*.pattern` | Files and folders matching a glob | `%*.trickplay` |

Examples:

```text
sample,trailer,.extras,*.trickplay,/.extras,/*.trickplay,%cache
```

This skips sample/trailer files, filenames containing `.extras`, files ending in
`.trickplay`, the exact `.extras` folder, folders ending in `.trickplay`, and
anything containing `cache`. A bare token such as `.extras` applies to files
only; prefix it with `/` to target a folder. Use `%` only when both files and
folders should match.

### Scan Button
- Split scan button: **All**, **TV**, **Movie**.
- Hover TV/Movie to pick a specific typed folder from a submenu.
- The main button shows the selected mode and folder target.
- **Skip unchanged files** walks every video, then skips analysis when the DB row exists, `file_size` matches, there is no `scan_error`, and file mtime is not newer than `last_scanned`. Force-rescan still analyzes all. Missing-file cleanup still runs. The API keeps `scan_scope=changed` for compatibility.
- If the latest job was interrupted or aborted with pending files, **Resume** continues those paths without a full recrawl. Scans do not auto-start after reboot.
- During scan: "Starting" shows for up to 3 seconds per volume, then "Found X (Y new / Z removed)".
- Click the progress bar during a scan to pause/resume scanning and analyzing.
- History button appears when idle and shows recent scan history with a per-entry report view (scanned, new, removed, failures).

### Export Button
- Split export button with scopes: **All**, **All Movies**, **All TV**, **All filtered**, **Current page**.
- Output format is set in the main menu (CSV/JSON).

---

## 🔧 Metadata Enrichment

### Kodi `.nfo` Support (Primary Source)
- Episode `.nfo` (`episodedetails`) → episode title, season, episode, year
- `tvshow.nfo` → show title
- `movie.nfo` → movie title + year

### NFO Column (Table)
- **NFO** indicates whether a matching `.nfo` file was found for that row.
- **Movies:** Counted only if an `.nfo` exists in the same folder as the video (same stem, `movie.nfo`, or folder-named `.nfo`). A `movie.nfo` in a parent folder does not count.
- **TV episodes:** Counted only if an episode-specific `.nfo` exists (same stem as the video, or same-folder `.nfo` with matching season/episode). `tvshow.nfo` is series-level and does not count as “NFO found” for individual episodes.

### Missing Column (Table)
- **Missing** indicates whether the file no longer exists on disk (Yes/No).
- When "Remove Missing from DB" is disabled, scans mark missing files instead of deleting them; use the Missing filter to find and delete them manually.

### Anomaly Column (Table)
- **Anomaly** is Yes when the file matched a quality heuristic (stored in `quality_anomaly`, separate from media-type `validation_flag`).
- Use the Anomaly Yes/No filter or the ribbon card; `/api/anomalies` lists matching rows.
- Click **Yes** to open a modal that explains each flag (low 4K bitrate, legacy 4K codec, low 1080p bitrate, unusual frame rate) with the file’s measured resolution, bitrate, codec, and fps.

### Filename Heuristics (Fallback Only)
- Movie title fallback (only if `.nfo` missing)
- Episode title fallback after `SxxEyy` or `1x02`
- Remux source inference:
  - `1080p` + remux → Bluray
  - `2160p` + remux → UHD Bluray

### Backfill Tool
- **Backfill Metadata** button in settings panel
- Fills missing fields using `.nfo` + filename
- Progress shown in scan info panel + logs

---

## 🔍 HDR Detection Examples

- `DOVI P7 FEL` → `category=dovi`, `profile=7`, `el=FEL`
- `DOVI P7 MEL` → `category=dovi`, `profile=7`, `el=MEL`
- `DOVI P10.1` / `DOVI P10.4` → AV1 Dolby Vision with HDR10/HLG backward compatibility
- `HDR10+` → `category=hdr10plus`
- `HDR10` → `category=hdr10`
- `HLG` → `category=hlg`
- `SDR` → `category=sdr_only`

Secondary HDR is detected from HDR side‑data and stored in `secondary_hdr`.

---

## 📊 API Reference (Highlights)

### GET `/api/health` / `/health`
Returns system health: `status`, `database`, `scan_status`, `uptime_seconds`, `version`, `tools`, `sonarr`, `radarr`. Each tool object includes `installed`, `version`, and `message`; each ARR object has `ok`, `configured`, `message`.

### GET `/api/videos`
Paginated table rows with filters and sorting. **Fast path:** returns `rows`, `page`, `total_items`, `total_pages`, and `library_total` only.

Key params:
- `search`, `category`, `profile`, `el`, `resolution`, `volume`, `container`
- `video_source`, `source_format`, `video_codec`, `media_type`
- `secondary_hdr`, `status`, `nfo_missing`, `missing` (1=yes, 0=no), `anomaly` / `quality_anomaly` (1=yes, 0=no)
- `size_op`, `size_val`, `bit_op`, `bit_val`
- `sort`, `order`, `page`, `per_page`

Includes duplicate fields in each row payload:
- `dup_group_key`
- `dup_exact_key`
- `dup_count`

### GET `/api/videos/meta`
Heavy dashboard metadata for the same filter params: `stats`, `stats_filtered`, `stats_media_scoped`, and filtered `total_items`.
Optional `include_options=0|1` (default `1`): when `1`, also returns `filter_options` facet counts for dropdowns. The UI loads stats with `include_options=0` first, then refreshes facet counts on a short delay.

### GET `/api/anomalies`
Returns rows whose `quality_anomaly` column is set.

### POST `/start`
Start a library scan. CSRF required. Body includes `threads`, `scan_mode` (`all`/`tv`/`movie`), `scan_scope` (`all` or `changed` = skip unchanged files), optional folder targets, and optional `resume_job_id` to continue pending paths without recrawling.

### POST `/api/duplicates/rebuild`
Rebuild persistent duplicate keys/counts for current filters.

Payload:
```json
{"filters": {}, "include_exact": false}
```

- `include_exact=false`: rebuild logical group keys/counts only
- `include_exact=true`: also recompute exact fingerprints

### POST `/api/duplicates/groups`
Return duplicate groups (logical + exact) for current filters.

Payload:
```json
{"filters": {}}
```

### POST `/api/duplicates/members`
Return all files in a duplicate group.

Payload:
```json
{"group_id": "logical|movie:tmdb:12345"}
```

### POST `/api/backfill_metadata`
Backfill missing metadata using `.nfo` + filename.

Payload:
```json
{"fill_blanks_only": true}
```

### POST `/api/update_metadata`
Update manual fields:
```json
{
  "full_path": "/path/file.mkv",
  "show_title": "Show Name",
  "episode_title": "Episode Title",
  "movie_title": "Movie Title",
  "season": 1,
  "episode": 4,
  "year": 2020,
  "video_source": "Bluray",
  "source_format": "Remux",
    "category": "dovi",
  "secondary_hdr": "HDR10+"
}
```

### POST `/api/update_media_type`
```json
{"full_path": "/path/file.mkv", "media_type": "tv"}
```

### POST `/api/rescan_file`
Rescan a single file and update DB entry:
```json
{"full_path": "/path/file.mkv"}
```

### POST `/api/rescan_files`
Batch rescan (max 50 paths per request; UI chunks larger selections):
```json
{"paths": ["/path/a.mkv", "/path/b.mkv"], "threads": 2}
```
Returns `ok` / `failed` counts and optional per-path `errors`. Paths must lie under allowed media mounts. Blocked while a scan/backfill is running.

### POST `/api/delete`
Remove DB rows (and optionally disk files/folders). Disk/folder delete requires an explicit `paths` list (not `delete_all_filter`).
```json
{
  "paths": ["/media/Movie/Movie.mkv"],
  "delete_files_on_disk": true,
  "delete_folders": true
}
```
Folder delete is skipped for media mount roots and folders that still contain other video files.

### POST `/api/delete/preview`
Dry-run folder impact for an explicit path list (no mutations).

### POST `/api/arr_search_replace`
Queue Sonarr/Radarr search commands for selected files:
```json
{"paths": ["/path/file1.mkv", "/path/file2.mkv"]}
```

---

## 🐛 Troubleshooting

### Backfill or scan not updating
- Check `/progress` and `/api/logs`
- Ensure no scan is running when backfill starts

### Filters not behaving
- Clear filters and re‑apply
- Verify search token syntax and quoting
