# 🎬 Video Analyzer | HDR Detection

Video analyzer and library for HDR content with deep metadata extraction, flexible filtering, and data cleanup tooling.

---

## 🚀 Key Features

- **HDR Detection:** Dolby Vision profiles, EL types (FEL/MEL), HDR10+, HDR10, HLG detection.
- **Metadata Enrichment:** Filename parsing, Kodi `.nfo` ingestion, and backfill tools.
- **Smart Filtering:** Multi‑select filters with counts, `All` + `Blanks` options, and advanced search tokens.
- **Media-type–aware ribbons:** When filtering on Movies or TV, ribbons and charts show totals for that type only; ribbon and badge clicks preserve the media type.
- **Charts:** Real‑time visualizations with **Totals / Filtered** toggle.
- **Manual Edits:** Edit titles, year, source, HDR info, and media type directly in the file modal.
- **Bulk Edit/Rescan:** Multi‑select rows and apply edits or rescan selected files.
- **Column Control:** Show/hide, resize, drag‑reorder columns, and header‑only scrollbar (persisted).
- **Scheduling:** Manual, daily, and interval scanning.
- **Exports:** CSV/JSON of All, Movies, TV, Filtered, or Current page.
- **Notifications:** Toasts for scan actions, backfill, settings saves, and more.
- **Scan Controls:** Split scan button with All/TV/Movie and per-folder targeting.

---

## 🛠 Tech Stack

- **Frontend:** HTML5, CSS3, JavaScript (ES6+)
- **Charts:** Chart.js
- **Backend:** Python / Flask
- **Database:** SQLite

---

## 📦 Installation

1. Map media volumes in `docker-compose.yml`.
2. Start container:
   ```bash
   docker-compose up -d
   ```
3. Open: `http://localhost:6002` (or host IP)

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
- A green/red dot next to the ARR menu option indicates connectivity status (checked when you open the context menu).

---

## 🖥 UI Guide

### Main Menu / Settings Panel
The top‑left menu contains:
- **Export format** toggle (CSV/JSON)
- **Database tools**: Backup, Restore, Optimize, Backfill Metadata, Clean DB
- **Scan folders**: Pick specific folders per volume, assign type, and mute
- **Filter presets** (save/load/delete)

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
- Resolution, volumes, codecs, formats, source, container, edition, etc.

### Advanced Search
Supports tokens and quoted values:
- `source:"UHD Bluray"`
- `res:2160p`
- `year:2020`
- `type:tv`
- `category:dovi`
- `status:failed`

Tokens supported: `year`, `source`, `format`, `codec`, `res`, `category`, `volume`, `container`, `edition`, `type`, `media_type`, `status`, `hybrid`, `3d`.

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
- Sticky left/right columns keep checkbox and delete visible
- Header scrollbar for wide column sets
- Column header menu stays pinned when the header is stickied

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
- Right-click one or more selected rows and choose **ARR Search/Replace**.
- A **green dot** next to the option means Sonarr and Radarr are reachable; **red** means one or both failed the connection check.
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

### Scan Button
- Split scan button: **All**, **TV**, **Movie**.
- Hover TV/Movie to pick a specific typed folder from a submenu.
- The main button shows the selected mode and folder target.
- Click the progress bar during a scan to pause/resume scanning and analyzing.
- History button appears when idle and shows recent scan history with a per-entry report view.

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

### GET `/api/videos`
Paginated data with filters, sorting, stats, and filter options.

Key params:
- `search`, `category`, `profile`, `el`, `resolution`, `volume`, `container`
- `video_source`, `source_format`, `video_codec`, `media_type`
- `secondary_hdr`, `status`, `size_op`, `size_val`, `bit_op`, `bit_val`
- `sort`, `order`, `page`, `per_page`

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
