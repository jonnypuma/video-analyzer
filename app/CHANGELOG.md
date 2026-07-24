# Changelog

All notable changes to Video Analyzer are documented here.

## 3.0.13

- DT-DL FEL: when multiple video streams exist, try RPU extract on secondary maps first (EL is usually not `v:0`); treat MediaInfo `EL+RPU` as FEL.

## 3.0.12

- DV FEL: try RPU extract on each video stream (real DT-DL EL may not be `v:0`); use MediaInfo `HDR_Format_Settings` (`BL+EL`/`FEL`/`MEL`) as EL fallback.
- `debug_deep.py`: list all video streams/tracks and per-map RPU attempts so mislabeled “DT-DL FEL” HDR10-only files are obvious.

## 3.0.11

- NFO Rotten Tomatoes: accept `tomatometerallcritics` / audience / critic variants (not only `rottentomatoes`), prefer All → users/audience → critics, and normalize max=10 scores to 0–100.

## 3.0.10

- Duplicates modal: sortable group and member column headers (asc/desc).
- Duplicate member rows now show bitrate, primary HDR (with DoVi profile/EL), secondary HDR, and audio codecs.

## 3.0.9

- Duplicates modal: remove scroll-area top padding so sticky Type/Match Basis headers sit flush under the toolbar (no gap with rows showing through).

## 3.0.8

- Duplicates modal: pin title + Refresh/Rebuild/Apply-filters toolbar outside the scroll area; keep Type/Match Basis/… column headers sticky while scrolling groups.

## 3.0.7

- Fix table collapse after opening Duplicates then using a ribbon filter: column-width sync now always targets `#video-table` thead (was matching the duplicates modal table first via `document.querySelector('thead')`).

## 3.0.6

- Duplicates modal defaults to the **entire library** (optional “Apply table filters”).
- When filters are applied, a group still appears if any member matches, and **Count** is the full group size (fixes 3+ copy titles disappearing when only one copy matched the table filter).

## 3.0.5

- Revert Docker build to `build: ./app` (repo-root context broke TrueNAS when `app/` was not the compose context).
- Ship `app/CHANGELOG.md` inside the image so the version badge is correct without relying on `APP_VERSION`.
- Keep `HOME=/tmp` to avoid gunicorn `/.gunicorn` permission errors.

## 3.0.4

- Docker build context is now the repo root so `CHANGELOG.md` is copied into the image (fixes version badge stuck on stale `APP_VERSION` / missing changelog when building only `./app`).
- Set `HOME=/tmp` in the image to avoid gunicorn `Permission denied: '/.gunicorn'` as non-root.
- After syncing code to the host that builds the image, rebuild with `docker compose build --no-cache && docker compose up -d`.

## 3.0.3

- Cache-bust static CSS/JS with `?v={{ app_version_label }}` so browsers pick up UI fixes after image rebuilds.
- Note: Docker only mounts `/output`; app code is baked into the image — rebuild with `docker compose up -d --build` to get duplicates delete-on-disk UI (native `confirm()` means an old image is still running).

## 3.0.2

- Scope freeze-pane sticky `thead` CSS to the main video table so the Duplicates modal no longer shows a scroll gap under the title.
- Clarify duplicates delete UX: hint text + confirm dialog copy for optional disk/folder delete (checkboxes appear after Delete Selected…).

## 3.0.1

- Duplicates / rebuild / bulk filter snapshots now re-read live filter controls from the DOM (fixes stale `activeFilters` showing far fewer duplicate groups).
- Raise z-index of delete confirmation modals so disk/folder checkboxes are visible above the duplicates overlay.

## 3.0.0

- Split the Flask monolith into the `video_analyzer` package (`config`, `state`, `db`, `queries`, `analysis`, `scan`, `routes`, …) with a thin `analyzer.py` WSGI entry (`analyzer:app` unchanged for Docker/Gunicorn).
- Extracted UI CSS/JS from `templates/index.html` into `static/css/app.css` and classic `static/js/*.js` scripts (onclick globals preserved; no module bundler).
- **Migration:** import internals from `video_analyzer.*` (or thin re-exports on `analyzer`) instead of the old single-file monolith symbols. Docker CMD stays `analyzer:app`.

## 2.1.9

- Added pytest suite for path confinement, schedule parsing, ZIP-slip validation, filter SQL, and `/api/health`.
- Added GitHub Actions CI workflow; `VIDEO_ANALYZER_TESTING` / `VIDEO_ANALYZER_OUTPUT` support for safe test imports.
- Dev deps in `app/requirements-dev.txt` (`pytest`).

## 2.1.8

- Docker maturity: `restart: unless-stopped`, compose + image `HEALTHCHECK` on `/api/health`, wire `SCAN_PATHS` / `APP_VERSION`.
- Pinned `app/requirements.txt`, pinned Chart.js CDN version, multi-arch `dovi_tool` (amd64/arm64), and `app/.dockerignore`.

## 2.1.7

- Safer deletes: main-table delete checks API errors/toasts; large or “delete all filtered” requires typing `DELETE`.
- Duplicates delete modal: optional **delete file on disk** and **delete containing folder** (preview lists folders; skips mount roots and folders with other videos).
- `POST /api/delete` supports `delete_files_on_disk` / `delete_folders`; new `POST /api/delete/preview` for folder impact. Disk delete requires explicit paths (not filter-all).

## 2.1.6

- CSV/JSON exports now use the same full column set as `/api/videos` (including titles, codecs, `missing`, `dup_*`, etc.).
- Large exports stream in 1000-row chunks to reduce peak memory use.

## 2.1.5

- Added `POST /api/rescan_files` for batched rescans (path confinement, threaded analysis, batched DB writes; max 50 paths/request).
- Bulk Edit and Duplicates rescan now chunk through the batch API instead of one HTTP call per file.

## 2.1.4

- Surface `/api/videos/meta` failures with an error toast and a **Retry** action that reloads ribbons/charts without reloading the table.
- Same toast path when refreshing charts-only after a meta error.

## 2.1.3

- Block heavy/mutating ops while a scan or backfill is running (`reject_if_busy`): restore, rescan, debug_deep, delete, duplicate rebuild, cleanup, DB maintenance, and start.
- Busy responses include current job detail and use HTTP 409 (start keeps 400 for compatibility).

## 2.1.2

- Hardened `/api/restore` against ZIP slip: reject traversal/symlink members, allow only `processed_videos.db` / `settings.json`, and write via explicit copy into `/output` (no `zipfile.extract` path joins).

## 2.1.1

- Confine `/api/rescan_file`, `/api/debug_deep`, and `/api/nfo_content` to paths under configured/discovered media mounts.
- Harden browse/cleanup path checks with `os.path.commonpath` so prefix tricks (e.g. `/mnt/movies` vs `/mnt/movies_backup`) cannot bypass roots.

## 2.1.0

- Weekly and monthly scan schedules now register APScheduler cron jobs (previously only daily/interval worked).
- Weekly/monthly accept an optional time (`dow|HH:MM` / `day|HH:MM`); default time is 03:00.
- Persisted schedule is restored on app startup so jobs survive container restarts.

## 2.0.9

- Fixed page scroll / jump-to-log: freeze-pane table height no longer grows on page scroll (which had pushed the activity console out of reach).
- Scan-info click uses `scrollIntoView` to jump to the system activity log under the table.

## 2.0.8

- Default table page size is now 50 (was 100) in the UI selector and `/api/videos` fallback, so first paint and filter reloads transfer fewer rows.

## 2.0.7

- Added SQLite indexes for common filter columns (`el_type`, `secondary_hdr`, `edition`, `missing`, `nfo_missing`, `is_source_hybrid`, `file_size`, `bitrate_mbps`, `dup_count`).
- Added `LOWER(column)` expression indexes so case-insensitive filter predicates can use the index.

## 2.0.6

- Count audio filter facets with a SQLite recursive CTE instead of loading every `audio_codecs` row into Python.

## 2.0.5

- Hot filter path loads `/api/videos/meta` without dropdown facet counts (`include_options=0`).
- Filter option counts refresh on a short delayed schedule so rapid filtering stays responsive.

## 2.0.4

- Debounce filter-driven table reloads (300ms) so rapid select/multiselect changes coalesce into one request.
- Search keeps a 400ms debounce; Clear Filters still reloads immediately.

## 2.0.3

- Compute ribbon/library stats with SQL aggregates instead of loading every matching row into Python.
- Filtered chart stats reuse the same SQL enrichment path (vol/res/path facets included).

## 2.0.2

- Cache unfiltered library stats (totals, movie/TV scopes, sizes, volume/resolution/path facets) across filter clicks.
- Invalidate that cache on any DB write commit and after database restore.

## 2.0.1

- Split video loading into a fast `/api/videos` (table rows) and `/api/videos/meta` (stats, charts, filter facet counts).
- The UI renders the table as soon as rows arrive; ribbons/charts/dropdowns update when meta completes.

## 2.0.0

- Added Excel-like freeze-pane table headers: column headers (with filters/sort) stay visible while scrolling rows.
- Table body scrolls inside a single viewport scrollport (`#table-h-scroll`) sized to the remaining window height.
- Switched the main grid to `border-collapse: separate` so sticky headers stay aligned with `<colgroup>` widths.
- Checkbox and delete columns remain frozen on the left/right edges, including in the sticky header corners.
- Column picker (hamburger menu) is sticky inside the same scrollport so it stays with the frozen header while scrolling.
- Moved column width reset into the left side of the column picker (label: Reset) to avoid overlapping the right-edge controls.

## 1.1.0

- Added top-header app version display beside the Video Analyzer title.
- Added MediaInfo, FFmpeg, FFprobe, dovi_tool, and Python version reporting to the System Health modal.
- Added this changelog.

## 1.0.1

- Documented that the app is intended for trusted LAN use only.
- Hardened request handling by allowlisting SQL sort direction and clamping pagination inputs.
- Fixed startup database initialization to use a write transaction.
- Escaped additional dynamic frontend HTML rendered from filenames, metadata, logs, scan folders, suggestions, and reports.

## 1.0.0

- Added persistent duplicate fields: `dup_group_key`, `dup_exact_key`, and `dup_count`.
- Added duplicate indexes and APIs for rebuilding keys, listing groups, and listing members.
- Added duplicate review UI with group details, keep recommendation, copy actions, rescan, and delete actions.
- Added optional duplicate checking during scans, disabled by default.
- Added duplicate badges in the main table.
- Hardened metadata precedence so NFO media type takes priority over filename parsing.
- Restored horizontal table scrolling and improved column sizing, sticky edge dividers, and column menu layout.
