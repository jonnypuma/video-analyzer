# Changelog

All notable changes to Video Analyzer are documented here.

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
