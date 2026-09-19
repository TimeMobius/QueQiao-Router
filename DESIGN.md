# Design System — QueQiao Router Monitoring UI

Implementation contract for the framework-free dashboard (`web/index.html`,
`web/records.html`, `web/analysis.html`) served under `/dashboard`. Every color, size, and spacing value in the
pages must trace to a token here. Framework: vanilla HTML/CSS/JS embedded with `rust-embed`.

## 0. Research Log

- **Branch:** existing project with implicit patterns and a partial component layer — the
  triage step is "extract what exists and codify it" (design router Phase 0, branch 4).
  What exists: `web/index.html` `:root` + `[data-theme="dark"]` (lines 11–51), the records
  navy topbar, and a small set of implicit primitives (`.btn-primary`, `.field`, inputs).
- **Embedded references:** skipped. This is an operational/internal dashboard with no named
  brand and no surface-ambition signal; the unified shell is a consolidation, not a
  greenfield art direction. No Layer A/B brand file applies to the existing slate/navy system.
- **Lazyweb / StyleGallery / imagen:** skipped (network research lanes). The spatial pattern
  required here is prescribed: bounded `100dvb` app shell with one scroll owner per region
  (`layout-skill.md` §2). Revisit StyleGallery (`layout/index.md`, `CATALOG.md`) only if the
  shell is redesigned.
- **Extracted from:** `web/index.html:11-51` (light/dark palette), `web/records.html:9-27`
  (navy + semantic status colors), and the shared type/spacing already in use.

## 1. Direction

A calm, information-dense operations console: a single stable navy brand bar over a
light/dark content plane, slate neutrals, and blue as the only interaction accent. Materials
are borders + a single soft shadow (no glass, no gradients); density and legibility carry the
surface. Every interactive element states its affordance through hover/focus/busy states.

The shell uses three exact-path tabs on every page: `实时监控` (`/dashboard`), `日志记录`
(`/dashboard/records`), and `日志分析` (`/dashboard/analysis`). `shell.js` owns the active
state by matching each link's normalized `href` to `location.pathname`.

## 2. Color tokens

Single source: `web/assets/shell.css`. Pages must not declare their own `:root`.

| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg-primary` | `#f8fafc` | `#0f172a` | page plane |
| `--bg-secondary` | `#ffffff` | `#1e293b` | cards, inputs, drawer |
| `--bg-tertiary` | `#f1f5f9` | `#334155` | table head, segmented track, hovers |
| `--text-primary` | `#1e293b` | `#f1f5f9` | body + values |
| `--text-secondary` | `#64748b` | `#94a3b8` | labels, secondary cells |
| `--text-muted` | `#94a3b8` | `#64748b` | placeholders, metadata |
| `--border-color` | `#e2e8f0` | `#334155` | control + card borders |
| `--border-light` | `#f1f5f9` | `#1e293b` | row dividers |
| `--primary` | `#3b82f6` | `#60a5fa` | focus ring, hover accent |
| `--primary-strong` | `#2563eb` | `#3b82f6` | links, active text |
| `--brand-navy` / `--brand-navy-deep` | `#1f3a6d` / `#16224a` | same | app bar, primary button |
| `--success` / `--warning` / `--danger` | `#10b981` / `#f59e0b` / `#ef4444` | same | 2xx / 3xx-4xx / 5xx + status dot |
| `--danger-bg` / `--danger-border` / `--danger-text` | `#fef3f2` / `#fee4e2` / `#b42318` | dark variants | error banner, log error block |
| `--warning-bg` / `--warning-text` | `#fff4e5` / `#b54708` | dark variants | stream-interrupted badge |
| `--success-bg` / `--success-border` / `--success-text` | `#ecfdf5` / `#a7f3d0` / `#059669` | dark variants | positive pill |
| `--accent-violet` | `#7c5cd6` | `#a78bfa` | token-count metric icon |
| `--chart-*`, `--focus-ring` | see shell.css | mirrors | ECharts + focus ring |

## 3. Typography

- Font stack (`--font-sans`): `'Inter'` (local `vendor/fonts/inter.css`) → system CJK
  (`'PingFang SC'`, `'Microsoft YaHei'`) → sans fallback. Both pages link the same stack;
  records.html must link `inter.css` too.
- Mono (`--font-mono`): `ui-monospace, SFMono-Regular, Menlo, Consolas, monospace` — IDs,
  timestamps, payload previews, token counts.
- Sizes: 32px stat value; 24px page title (records uses table-led layout); 16px card title;
  15px brand; 14px body/nav; 13px controls/table; 12px metadata; 11px labels/pills.
- Numeric data uses `font-variant-numeric: tabular-nums`.

## 4. Spacing & shape

- 4px base unit; gap/padding steps 4 / 8 / 10 / 12 / 14 / 16 / 20 / 22 / 24 / 32.
- Radii: `--radius-lg` 16px (cards/charts), `--radius-md` 10px (records card), `--radius-sm`
  8px (controls), `--radius-pill` 999px (statuses, badges).
- Depth: borders-first. Only `--shadow-sm` (cards) and `--shadow-md` (hover/drawer) allowed.
- Header height: `--header-h` 56px.

## 5. Primitives (shared, `web/assets/shell.css`)

| Primitive | States |
|---|---|
| `.app-header` / `.app-brand` / `.app-nav` | default; nav `:hover`; `[aria-current="page"]` |
| `.theme-select` | default, `:hover`, `:focus-visible` |
| `.app-status` + `.app-status-dot` | `data-state="ok" \| "down" \| "unknown"` (dot + label) |
| `.app-updated` | `<time>` set by `QQShell.setUpdated()` |
| `.btn` / `.btn-primary` / `.btn-ghost` | default, `:hover:not(:disabled)`, `:focus-visible`, `:disabled` |
| `.field` | icon-prefixed inputs; `input.has-counter` adds right padding |
| `input` / `select` / `textarea` | default, `:focus` (primary border + `--focus-ring`), `:disabled` |
| `.banner` / `.banner-error` | hidden; `.show` reveals |
| `.page-fill` | fills remaining `app-main` height, `min-height:0`, column flex |

Records-specific (kept in `records.html`): segmented mode toggle, `.rounded` range, table,
preview clamp, status text colors, log entries, drawer, pager size overrides.
Index-specific (kept in `index.html`): stats grid, charts grid, `.chart-select`, model table.
Analysis-specific (kept in `analysis.html` / `assets/analysis.js`): two-row filter cluster,
eight summary metrics, ECharts trend, error ranking table, dimension pagination table, and
top-distribution bars. The page consumes `GET /dashboard/api/analysis` with explicit `from`
and `to` epoch milliseconds plus `interval`, `dimension`, `orderBy`, `page`, `pageSize`,
`topLimit`, and the shared `model`, `ip`, `apikey`, `type`, `backend`, `status`, `client`,
and `errors` filters. Error ranking consumes `GET /dashboard/api/analysis/errors` with the
same shared filters plus `source=db|log` and `limit`.

## 6. Layout & scroll ownership

- Shell: `body` is `display:flex; flex-direction:column; min-height:100dvh` (never `vh`).
  Header is fixed height; `.app-main` is `flex:1; min-height:0`.
- **One scroll owner per region.** Index: `.main-content` (`overflow:auto`) inside
  `.app-main`. Records: `.card.page-fill` owns no scroll; `.table-scroll` / `.log-list` are
  the scroll owners each with `min-height:0`. The drawer body owns its own scroll.
- Analysis: `.analysis-scroll` is the single page scroll owner below the fixed shell header;
  chart and table regions remain intrinsic and the two-column rows stack below 900px.
- The 375px reflow turns filters into a single column with no horizontal scrollbar; the time
  range stacks vertically and its inputs are `width:100%`.

## 7. Motion

- Motion communicates state only: color/border transitions 120–150ms, drawer transform
  240ms `cubic-bezier(.4,0,.2,1)`, banner reveal. No decorative animation.
- GPU-composited properties only (`transform`, `opacity`, `border-color`, `color`).
- `prefers-reduced-motion: reduce` collapses all durations (defined in `shell.css`).
- Status dot is static; no pulse.

## 8. Accessibility constraints

- `<header>`, `<nav aria-label>`, `<main>`, `<time>` landmarks; nav active state via
  `aria-current="page"`; status uses `role="status"`; theme select has `aria-label`.
- Focus is always visible via `:focus-visible` (never `outline:none` without a ring).
- No emojis as icons — inline SVG (Lucide-style stroke paths) or text only.
- Body text contrast meets WCAG AA against `--bg-primary` / `--bg-secondary` in both themes.

## 9. Accepted debt

- Records metric-icon accents (`.metric.tokens` violet) are the only page-local accents; the
  rest are tokenized. Full records dark-mode polish of the drawer/table is deferred.
- The `index.html` chart series palette is generated in JS (ECharts), not token-derived;
  chart theming reads the effective theme via `QQShell.onThemeChange`.
- Table redesign (cards vs table, column priority, empty/long/unbroken stress states) is a
  later phase; this phase only unifies the shell and fixes the listed 375px / height bugs.
- Records table (Phase 2, refined): `table-layout: fixed`, 9 columns — 请求时间 178 / 模型 215 /
  状态 70 / 客户端 112 / 会话·请求 112 / 用量 140 / 时延 160 / 提问预览 auto / 操作 78.
  No `#` column (id lives in the drawer); list timestamps are second-precision with the full
  value in `title` and in the drawer. 用量 stacks message rounds + tool count on the first row
  and total tokens on the second, reusing the row's existing two-line height instead of
  wrapping unpredictably; 时延/用量 never ellipsize numeric values. 会话·请求 hidden ≤1279px;
  客户端 + 用量 hidden ≤900px; 提问预览 gets a fixed
  170/180px at those breakpoints; 状态 stays third in source order so it is reachable with a
  short scroll at 375px. `.table-wrap` fade/shadow is the scroll affordance whenever
  `scrollWidth > clientWidth`. The table still scrolls horizontally on narrow screens rather
  than reflowing to cards (deferred).
- Filter regroup (Phase 2): `.range-preset` quick ranges, `<details class="advanced-filters">`
  for exact from/to, 300ms search debounce with a short-keyword hint, and URL state via
  `history.replaceState` (no cursor in the URL, so paging stays in-memory).
- States/a11y (Phase 3): shimmer skeletons (collapsed by `prefers-reduced-motion`), empty vs
  no-match with 清除筛选, `.banner[role=alert]` + 重试, drawer `role="dialog"`/focus trap/
  `inert` background, and row actions as real `<button>`s. Drawer inner `apiKeyToggle` remains
  a clickable `<span>` (not keyboard-operable) — deferred.
