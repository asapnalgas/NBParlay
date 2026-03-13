function getApiCandidates(url) {
  const raw = String(url || "").trim();
  if (/^https?:\/\//i.test(raw)) {
    return [raw];
  }

  const normalized = raw.startsWith("/") ? raw : `/${raw}`;
  const isHttpContext = window.location.protocol === "http:" || window.location.protocol === "https:";
  const candidates = [];

  if (isHttpContext) {
    candidates.push(normalized);
  }
  candidates.push(`http://localhost:8010${normalized}`);
  candidates.push(`http://127.0.0.1:8010${normalized}`);

  return [...new Set(candidates)];
}

async function parseJSONResponse(response) {
  const text = await response.text();
  if (!text) {
    return {};
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`Invalid JSON from server (${response.status})`);
  }
}

function resolveSnapshotPath(url) {
  const raw = String(url || "").trim();
  if (!raw || /^https?:\/\//i.test(raw)) {
    return null;
  }

  const normalized = raw.startsWith("/") ? raw : `/${raw}`;
  const [path, query] = normalized.split("?");
  const params = new URLSearchParams(query || "");

  if (path === "/api/schema") return "/ui/snapshot/schema.json";
  if (path === "/api/status") return "/ui/snapshot/status.json";
  if (path === "/api/live/sources") return "/ui/snapshot/live-sources.json";
  if (path === "/api/data/pipeline") return "/ui/snapshot/pipeline.json";
  if (path === "/api/data/drift-audit") return "/ui/snapshot/drift-audit.json";

  if (path === "/api/player-board") {
    const date = (params.get("date") || "").trim();
    if (date && date !== "all") {
      return `/ui/snapshot/player-board-${encodeURIComponent(date)}.json`;
    }
    if (date === "all") {
      return "/ui/snapshot/player-board-all.json";
    }
    return "/ui/snapshot/player-board.json";
  }

  return null;
}

async function fetchSnapshotJSON(url) {
  const candidate = resolveSnapshotPath(url);
  if (!candidate) {
    throw new Error("Snapshot fallback is not available for this endpoint.");
  }

  const fallbackCandidates = [candidate];
  // Compatibility fallback when app is served from local API root.
  if (candidate.startsWith("/ui/")) {
    fallbackCandidates.push(candidate.replace("/ui/", "/"));
  }

  let lastError = null;
  for (const snapshotPath of [...new Set(fallbackCandidates)]) {
    try {
      const response = await fetch(snapshotPath, { cache: "no-store" });
      const payload = await parseJSONResponse(response);
      if (!response.ok) {
        throw new Error(payload.error || `Snapshot request failed (${response.status})`);
      }
      return payload;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("Snapshot request failed");
}

function isHostedContext() {
  const host = String(window.location.hostname || "").toLowerCase();
  return !["127.0.0.1", "localhost"].includes(host);
}

async function fetchJSON(url, options = {}) {
  const snapshotPath = resolveSnapshotPath(url);
  if (snapshotModeEnabled && snapshotPath) {
    return fetchSnapshotJSON(url);
  }

  // On hosted static pages, prefer snapshot payloads immediately for known endpoints.
  if (!snapshotModeEnabled && snapshotPath && isHostedContext()) {
    try {
      const payload = await fetchSnapshotJSON(url);
      enableSnapshotMode();
      return payload;
    } catch (_error) {
      // Fall through to API candidates if snapshot files are missing.
    }
  }

  const candidates = getApiCandidates(url);
  let lastError = null;

  for (const candidate of candidates) {
    try {
      const response = await fetch(candidate, options);
      const payload = await parseJSONResponse(response);
      if (!response.ok) {
        throw new Error(payload.error || "Request failed");
      }
      return payload;
    } catch (error) {
      lastError = error;
    }
  }

  if (snapshotPath) {
    try {
      const payload = await fetchSnapshotJSON(url);
      enableSnapshotMode();
      return payload;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("Request failed");
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function pollBackgroundJob(jobId, outputId, label, options = {}) {
  const intervalMs = Number(options.intervalMs || 2000);
  const refreshOnDone = options.refreshOnDone !== false;
  const maxMs = Number(options.maxMs || 20 * 60 * 1000);
  const startedAt = Date.now();

  while (true) {
    const payload = await fetchJSON(`/api/jobs/status?job_id=${encodeURIComponent(jobId)}`);
    const job = payload.job || {};
    const status = String(job.status || "unknown");
    const elapsedSec = Math.max(0, Math.round((Date.now() - startedAt) / 1000));

    if (status === "completed") {
      setText(
        outputId,
        prettyJSON({
          message: `${label} completed.`,
          elapsed_seconds: elapsedSec,
          job_id: jobId,
          result: job.result ?? null,
        })
      );
      if (refreshOnDone) {
        await refresh();
      }
      return job;
    }

    if (status === "failed") {
      setText(
        outputId,
        prettyJSON({
          message: `${label} failed.`,
          elapsed_seconds: elapsedSec,
          job_id: jobId,
          error: job.error || "Unknown error",
        })
      );
      if (refreshOnDone) {
        await refresh();
      }
      return job;
    }

    setText(
      outputId,
      `${label} is running...\nJob: ${jobId}\nStatus: ${status}\nElapsed: ${elapsedSec}s\nStarted: ${job.started_at || "n/a"}`
    );

    if (Date.now() - startedAt > maxMs) {
      setText(
        outputId,
        `${label} is still running after ${Math.round(maxMs / 1000)}s.\nJob: ${jobId}\nYou can continue using the board and check back.`
      );
      return job;
    }

    await sleep(intervalMs);
  }
}

async function runBackgroundJob(startEndpoint, outputId, label, options = {}) {
  setText(outputId, `${label} queued...`);
  const payload = await fetchJSON(startEndpoint, { method: "POST" });
  const job = payload.job || null;

  if (!job || !job.job_id) {
    setText(outputId, prettyJSON(payload));
    if (options.refreshOnDone) {
      await refresh();
    }
    return payload;
  }

  if (payload.status === "already_running") {
    setText(outputId, `${label} is already running.\nJob: ${job.job_id}\nPolling status...`);
  }

  return pollBackgroundJob(job.job_id, outputId, label, options);
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value;
  }
}

function prettyJSON(value) {
  return JSON.stringify(value, null, 2);
}

let boardPayload = null;
let boardRenderLimit = 200;
let boardQuickChip = "all";
let latestStatus = null;
let latestLiveSourcesPayload = null;
const WATCHLIST_STORAGE_KEY = "nba_model_watchlist_v1";
const DEVELOPER_MODE_STORAGE_KEY = "nba_model_developer_mode_v1";
const BOARD_LAYOUT_STORAGE_KEY = "nba_model_board_layout_mode_v1";
const watchlist = new Set();
const AUTO_UI_REFRESH_TIMER_MS_DEFAULT = 10 * 1000;
let autoUiRefreshTimerMs = AUTO_UI_REFRESH_TIMER_MS_DEFAULT;
let autoUiRefreshNextAt = Date.now() + autoUiRefreshTimerMs;
let autoRefreshTimerHandle = null;
let developerModeEnabled = false;
let boardLayoutMode = "best_of_both";
let liveAutoStartAttempted = false;
let snapshotModeEnabled = false;
let fridayConversation = [];
let fridayInitialized = false;
let fridayBusy = false;
let fridayStatusPayload = null;
let accountStatusPayload = null;

const SNAPSHOT_MODE_DISABLED_BUTTON_IDS = [
  "upload-training",
  "upload-upcoming",
  "upload-context",
  "upload-season-priors",
  "paste-season-priors",
  "upload-prizepicks",
  "paste-prizepicks",
  "train-engine",
  "run-predictions",
  "run-recheck",
  "generate-prizepicks-edges",
  "live-start",
  "live-stop",
  "live-sync-now",
  "live-in-game-sync-now",
  "sync-and-refresh-live",
  "board-daily-refresh",
  "friday-send",
  "friday-connect",
  "friday-disconnect",
  "account-register",
  "account-login",
  "account-logout",
  "account-upgrade-monthly",
  "account-upgrade-yearly",
  "account-manage-billing",
];

function applySnapshotModeUI() {
  document.body.classList.add("snapshot-mode");
  SNAPSHOT_MODE_DISABLED_BUTTON_IDS.forEach((id) => {
    const node = document.getElementById(id);
    if (!node) return;
    node.disabled = true;
    node.setAttribute("title", "Disabled in web snapshot mode");
  });
  ["friday-api-key", "friday-model", "friday-agent-mode", "friday-input"].forEach((id) => {
    const node = document.getElementById(id);
    if (!node) return;
    node.disabled = true;
    node.setAttribute("title", "Disabled in web snapshot mode");
  });
  setText("live-source-status", "Web snapshot mode: read-only data loaded from packaged JSON snapshots.");
}

function enableSnapshotMode() {
  if (snapshotModeEnabled) return;
  snapshotModeEnabled = true;
  applySnapshotModeUI();
}

function playerKey(player) {
  return `${String(player.player_name || "").toLowerCase().trim()}|${String(player.team || "").toLowerCase().trim()}`;
}

function loadWatchlist() {
  try {
    const raw = window.localStorage.getItem(WATCHLIST_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return;
    parsed.forEach((entry) => {
      if (typeof entry === "string" && entry.trim()) {
        watchlist.add(entry.trim());
      }
    });
  } catch (_error) {
    // Ignore localStorage parsing errors and continue with empty watchlist.
  }
}

function saveWatchlist() {
  try {
    window.localStorage.setItem(WATCHLIST_STORAGE_KEY, JSON.stringify([...watchlist]));
  } catch (_error) {
    // Ignore write failures (for example, private browsing quotas).
  }
}

function loadDeveloperMode() {
  try {
    const raw = window.localStorage.getItem(DEVELOPER_MODE_STORAGE_KEY);
    developerModeEnabled = raw === "1";
  } catch (_error) {
    developerModeEnabled = false;
  }
}

function saveDeveloperMode() {
  try {
    window.localStorage.setItem(DEVELOPER_MODE_STORAGE_KEY, developerModeEnabled ? "1" : "0");
  } catch (_error) {
    // Ignore localStorage write failures.
  }
}

function renderDeveloperMode() {
  const button = document.getElementById("developer-mode-toggle");
  const statusNode = document.getElementById("developer-mode-status");
  document.body.classList.toggle("developer-mode-enabled", developerModeEnabled);
  if (button) {
    button.textContent = developerModeEnabled ? "Developer Mode: On" : "Developer Mode: Off";
    button.classList.toggle("alt", developerModeEnabled);
    button.classList.toggle("ghost", !developerModeEnabled);
  }
  if (statusNode) {
    statusNode.textContent = developerModeEnabled
      ? "Developer mode is active. Advanced data/model controls are visible."
      : "Public mode is active. Advanced data/model controls are hidden.";
  }
}

function toggleDeveloperMode() {
  developerModeEnabled = !developerModeEnabled;
  saveDeveloperMode();
  renderDeveloperMode();
  if (latestLiveSourcesPayload) {
    renderLiveSources(latestLiveSourcesPayload);
  }
}

function loadBoardLayoutMode() {
  try {
    const raw = window.localStorage.getItem(BOARD_LAYOUT_STORAGE_KEY);
    if (raw === "rotowire" || raw === "betr" || raw === "best_of_both") {
      boardLayoutMode = raw;
    }
  } catch (_error) {
    boardLayoutMode = "best_of_both";
  }
}

function saveBoardLayoutMode() {
  try {
    window.localStorage.setItem(BOARD_LAYOUT_STORAGE_KEY, boardLayoutMode);
  } catch (_error) {
    // Ignore localStorage write failures.
  }
}

function renderBoardLayoutMode() {
  document.body.classList.toggle("layout-rotowire", boardLayoutMode === "rotowire");
  document.body.classList.toggle("layout-betr", boardLayoutMode === "betr");
  const select = document.getElementById("board-layout-mode");
  if (select && select.value !== boardLayoutMode) {
    select.value = boardLayoutMode;
  }
}

function setBoardLayoutMode(mode) {
  const normalized = String(mode || "").trim();
  if (normalized === "rotowire" || normalized === "betr" || normalized === "best_of_both") {
    boardLayoutMode = normalized;
  } else {
    boardLayoutMode = "best_of_both";
  }
  saveBoardLayoutMode();
  renderBoardLayoutMode();
}

function toggleWatchlist(player) {
  const key = playerKey(player);
  if (watchlist.has(key)) {
    watchlist.delete(key);
  } else {
    watchlist.add(key);
  }
  saveWatchlist();
}

function reportUiError(error, preferredOutputId = "board-output") {
  const message = (error && error.message) ? error.message : String(error || "Unknown UI error");
  setText(preferredOutputId, `Error: ${message}`);
  setText("live-source-status", `Error: ${message}`);
}

function bindElementEvent(id, eventName, handler, preferredOutputId = "board-output") {
  const node = document.getElementById(id);
  if (!node) return;
  node.addEventListener(eventName, (event) => {
    try {
      const maybePromise = handler(event);
      if (maybePromise && typeof maybePromise.then === "function") {
        maybePromise.catch((error) => reportUiError(error, preferredOutputId));
      }
    } catch (error) {
      reportUiError(error, preferredOutputId);
    }
  });
}

function renderAutoRefreshStatus() {
  const remainingMs = Math.max(0, autoUiRefreshNextAt - Date.now());
  const minutes = Math.floor(remainingMs / 60000);
  const seconds = Math.floor((remainingMs % 60000) / 1000);
  const intervalLabel = autoUiRefreshTimerMs <= 60000
    ? `${Math.round(autoUiRefreshTimerMs / 1000)} seconds`
    : `${Math.round(autoUiRefreshTimerMs / 60000)} minutes`;
  setText(
    "ui-auto-refresh-status",
    `UI auto-refresh runs every ${intervalLabel}. Next refresh in ${minutes}m ${String(seconds).padStart(2, "0")}s.`
  );
}

function resolveUiRefreshIntervalMs(statusPayload) {
  const automation = statusPayload?.automation || {};
  const live = statusPayload?.live_sync || {};
  const state = live.state || {};
  const config = live.config || {};
  const secondsCandidate = Number(
    automation.projection_refresh_interval_seconds
    || config.projection_refresh_interval_seconds
    || config.poll_interval_seconds
    || state.projection_refresh_interval_seconds
    || 10
  );
  if (!Number.isFinite(secondsCandidate) || secondsCandidate <= 0) {
    return Math.max(AUTO_UI_REFRESH_TIMER_MS_DEFAULT, 10000);
  }
  const clampedSeconds = Math.max(10, Math.min(60, Math.round(secondsCandidate)));
  return clampedSeconds * 1000;
}

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderSchema(schema) {
  const shell = document.getElementById("schema-block");
  shell.innerHTML = "";

  const groups = [
    ["Required Training Columns", schema.required_training_columns || []],
    ["Optional Context Columns", schema.optional_context_columns || []],
    ["Fantasy Support Columns", schema.optional_support_columns_for_fantasy || []],
    ["Notes", schema.notes || []],
  ];

  groups.forEach(([label, items]) => {
    const div = document.createElement("div");
    div.className = "schema-group";
    div.innerHTML = `<strong>${label}</strong><div>${items.join(", ")}</div>`;
    shell.appendChild(div);
  });
}

function renderDownloads(status) {
  const shell = document.getElementById("download-links");
  shell.innerHTML = "";
  if (snapshotModeEnabled) {
    shell.innerHTML = `<div class="download-item"><strong>Web Snapshot Mode</strong><div>Runtime downloads are disabled in this deployed read-only view.</div></div>`;
    return;
  }
  const downloads = status.downloads || {};
  Object.entries(downloads).forEach(([label, filename]) => {
    const div = document.createElement("div");
    div.className = "download-item";
    if (filename) {
      div.innerHTML = `<strong>${label}</strong><div><a href="/downloads/${filename}">Download ${filename}</a></div>`;
    } else {
      div.innerHTML = `<strong>${label}</strong><div>Not available yet</div>`;
    }
    shell.appendChild(div);
  });
}

function renderTableInto(id, rows, emptyMessage, options = {}) {
  const shell = document.getElementById(id);
  const previousSearch = shell.querySelector(".table-search-input")?.value || "";
  shell.innerHTML = "";
  if (!rows || rows.length === 0) {
    shell.textContent = emptyMessage;
    return;
  }

  const columns = Object.keys(rows[0]);
  const wrapper = document.createElement("div");
  const toolbar = document.createElement("div");
  const searchInput = document.createElement("input");
  const meta = document.createElement("div");
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const tbody = document.createElement("tbody");

  wrapper.className = "table-wrap";
  toolbar.className = "table-toolbar";
  searchInput.className = "table-search-input";
  searchInput.type = "search";
  searchInput.placeholder = options.searchPlaceholder || "Search this table";
  searchInput.value = previousSearch;
  meta.className = "table-meta";

  const headRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);

  function renderBody(visibleRows) {
    tbody.innerHTML = "";

    if (!visibleRows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = columns.length;
      td.textContent = "No matching rows.";
      td.className = "table-empty";
      tr.appendChild(td);
      tbody.appendChild(tr);
    } else {
      visibleRows.forEach((row) => {
        const tr = document.createElement("tr");
        columns.forEach((column) => {
          const td = document.createElement("td");
          td.textContent = row[column];
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }

    meta.textContent = `Showing ${visibleRows.length} of ${rows.length} rows`;
  }

  function applyFilter() {
    const term = searchInput.value.trim().toLowerCase();
    const visibleRows = !term
      ? rows
      : rows.filter((row) =>
          columns.some((column) => String(row[column] ?? "").toLowerCase().includes(term))
        );
    renderBody(visibleRows);
  }

  table.appendChild(thead);
  table.appendChild(tbody);
  toolbar.appendChild(searchInput);
  toolbar.appendChild(meta);
  wrapper.appendChild(toolbar);
  wrapper.appendChild(table);
  shell.appendChild(wrapper);

  searchInput.addEventListener("input", applyFilter);
  applyFilter();
}

function renderLiveSummary(live) {
  const shell = document.getElementById("live-summary");
  shell.innerHTML = "";
  if (!live) {
    shell.textContent = "Live sync not configured.";
    return;
  }

  const state = live.state || {};
  const providers = state.providers || {};
  const projectionRefreshSeconds = Number(
    state.projection_refresh_interval_seconds
    || live.config.projection_refresh_interval_seconds
    || 10
  );
  const optimizationRefreshSeconds = Number(
    state.optimization_interval_seconds
    || live.config.optimization_interval_seconds
    || 10
  );
  const retrainRefreshSeconds = Number(
    state.retrain_interval_seconds
    || live.config.retrain_interval_seconds
    || projectionRefreshSeconds
  );
  const formatInterval = (seconds, fallback) => {
    const value = Number(seconds);
    if (!Number.isFinite(value) || value <= 0) return fallback;
    if (value < 60) return `${Math.round(value)} sec`;
    if (value % 60 === 0) return `${Math.round(value / 60)} min`;
    return `${Math.round(value)} sec`;
  };

  const pill = document.createElement("div");
  pill.className = `live-pill ${live.running ? "" : "off"}`;
  const inGameInterval = Number(live.config.in_game_projection_refresh_interval_seconds || state.in_game_projection_refresh_interval_seconds || 10);
  const inGameEnabled = Boolean(live.config.auto_refresh_in_game_projections ?? true);
  pill.textContent = live.running
    ? `Running full sync every ${Number(live.config.poll_interval_seconds || 10)} sec | In-game projection refresh every ${inGameEnabled ? inGameInterval : "off"}${inGameEnabled ? " sec" : ""}`
    : "Live sync stopped";
  shell.appendChild(pill);

  const stats = document.createElement("div");
  stats.className = "live-stat-grid";
  const items = [
    ["Last sync", state.last_sync_at || "Never"],
    ["Games seen", state.games_seen ?? 0],
    ["Scheduled", state.games_scheduled ?? 0],
    ["Live", state.games_live ?? 0],
    ["Final", state.games_final ?? 0],
    ["Backfill window", state.backfill_window_start && state.backfill_window_end ? `${state.backfill_window_start} -> ${state.backfill_window_end}` : "n/a"],
    ["Backfill rows", state.backfill_rows_appended ?? 0],
    ["Scoreboard rows", state.scoreboard_rows_appended ?? 0],
    ["Backfill games fetched", state.backfill_games_fetched ?? 0],
    ["Backfill games failed", state.backfill_games_failed ?? 0],
    ["Slate games", state.scheduled_games_found ?? 0],
    ["Slate dates", (state.scheduled_game_dates || []).join(", ") || "None"],
    ["Slate sources", (state.scheduled_sources || []).join(", ") || "None"],
    ["Rows appended", state.completed_rows_appended ?? 0],
    ["Upcoming rows", state.upcoming_rows_generated ?? 0],
    ["Provider rows", state.provider_context_rows ?? 0],
    ["Training lookback", state.model_training_lookback_days ? `${state.model_training_lookback_days} days` : "full history"],
    ["Projection refresh interval", formatInterval(projectionRefreshSeconds, "10 sec")],
    ["In-game refresh interval", `${Math.round(inGameInterval)} sec`],
    ["Retrain interval", formatInterval(retrainRefreshSeconds, "10 sec")],
    ["Optimization interval", formatInterval(optimizationRefreshSeconds, "60 min")],
    ["Last projection refresh", state.last_projection_refresh_at || "Never"],
    ["Next projection refresh", state.next_projection_refresh_due_at || "n/a"],
    ["Last retrain", state.last_retrain_refresh_at || "Never"],
    ["Next retrain", state.next_retrain_due_at || "n/a"],
    ["Last in-game refresh", state.last_in_game_projection_refresh_at || "Never"],
    ["Next in-game refresh", state.next_in_game_projection_refresh_due_at || "n/a"],
    ["In-game rows updated", state.in_game_projection_rows_updated ?? 0],
    ["In-game players tracked", state.in_game_projection_players_tracked ?? 0],
    ["In-game games tracked", state.in_game_projection_games_tracked ?? 0],
    ["Live games active", state.in_game_projection_live_games_active ?? 0],
    ["In-game note", state.in_game_projection_note || "None"],
    ["In-game error", state.in_game_projection_last_error || "None"],
    ["Last optimization", state.last_optimization_at || "Never"],
    ["Next optimization", state.next_optimization_due_at || "n/a"],
    ["Lineup rows matched", state.lineup_rows_matched ?? 0],
    ["Live roster rows matched", state.live_roster_rows_matched ?? 0],
    ["Player-prop rows matched", state.player_props_rows_matched ?? 0],
    ["News rows matched", state.news_rows_matched ?? 0],
    ["News articles (24h pull)", state.news_articles_loaded ?? 0],
    ["Home context rows", state.home_context_rows ?? 0],
    ["Hometown context rows", state.hometown_context_rows ?? 0],
    ["Teammate context rows", state.teammate_context_rows ?? 0],
    ["Profile cache rows", state.profile_cache_rows ?? 0],
    ["Profiles fetched this cycle", state.profiles_fetched ?? 0],
    ["Starter probability rows", state.starter_probability_rows ?? 0],
    ["Injury risk rows", state.injury_risk_rows ?? 0],
    ["Injury multiplier rows", state.injury_multiplier_rows ?? 0],
    ["Backfill note", state.backfill_note || "None"],
    ["Odds rows", providers.odds ? providers.odds.rows ?? 0 : 0],
    ["Props rows", providers.player_props ? providers.player_props.rows ?? 0 : 0],
    ["Lineup rows", providers.lineups ? providers.lineups.rows ?? 0 : 0],
    ["Injury rows", providers.injuries ? providers.injuries.rows ?? 0 : 0],
    ["Official report rows", providers.injuries ? providers.injuries.official_report_count ?? 0 : 0],
    ["Last error", state.last_error || "None"],
  ];
  items.forEach(([label, value]) => {
    const block = document.createElement("div");
    block.className = "live-stat";
    block.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
    stats.appendChild(block);
  });
  shell.appendChild(stats);
}

function renderCoverage(status) {
  const coverage = status.model_coverage || {};
  const recheck = status.recheck || {};
  const benchmark = status.rotowire_benchmark || {};
  const benchmarkOverall = benchmark.overall || {};
  const calibrationPayload = benchmark.calibration || {};
  const calibrationMaeFallback = Array.isArray(calibrationPayload)
    ? Math.abs(
        (
          calibrationPayload.reduce((acc, item) => acc + Number(item?.hit_rate || 0), 0) /
          Math.max(calibrationPayload.length, 1)
        ) - 0.5
      )
    : null;
  const overallRecheck = recheck.overall || {};
  const perTarget = recheck.per_target || {};
  const roleSplits = recheck.role_splits || {};
  const asFixed = (value, digits = 2) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return null;
    return Number(numeric.toFixed(digits));
  };

  const recheckPerTarget = {};
  Object.keys(perTarget).forEach((target) => {
    const metrics = perTarget[target] || {};
    recheckPerTarget[target] = {
      mean_absolute_error: asFixed(metrics.mean_absolute_error, 1),
      mean_abs_pct_error: asFixed(metrics.mean_abs_pct_error, 2),
      count: Number(metrics.count ?? 0),
    };
  });

  const payload = {
    training_rows: coverage.training_rows || 0,
    predicted_rows: coverage.predicted_rows || 0,
    season_priors_rows: coverage.season_priors_rows || 0,
    low_confidence_projection_count: status.low_confidence_projection_count || 0,
    recheck_evaluated_rows: coverage.recheck_evaluated_rows || 0,
    recheck_sample_rows: coverage.recheck_sample_rows || 0,
    recheck_lookback_days: coverage.recheck_lookback_days || null,
    recheck_overall_mean_abs_pct_error: asFixed(coverage.recheck_overall_mean_abs_pct_error, 2) || 0,
    recheck_per_target: recheckPerTarget,
    recheck_role_splits: roleSplits,
    recheck_period: {
      from: overallRecheck.from || null,
      to: overallRecheck.to || null,
    },
    rotowire_benchmark: {
      generated_at: benchmark.generated_at || null,
      rows_evaluated: Number(benchmark.rows_evaluated || 0),
      overall_projection_hit_rate: asFixed(benchmarkOverall?.projection_hit_rate ?? benchmark?.hit_rate, 3),
      overall_market_hit_rate: asFixed(benchmarkOverall?.market_hit_rate ?? 0.5, 3),
      overall_mean_abs_error: asFixed(benchmarkOverall?.mean_abs_error ?? benchmark?.model_projection_mae, 3),
      overall_mean_abs_pct_error: asFixed(benchmarkOverall?.mean_abs_pct_error, 3),
      calibration_mae: asFixed(calibrationPayload?.confidence_mae ?? calibrationMaeFallback, 3),
    },
  };
  setText("coverage-output", prettyJSON(payload));
}

function toTitleFromSnake(value) {
  return String(value || "")
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function renderV05Rubric(status) {
  const shell = document.getElementById("v05-rubric-panel");
  if (!shell) return;

  const rubric = status?.v05_rubric || null;
  const actionPlan = status?.v05_action_plan || null;
  if (!rubric || typeof rubric !== "object") {
    shell.innerHTML = `<p class="v05-rubric-notes">v0.5 rubric is not available yet. Refresh status after running a prediction cycle.</p>`;
    return;
  }

  const overallScore = Number(rubric.overall_score || 0);
  const overallGrade = String(rubric.grade || "unknown");
  const rowsScored = Number(rubric.rows_scored || 0);
  const dimensions = Object.entries(rubric.dimensions || {}).sort(
    (a, b) => Number((b[1] || {}).score || 0) - Number((a[1] || {}).score || 0)
  );
  const recommendations = Array.isArray(rubric.recommendations) ? rubric.recommendations : [];
  const topRecommendations = recommendations.slice(0, 3);
  const v2Step = Array.isArray(actionPlan?.v2_execution_plan) && actionPlan.v2_execution_plan.length
    ? actionPlan.v2_execution_plan[0]?.name || null
    : null;
  const sourceFeasibility = Array.isArray(actionPlan?.source_feasibility)
    ? actionPlan.source_feasibility.slice(0, 4)
    : [];
  const manualChatSteps = Array.isArray(actionPlan?.manual_chatbox_steps)
    ? actionPlan.manual_chatbox_steps.slice(0, 3)
    : [];
  const accuracyGate = actionPlan?.accuracy_gate_diagnostic || null;
  const coverageGate = actionPlan?.coverage_gate_diagnostic || null;

  const dimensionHTML = dimensions
    .map(([key, details]) => {
      const score = Number(details?.score || 0).toFixed(1);
      const statusLabel = String(details?.status || "unknown");
      return `
        <div class="v05-rubric-item ${escapeHTML(statusLabel)}">
          <strong>${escapeHTML(toTitleFromSnake(key))}</strong>
          <span>Score: ${escapeHTML(score)}</span>
          <span>Status: ${escapeHTML(statusLabel.replace(/_/g, " "))}</span>
        </div>
      `;
    })
    .join("");

  const recommendationText = topRecommendations.length
    ? topRecommendations
        .map((item, index) => `${index + 1}. ${item?.action || "Action required"}`)
        .join(" ")
    : "No high-priority gaps detected in the current v0.5 pass.";
  const sourceText = sourceFeasibility.length
    ? sourceFeasibility
        .map((item) => {
          const mode = item?.mode ? ` (${item.mode})` : "";
          const status = item?.status ? `: ${item.status}` : "";
          const rows = Number.isFinite(Number(item?.rows_last_sync))
            ? ` [rows ${Number(item.rows_last_sync)}]`
            : "";
          return `${item?.source || "source"}${mode}${status}${rows}`;
        })
        .join(" | ")
    : "Source feasibility details will appear after live sync.";
  const manualText = manualChatSteps.length
    ? manualChatSteps
        .map((item) => `${item?.step || "?"}. ${item?.what_to_send || "Provide manual data input."}`)
        .join(" ")
    : "Manual chatbox fallback steps are not available yet.";
  const gateText = [
    accuracyGate ? `Accuracy gate ${Number(accuracyGate.score || 0).toFixed(1)} (${String(accuracyGate.status || "unknown")})` : null,
    coverageGate ? `Coverage gate ${Number(coverageGate.score || 0).toFixed(1)} (${String(coverageGate.status || "unknown")})` : null,
  ].filter(Boolean).join(" · ");

  shell.innerHTML = `
    <div class="v05-rubric-head">
      <h3>v0.5 Rubric</h3>
      <div class="score">Overall ${escapeHTML(overallScore.toFixed(1))} (${escapeHTML(overallGrade)}) · Rows ${escapeHTML(String(rowsScored))}</div>
    </div>
    <div class="v05-rubric-grid">
      ${dimensionHTML || `<div class="v05-rubric-item"><strong>No dimensions</strong><span>Run prediction + status refresh.</span></div>`}
    </div>
    <p class="v05-rubric-notes">${escapeHTML(recommendationText)}</p>
    ${v2Step ? `<p class="v05-rubric-notes"><strong>Next v2 step:</strong> ${escapeHTML(v2Step)}</p>` : ""}
    ${gateText ? `<p class="v05-rubric-notes"><strong>Gate diagnostics:</strong> ${escapeHTML(gateText)}</p>` : ""}
    <p class="v05-rubric-notes"><strong>Source feasibility:</strong> ${escapeHTML(sourceText)}</p>
    <p class="v05-rubric-notes"><strong>Manual fallback (chatbox):</strong> ${escapeHTML(manualText)}</p>
  `;
}

function sourceRowsByKeyword(payload, keyword) {
  const sources = Array.isArray(payload?.sources) ? payload.sources : [];
  const match = sources.find((source) =>
    String(source?.name || "").toLowerCase().includes(String(keyword || "").toLowerCase())
  );
  const rows = Number(match?.rows ?? 0);
  return Number.isFinite(rows) ? rows : 0;
}

function renderCompetitiveGapPanel(payload) {
  const shell = document.getElementById("competitive-gap-panel");
  if (!shell) return;
  shell.innerHTML = "";

  const summary = payload?.summary || null;
  if (!summary) {
    shell.innerHTML = `<div class="gap-empty">Competitive gap assessment will appear after the board loads.</div>`;
    return;
  }

  const totalPlayers = Math.max(1, Number(summary.total_players || 0));
  const actionableRate = Number(summary.actionable_players || 0) / totalPlayers;
  const highConfRate = Number(summary.high_confidence_under_10pct || 0) / totalPlayers;
  const noBetRate = Number(summary.no_bet_players || 0) / totalPlayers;
  const starterRate = Number(summary.starters || 0) / totalPlayers;

  const status = latestStatus || {};
  const benchmark = status?.rotowire_benchmark || {};
  const benchmarkOverall = benchmark?.overall || {};
  const modelCoverage = status?.model_coverage || {};
  const liveState = status?.live_sync?.state || {};
  const liveRunning = Boolean(status?.live_sync?.running);

  const projectionHitRate = Number(
    benchmarkOverall.projection_hit_rate ?? benchmark.hit_rate ?? 0
  ) * 100;
  const marketHitRate = Number(benchmarkOverall.market_hit_rate ?? 0.5) * 100;
  const mape = Number(modelCoverage.recheck_overall_mean_abs_pct_error);
  const calibrationGap = projectionHitRate - marketHitRate;

  const lineupRows = sourceRowsByKeyword(latestLiveSourcesPayload, "lineup");
  const injuryRows = sourceRowsByKeyword(latestLiveSourcesPayload, "injur");
  const oddsRows = sourceRowsByKeyword(latestLiveSourcesPayload, "odds");

  const crossReference = payload?.cross_reference || {};
  const matchedPlayers = Number(crossReference.matched_players || 0);
  const lineRows = Number(crossReference.lines_rows || 0);

  const minutesCertaintyScore = clamp(
    starterRate * 35
      + highConfRate * 40
      + (lineupRows > 0 ? 25 : 8),
    0,
    100
  );
  const marketCalibrationScore = clamp(
    (Number.isFinite(mape) ? (100 - mape * 2.4) : 50)
      + calibrationGap * 0.8
      + (lineRows > 0 ? 6 : -8)
      + (oddsRows > 0 ? 4 : 0),
    0,
    100
  );
  const coverageScore = clamp(
    (Math.min(1, totalPlayers / 320) * 45)
      + (Math.min(1, matchedPlayers / 120) * 25)
      + (injuryRows > 0 ? 12 : 0)
      + (lineupRows > 0 ? 10 : 0)
      + (oddsRows > 0 ? 8 : 0),
    0,
    100
  );
  const reliabilityScore = clamp(
    (liveRunning ? 55 : 15)
      + (liveState.last_error ? -18 : 12)
      + (liveState.last_sync_at ? 12 : 0)
      + (liveState.last_projection_refresh_at ? 11 : 0),
    0,
    100
  );
  const productReadinessScore = clamp(
    58
      + (actionableRate >= 0.15 ? 10 : 0)
      + (noBetRate <= 0.55 ? 10 : 0)
      + (totalPlayers >= 240 ? 12 : totalPlayers >= 160 ? 6 : 0),
    0,
    100
  );

  const dimensions = [
    {
      name: "Pregame Minutes Certainty",
      score: minutesCertaintyScore,
      target: ">= 80",
      appAction: "Improve lock-window starter certainty (T-90/T-30/T-5) and last-minute lineup ingestion.",
      userAction: "Feed confirmed starters/rotation notes before lock for thin teams and late injury situations.",
    },
    {
      name: "Market Calibration",
      score: marketCalibrationScore,
      target: ">= 78",
      appAction: "Increase per-market calibration and open-to-close line movement anchoring by market.",
      userAction: "Keep recent, same-day line imports current so the anchor stays aligned to real market state.",
    },
    {
      name: "Coverage Depth",
      score: coverageScore,
      target: ">= 82",
      appAction: "Backfill broader player pool and maintain full injury/odds/lineup context joins each cycle.",
      userAction: "Prioritize complete historical game logs and keep season priors fresh when rosters change.",
    },
    {
      name: "Runtime Reliability",
      score: reliabilityScore,
      target: ">= 90",
      appAction: "Harden live loops and retry logic so sync stays continuously active without manual restarts.",
      userAction: "Keep API keys funded/active and validate provider quotas to avoid silent data stalls.",
    },
    {
      name: "Product Readiness",
      score: productReadinessScore,
      target: ">= 85",
      appAction: "Tighten no-bet explanations, simplify default board, and keep top actionable cards front-loaded.",
      userAction: "Define pricing tier + onboarding copy and track conversion from free board to paid features.",
    },
  ];

  const overallScore = average(dimensions.map((dimension) => dimension.score));
  const overallTier = scoreTier(overallScore);
  const boardDate = payload?.board_date || "current slate";

  const cardsHtml = dimensions
    .map((dimension) => {
      const score = Number(dimension.score || 0);
      const tier = scoreTier(score);
      return `
        <article class="gap-card ${escapeHTML(tier)}">
          <div class="gap-card-head">
            <h4>${escapeHTML(dimension.name)}</h4>
            <span class="gap-score">${escapeHTML(formatNumber(score, 1))}</span>
          </div>
          <div class="gap-meter">
            <span style="width:${escapeHTML(formatNumber(clamp(score, 0, 100), 1))}%"></span>
          </div>
          <p class="gap-target">Target: ${escapeHTML(dimension.target)}</p>
          <p class="gap-action"><strong>App:</strong> ${escapeHTML(dimension.appAction)}</p>
          <p class="gap-action"><strong>You:</strong> ${escapeHTML(dimension.userAction)}</p>
        </article>
      `;
    })
    .join("");

  shell.innerHTML = `
    <div class="board-subsection-head">
      <h3>Competitive Gap Assessment (${escapeHTML(boardDate)})</h3>
      <p>Live parity score versus high-end fantasy projection products, with concrete actions by owner.
      <a href="https://www.rotowire.com/basketball/nba-lineups.php" target="_blank" rel="noreferrer noopener">RotoWire Lineups</a> |
      <a href="https://www.rotowire.com/picks/prizepicks/" target="_blank" rel="noreferrer noopener">RotoWire PrizePicks</a> |
      <a href="https://www.betr.app/" target="_blank" rel="noreferrer noopener">Betr</a></p>
    </div>
    <div class="gap-overview ${escapeHTML(overallTier)}">
      <span>Overall parity score</span>
      <strong>${escapeHTML(formatNumber(overallScore, 1))} / 100</strong>
      <span>Benchmark rows: ${escapeHTML(benchmark.rows_evaluated || 0)} | Projection hit: ${escapeHTML(formatPct(projectionHitRate))} | Market hit: ${escapeHTML(formatPct(marketHitRate))}</span>
    </div>
    <div class="gap-grid">${cardsHtml}</div>
  `;
}

function renderMonetizationRoadmap(payload) {
  const shell = document.getElementById("monetization-roadmap");
  if (!shell) return;
  shell.innerHTML = "";

  const summary = payload?.summary || null;
  if (!summary) {
    shell.innerHTML = `<div class="gap-empty">Monetization roadmap will appear after board data is loaded.</div>`;
    return;
  }

  const status = latestStatus || {};
  const coverage = status.model_coverage || {};
  const benchmarkRows = Number(status?.rotowire_benchmark?.rows_evaluated || 0);
  const liveRunning = Boolean(status?.live_sync?.running);
  const recheckError = Number(coverage.recheck_overall_mean_abs_pct_error);

  const steps = [
    {
      name: "Step 1: Reliability Gate",
      done: liveRunning && !status?.live_sync?.state?.last_error,
      detail: "Live sync must stay running with no current runtime errors.",
    },
    {
      name: "Step 2: Accuracy Gate",
      done: Number.isFinite(recheckError) && recheckError <= 28,
      detail: "Recheck overall error must be consistently controlled before broad paid rollout.",
    },
    {
      name: "Step 3: Coverage Gate",
      done: Number(summary.total_players || 0) >= 240 && Number(summary.starters || 0) >= 45,
      detail: "Slate coverage should include deep rotation players and starter-heavy visibility.",
    },
    {
      name: "Step 4: Benchmark Gate",
      done: benchmarkRows >= 400,
      detail: "Need benchmark sample depth so confidence labels are statistically trustworthy.",
    },
    {
      name: "Step 5: Paid Layer",
      done: false,
      detail: "Add account/paywall/events instrumentation and track conversion from free to paid picks.",
    },
  ];

  const stepsHtml = steps
    .map((step) => `
      <article class="roadmap-step ${step.done ? "done" : "pending"}">
        <strong>${escapeHTML(step.name)}</strong>
        <span>${escapeHTML(step.done ? "Ready" : "Pending")}</span>
        <p>${escapeHTML(step.detail)}</p>
      </article>
    `)
    .join("");

  shell.innerHTML = `
    <div class="board-subsection-head">
      <h3>Monetization Readiness Roadmap</h3>
      <p>Release gates to hit before paid launch.</p>
    </div>
    <div class="roadmap-grid">${stepsHtml}</div>
  `;
}

function formatNumber(value, digits = 1) {
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return "-";
  return numeric.toFixed(digits);
}

function formatPct(value) {
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return "-";
  return `${numeric.toFixed(1)}%`;
}

function formatSigned(value, digits = 2) {
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return "-";
  const rounded = numeric.toFixed(digits);
  return numeric > 0 ? `+${rounded}` : rounded;
}

function clamp(value, min, max) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return min;
  return Math.min(max, Math.max(min, numeric));
}

function average(values) {
  const valid = values.filter((value) => Number.isFinite(Number(value)));
  if (!valid.length) return 0;
  return valid.reduce((sum, value) => sum + Number(value), 0) / valid.length;
}

function scoreTier(score) {
  const numeric = Number(score);
  if (numeric >= 82) return "strong";
  if (numeric >= 65) return "medium";
  return "weak";
}

function formatDateTime(value) {
  if (!value) return "n/a";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function computeBestEdge(player) {
  const markets = Array.isArray(player.market_references) ? player.market_references : [];
  if (!markets.length) return Number.NEGATIVE_INFINITY;
  let best = Number.NEGATIVE_INFINITY;
  markets.forEach((market) => {
    const edge = Math.abs(Number(market.edge));
    if (Number.isFinite(edge) && edge > best) best = edge;
  });
  return best;
}

function renderBoardLegend() {
  const legend = document.getElementById("board-legend");
  legend.innerHTML = `
    <div class="legend-item"><strong>Actionable</strong><span>Guardrails passed; still verify lineup/injury context.</span></div>
    <div class="legend-item"><strong>No Bet</strong><span>Risk guardrails triggered (minutes, injury/news, or volatility).</span></div>
    <div class="legend-item"><strong>Error %</strong><span>Lower is better. Confidence is derived from historical error behavior.</span></div>
  `;
}

function renderBoardSummary(payload) {
  const summaryShell = document.getElementById("board-summary");
  const crossRefShell = document.getElementById("board-cross-reference");
  const freshnessShell = document.getElementById("board-freshness");
  const gapShell = document.getElementById("competitive-gap-panel");
  const roadmapShell = document.getElementById("monetization-roadmap");
  summaryShell.innerHTML = "";

  if (!payload || !payload.summary) {
    crossRefShell.textContent = "Projection board is not available yet.";
    freshnessShell.textContent = "";
    if (gapShell) gapShell.innerHTML = "";
    if (roadmapShell) roadmapShell.innerHTML = "";
    return;
  }

  const summary = payload.summary;
  const liveState = latestStatus?.live_sync?.state || {};
  const generatedAt = payload.generated_at || null;
  const liveRefreshAt = liveState.last_in_game_projection_refresh_at || liveState.last_projection_refresh_at || null;
  freshnessShell.innerHTML = `
    <div class="fresh-pill">Board generated: <strong>${escapeHTML(formatDateTime(generatedAt))}</strong></div>
    <div class="fresh-pill">Last model refresh: <strong>${escapeHTML(formatDateTime(liveRefreshAt))}</strong></div>
    <div class="fresh-pill">Watchlist: <strong>${watchlist.size}</strong> players</div>
  `;
  const topTiles = [
    ["Board Date", payload.board_date || "N/A"],
    ["Total Players", summary.total_players ?? 0],
    ["Playable", summary.playable_players ?? 0],
    ["Non-Playable", summary.non_playable_players ?? 0],
    ["Actionable", summary.actionable_players ?? 0],
    ["No Bet", summary.no_bet_players ?? 0],
    ["Starters", summary.starters ?? 0],
    ["Likely Non-Starters", summary.likely_non_starters ?? 0],
    ["High Confidence <=10%", summary.high_confidence_under_10pct ?? 0],
    ["Elite <=5%", summary.elite_confidence_under_5pct ?? 0],
    ["High Error >10%", summary.over_10pct_error ?? 0],
    ["Popular High Confidence", summary.popular_high_confidence ?? 0],
  ];

  topTiles.forEach(([label, value]) => {
    const tile = document.createElement("div");
    tile.className = "board-summary-tile";
    tile.innerHTML = `<span>${escapeHTML(label)}</span><strong>${escapeHTML(value)}</strong>`;
    summaryShell.appendChild(tile);
  });

  const crossRef = payload.cross_reference || {};
  crossRefShell.innerHTML = `
    <div class="crossref-pill ${escapeHTML(crossRef.status || "unknown")}">
      <strong>PrizePicks Cross-Reference:</strong>
      <span>${escapeHTML(crossRef.message || "No status available.")}</span>
      <span>Lines Rows: ${escapeHTML(crossRef.lines_rows ?? 0)} | Matched Players: ${escapeHTML(crossRef.matched_players ?? 0)}</span>
    </div>
  `;
  renderCompetitiveGapPanel(payload);
  renderMonetizationRoadmap(payload);
  renderBoardLegend();
  renderTopEdges(payload.cards || [], payload.board_date || "N/A");
  renderGameHub(payload.cards || [], payload.board_date || "N/A");
}

function renderTopEdges(cards, boardDate) {
  const shell = document.getElementById("board-top-edges");
  if (!shell) return;
  shell.innerHTML = "";

  if (!Array.isArray(cards) || !cards.length) {
    shell.textContent = "No edge rows available.";
    return;
  }

  const rows = [];
  cards.forEach((card) => {
    const marketReferences = Array.isArray(card.market_references) ? card.market_references : [];
    marketReferences.forEach((market) => {
      const edge = Number(market.edge);
      const line = Number(market.line);
      const projection = Number(market.projection);
      const recommendation = String(market.recommendation || "Pass");
      if (!Number.isFinite(edge) || !Number.isFinite(line) || !Number.isFinite(projection)) return;
      rows.push({
        player_name: card.player_name,
        team: card.team,
        opponent: card.opponent,
        market: String(market.market || "").toUpperCase(),
        line,
        projection,
        edge,
        recommendation,
        signal: recommendation === "Pass"
          ? "Pass"
          : Math.abs(edge) >= 2.5
            ? "Boost"
            : Math.abs(edge) >= 1.2
              ? "Edge"
              : "Anchor",
        confidence_pct: Number(card.confidence_pct || 0),
        error_pct_estimate: Number(card.error_pct_estimate || 0),
        actionable: Boolean(card.is_actionable),
      });
    });
  });

  const actionable = rows.filter((row) => row.actionable && row.recommendation !== "Pass");
  const ordered = (actionable.length ? actionable : rows)
    .sort((a, b) => Math.abs(b.edge) - Math.abs(a.edge))
    .slice(0, 24);

  if (!ordered.length) {
    shell.textContent = "No market rows available.";
    return;
  }

  const tableRows = ordered
    .map(
      (row) => `
      <tr>
        <td>${escapeHTML(row.player_name)}</td>
        <td>${escapeHTML(row.team)} ${escapeHTML(row.opponent ? `vs ${row.opponent}` : "")}</td>
        <td>${escapeHTML(row.market)}</td>
        <td>${escapeHTML(formatNumber(row.line, 2))}</td>
        <td>${escapeHTML(formatNumber(row.projection, 2))}</td>
        <td>${escapeHTML(formatSigned(row.edge, 2))}</td>
        <td>${escapeHTML(row.recommendation)}</td>
        <td>${escapeHTML(row.signal)}</td>
        <td>${escapeHTML(formatPct(row.confidence_pct))}</td>
        <td>${escapeHTML(formatPct(row.error_pct_estimate))}</td>
      </tr>
    `
    )
    .join("");

  shell.innerHTML = `
    <div class="board-subsection-head">
      <h3>Top Market Edges (${escapeHTML(boardDate)})</h3>
      <p>Best actionable market deltas after confidence/no-bet guardrails.</p>
    </div>
    <div class="top-edges-table-wrap">
      <table class="top-edges-table">
        <thead>
          <tr>
            <th>Player</th>
            <th>Game</th>
            <th>Market</th>
            <th>Line</th>
            <th>Proj</th>
            <th>Edge</th>
            <th>Pick</th>
            <th>Signal</th>
            <th>Conf</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>${tableRows}</tbody>
      </table>
    </div>
  `;
}

function renderGameHub(cards, boardDate) {
  const shell = document.getElementById("board-game-hub");
  if (!shell) return;
  shell.innerHTML = "";

  if (!Array.isArray(cards) || !cards.length) {
    shell.textContent = "No game hub rows available.";
    return;
  }

  const grouped = new Map();
  cards.forEach((card) => {
    const gameKey = `${card.game_date || boardDate}|${card.team || ""}|${card.opponent || ""}|${card.home ? "home" : "away"}`;
    if (!grouped.has(gameKey)) {
      grouped.set(gameKey, {
        game_date: card.game_date || boardDate,
        team: card.team || "",
        opponent: card.opponent || "",
        home: Boolean(card.home),
        starters: 0,
        actionable: 0,
        no_bet: 0,
        players: 0,
        confidence_total: 0,
      });
    }
    const bucket = grouped.get(gameKey);
    bucket.players += 1;
    bucket.starters += card.is_starter ? 1 : 0;
    bucket.actionable += card.is_actionable ? 1 : 0;
    bucket.no_bet += card.no_bet ? 1 : 0;
    bucket.confidence_total += Number(card.confidence_pct || 0);
  });

  const rows = [...grouped.values()]
    .map((row) => ({
      ...row,
      avg_confidence: row.players ? row.confidence_total / row.players : 0,
    }))
    .sort((a, b) => b.avg_confidence - a.avg_confidence)
    .slice(0, 16);

  if (!rows.length) {
    shell.textContent = "No grouped games available.";
    return;
  }

  const scoreboardDate = String(boardDate || "").replaceAll("-", "");
  const scoreboardHref = scoreboardDate
    ? `https://www.espn.com/nba/scoreboard/_/date/${scoreboardDate}`
    : "https://www.espn.com/nba/scoreboard";
  const cardsHtml = rows
    .map(
      (row) => `
      <article class="game-hub-card">
        <h4>${escapeHTML(row.team)} ${row.home ? "vs" : "@"} ${escapeHTML(row.opponent)}</h4>
        <p>${escapeHTML(row.game_date)}</p>
        <div class="game-hub-metrics">
          <span>Players <strong>${escapeHTML(row.players)}</strong></span>
          <span>Starters <strong>${escapeHTML(row.starters)}</strong></span>
          <span>Actionable <strong>${escapeHTML(row.actionable)}</strong></span>
          <span>No Bet <strong>${escapeHTML(row.no_bet)}</strong></span>
          <span>Avg Conf <strong>${escapeHTML(formatPct(row.avg_confidence))}</strong></span>
        </div>
      </article>
    `
    )
    .join("");

  shell.innerHTML = `
    <div class="board-subsection-head">
      <h3>Game Hub (${escapeHTML(boardDate)})</h3>
      <p>Starter/actionability snapshots by game context.
      <a href="${escapeHTML(scoreboardHref)}" target="_blank" rel="noreferrer noopener">Open scoreboard</a> |
      <a href="https://www.rotowire.com/basketball/nba-lineups.php" target="_blank" rel="noreferrer noopener">Open lineups</a></p>
    </div>
    <div class="game-hub-grid">${cardsHtml}</div>
  `;
}

function populateBoardDateSelect(payload) {
  const select = document.getElementById("board-date-select");
  if (!select) return;
  const previous = select.value;
  const availableDates = payload?.available_dates || [];
  select.innerHTML = "";

  availableDates.forEach((date) => {
    const option = document.createElement("option");
    option.value = date;
    option.textContent = date === "all" ? "ALL DATES (Max Coverage)" : date;
    select.appendChild(option);
  });

  if (availableDates.includes(previous)) {
    select.value = previous;
  } else if (payload?.board_date && availableDates.includes(payload.board_date)) {
    select.value = payload.board_date;
  } else if (availableDates.length) {
    select.value = availableDates[0];
  }

  populateFridayDateSelect(availableDates, select.value || payload?.board_date || availableDates[0] || "");
}

function populateFridayDateSelect(availableDates, preferredValue) {
  const select = document.getElementById("friday-board-date");
  if (!select) return;
  const previous = select.value;
  select.innerHTML = "";

  (availableDates || []).forEach((date) => {
    const option = document.createElement("option");
    option.value = date;
    option.textContent = date === "all" ? "ALL DATES" : date;
    select.appendChild(option);
  });

  if ((availableDates || []).includes(previous)) {
    select.value = previous;
    return;
  }
  if ((availableDates || []).includes(preferredValue)) {
    select.value = preferredValue;
    return;
  }
  if ((availableDates || []).length) {
    select.value = availableDates[0];
  }
}

function buildMarketRows(marketReferences) {
  if (!marketReferences || !marketReferences.length) {
    return `<div class="card-note">No imported PrizePicks lines for this player/date.</div>`;
  }
  return marketReferences
    .map((market) => {
      const recommendationClass = String(market.recommendation || "").toLowerCase();
      const edgeValue = Math.abs(Number(market.edge || 0));
      const signalTier = recommendationClass === "pass"
        ? "pass"
        : edgeValue >= 2.5
          ? "boost"
          : edgeValue >= 1.2
            ? "edge"
            : "anchor";
      return `
        <div class="market-row">
          <span class="market-name">${escapeHTML(String(market.market || "").toUpperCase())}</span>
          <span>L: ${escapeHTML(formatNumber(market.line, 2))}</span>
          <span>P: ${escapeHTML(formatNumber(market.projection, 2))}</span>
          <span>E: ${escapeHTML(formatNumber(market.edge, 2))}</span>
          <span class="market-rec ${escapeHTML(recommendationClass)}">${escapeHTML(market.recommendation || "Pass")}</span>
          <span class="market-rec ${escapeHTML(signalTier)}">${escapeHTML(signalTier)}</span>
        </div>
      `;
    })
    .join("");
}

function buildConfidenceDecompositionRows(decomposition) {
  if (!decomposition || typeof decomposition !== "object") {
    return `<div class="card-note">Confidence decomposition unavailable.</div>`;
  }
  const orderedMarkets = ["points", "rebounds", "assists", "pra"];
  const labels = { points: "PTS", rebounds: "REB", assists: "AST", pra: "PRA" };
  return orderedMarkets
    .filter((market) => decomposition[market])
    .map((market) => {
      const item = decomposition[market];
      return `
        <div class="market-row">
          <span class="market-name">${labels[market]}</span>
          <span>Score ${escapeHTML(formatPct(item.score_pct))}</span>
          <span>Min ${escapeHTML(formatPct(Number(item.minutes || 0) * 100))}</span>
          <span>Lineup ${escapeHTML(formatPct(Number(item.lineup || 0) * 100))}</span>
          <span>Lock ${escapeHTML(formatPct(Number(item.lock || 0) * 100))}</span>
          <span>Market ${escapeHTML(formatPct(Number(item.market_data || 0) * 100))}</span>
          <span>Model ${escapeHTML(formatPct(Number(item.model || 0) * 100))}</span>
        </div>
      `;
    })
    .join("");
}

function buildScenarioRows(scenarioCards) {
  if (!scenarioCards || typeof scenarioCards !== "object") {
    return `<div class="card-note">Scenario cards unavailable.</div>`;
  }
  const lineupConditional = scenarioCards.lineup_conditional || {};
  const teammate = scenarioCards.teammate_in_out || {};
  const orderedMarkets = ["points", "rebounds", "assists", "pra"];
  const labels = { points: "PTS", rebounds: "REB", assists: "AST", pra: "PRA" };
  return orderedMarkets
    .map((market) => {
      const lineup = lineupConditional[market] || {};
      const mates = teammate[market] || {};
      return `
        <div class="market-row">
          <span class="market-name">${labels[market]}</span>
          <span>Starter ${escapeHTML(formatNumber(lineup.as_starter, 2))}</span>
          <span>Bench ${escapeHTML(formatNumber(lineup.as_non_starter, 2))}</span>
          <span>Mate Out ${escapeHTML(formatNumber(mates.if_out, 2))}</span>
          <span>Mate In ${escapeHTML(formatNumber(mates.if_in, 2))}</span>
        </div>
      `;
    })
    .join("");
}

function renderPlayerCard(player) {
  const card = document.createElement("article");
  const key = playerKey(player);
  const inWatchlist = watchlist.has(key);
  const bestEdge = computeBestEdge(player);
  const starterClass = player.is_starter ? "starter" : "bench";
  const playableClass = player.is_playable ? "playable" : "non-playable";
  const noBetClass = player.no_bet ? "no-bet" : "actionable";
  const confidenceClass = String(player.error_band || "high_error");
  const popularBadge = player.popular_high_confidence && !player.no_bet ? `<span class="badge popular">Popular HC</span>` : "";
  const noBetBadge = player.no_bet ? `<span class="badge no-bet">NO BET</span>` : `<span class="badge actionable">Actionable</span>`;
  const liveBadge = player.live_projection_in_game_flag ? `<span class="badge live">LIVE</span>` : "";
  const watchBadge = inWatchlist ? `<span class="badge watchlist">Watchlist</span>` : "";
  const playableBadge = `<span class="badge ${escapeHTML(playableClass)}">${player.is_playable ? "Playable" : "Not Playable"}</span>`;
  const starterBadge = `<span class="badge ${escapeHTML(starterClass)}">${escapeHTML(player.start_label)}</span>`;
  const confidenceBadge = `<span class="badge ${escapeHTML(confidenceClass)}">Error ${escapeHTML(formatPct(player.error_pct_estimate))}</span>`;
  const qualityBadge = player.prediction_quality_blocked
    ? `<span class="badge no-bet">Quality Blocked</span>`
    : `<span class="badge actionable">Quality ${escapeHTML(formatPct(Number(player.prediction_quality_score || 0) * 100))}</span>`;
  const shortReason = player.no_bet
    ? `Pass reason: ${player.no_bet_reason_text || "risk guardrail triggered"}`
    : `Best edge: ${Number.isFinite(bestEdge) ? formatNumber(bestEdge, 2) : "-"} | Confidence ${formatPct(player.confidence_pct)}`;
  const confidenceDecompositionHtml = buildConfidenceDecompositionRows(player.confidence_decomposition);
  const scenarioRowsHtml = buildScenarioRows(player.scenario_cards);
  const lockWindowStage = String(player.pregame_lock_window_stage || "unknown").replace(/_/g, " ");
  const lockWindowMinutes = Number.isFinite(Number(player.pregame_lock_minutes_to_tipoff))
    ? formatNumber(player.pregame_lock_minutes_to_tipoff, 1)
    : "n/a";

  card.className = `player-prop-card ${starterClass} ${playableClass} ${confidenceClass} ${noBetClass}`;
  card.innerHTML = `
    <div class="player-card-toolbar">
      <button type="button" class="watch-toggle ${inWatchlist ? "active" : ""}" aria-pressed="${inWatchlist ? "true" : "false"}" title="${inWatchlist ? "Remove from watchlist" : "Add to watchlist"}">
        ${inWatchlist ? "★ Watchlist" : "☆ Add Watch"}
      </button>
      <span class="mini-metric">Starter Prob ${escapeHTML(formatPct(Number(player.starter_probability || 0) * 100))} | Lock ${escapeHTML(formatPct(Number(player.pregame_lock_confidence || 0) * 100))}</span>
    </div>
    <button class="player-prop-card-button" type="button">
      <div class="player-card-top">
        <div>
          <h3>${escapeHTML(player.player_name)}</h3>
          <p>${escapeHTML(player.team)} ${player.home ? "vs" : "@"} ${escapeHTML(player.opponent)} ${player.position ? `| ${escapeHTML(player.position)}` : ""}</p>
        </div>
        <div class="badges">
          ${liveBadge}
          ${noBetBadge}
          ${playableBadge}
          ${starterBadge}
          ${confidenceBadge}
          ${qualityBadge}
          ${popularBadge}
          ${watchBadge}
        </div>
      </div>
      <div class="quick-stats">
        <span>PTS <strong>${escapeHTML(formatNumber(player.projected_points, 1))}</strong></span>
        <span>REB <strong>${escapeHTML(formatNumber(player.projected_rebounds, 1))}</strong></span>
        <span>AST <strong>${escapeHTML(formatNumber(player.projected_assists, 1))}</strong></span>
        <span>PRA <strong>${escapeHTML(formatNumber(player.projected_pra, 1))}</strong></span>
        <span>DK <strong>${escapeHTML(formatNumber(player.projected_draftkings_points, 1))}</strong></span>
        <span>FD <strong>${escapeHTML(formatNumber(player.projected_fanduel_points, 1))}</strong></span>
      </div>
      <div class="card-subline">
        Confidence ${escapeHTML(formatPct(player.confidence_pct))} | Model conf ${escapeHTML(formatPct(player.projection_confidence_pct || player.confidence_pct))} | Historical games ${escapeHTML(player.historical_games_used)} | Starter prob ${escapeHTML(formatPct(Number(player.starter_probability || 0) * 100))} | Lock ${escapeHTML(formatPct(Number(player.pregame_lock_confidence || 0) * 100))} (${escapeHTML(player.pregame_lock_tier || "n/a")}) | Lock window ${escapeHTML(lockWindowStage)} (${escapeHTML(lockWindowMinutes)}m) | Minutes conf ${escapeHTML(formatPct(Number(player.expected_minutes_confidence || 0) * 100))}
      </div>
      <div class="card-note reason-note">${escapeHTML(shortReason)}</div>
      ${
        player.prediction_quality_blocked
          ? `<div class="card-note no-bet-note">Prediction quality gate blocked this row: ${escapeHTML(player.prediction_quality_issues || "missing_core_context")}</div>`
          : ""
      }
      ${
        player.live_projection_in_game_flag
          ? `<div class="card-note live-note">Live game: ${escapeHTML(formatNumber(player.live_minutes_played, 1))} min played | Current PTS/REB/AST ${escapeHTML(formatNumber(player.live_points_current, 0))}/${escapeHTML(formatNumber(player.live_rebounds_current, 0))}/${escapeHTML(formatNumber(player.live_assists_current, 0))} | Updated ${escapeHTML(player.live_projection_updated_at || "")}</div>`
          : ""
      }
      ${
        player.no_bet
          ? `<div class="card-note no-bet-note">No-bet score ${escapeHTML(formatNumber(player.no_bet_score, 2))}: ${escapeHTML(player.no_bet_reason_text || "risk_guardrails")}</div>`
          : ""
      }
      <div class="card-detail">
        <div class="detail-grid anchor-grid">
          <span>Anchor PTS <strong>${escapeHTML(formatNumber(player.pregame_anchor_points, 2))}</strong> (${escapeHTML(formatSigned(player.pregame_anchor_gap_points, 2))})</span>
          <span>Anchor REB <strong>${escapeHTML(formatNumber(player.pregame_anchor_rebounds, 2))}</strong> (${escapeHTML(formatSigned(player.pregame_anchor_gap_rebounds, 2))})</span>
          <span>Anchor AST <strong>${escapeHTML(formatNumber(player.pregame_anchor_assists, 2))}</strong> (${escapeHTML(formatSigned(player.pregame_anchor_gap_assists, 2))})</span>
          <span>Anchor PRA <strong>${escapeHTML(formatNumber(player.pregame_anchor_pra, 2))}</strong> (${escapeHTML(formatSigned(player.pregame_anchor_gap_pra, 2))})</span>
        </div>
        <div class="detail-grid anchor-grid">
          <span>PTS Band (P10-P90) <strong>${escapeHTML(formatNumber(player.projected_points_p10, 1))} - ${escapeHTML(formatNumber(player.projected_points_p90, 1))}</strong></span>
          <span>REB Band (P10-P90) <strong>${escapeHTML(formatNumber(player.projected_rebounds_p10, 1))} - ${escapeHTML(formatNumber(player.projected_rebounds_p90, 1))}</strong></span>
          <span>AST Band (P10-P90) <strong>${escapeHTML(formatNumber(player.projected_assists_p10, 1))} - ${escapeHTML(formatNumber(player.projected_assists_p90, 1))}</strong></span>
          <span>PRA Band (P10-P90) <strong>${escapeHTML(formatNumber(player.projected_pra_p10, 1))} - ${escapeHTML(formatNumber(player.projected_pra_p90, 1))}</strong></span>
        </div>
        <div class="detail-grid">
          <span>Line PTS open/close/move <strong>${escapeHTML(formatNumber(player.line_points_open, 2))} / ${escapeHTML(formatNumber(player.line_points_close, 2))} / ${escapeHTML(formatSigned(player.line_points_movement, 2))}</strong></span>
          <span>Line REB open/close/move <strong>${escapeHTML(formatNumber(player.line_rebounds_open, 2))} / ${escapeHTML(formatNumber(player.line_rebounds_close, 2))} / ${escapeHTML(formatSigned(player.line_rebounds_movement, 2))}</strong></span>
          <span>Line AST open/close/move <strong>${escapeHTML(formatNumber(player.line_assists_open, 2))} / ${escapeHTML(formatNumber(player.line_assists_close, 2))} / ${escapeHTML(formatSigned(player.line_assists_movement, 2))}</strong></span>
          <span>Line PRA open/close/move <strong>${escapeHTML(formatNumber(player.line_pra_open, 2))} / ${escapeHTML(formatNumber(player.line_pra_close, 2))} / ${escapeHTML(formatSigned(player.line_pra_movement, 2))}</strong></span>
          <span>Line PTS <strong>${escapeHTML(formatNumber(player.line_points, 2))}</strong></span>
          <span>Line REB <strong>${escapeHTML(formatNumber(player.line_rebounds, 2))}</strong></span>
          <span>Line AST <strong>${escapeHTML(formatNumber(player.line_assists, 2))}</strong></span>
          <span>Line PRA <strong>${escapeHTML(formatNumber(player.line_pra, 2))}</strong></span>
          <span>Line 3PM <strong>${escapeHTML(formatNumber(player.line_three_points_made, 2))}</strong></span>
          <span>Line PR <strong>${escapeHTML(formatNumber(player.line_points_rebounds, 2))}</strong></span>
          <span>Line PA <strong>${escapeHTML(formatNumber(player.line_points_assists, 2))}</strong></span>
          <span>Line RA <strong>${escapeHTML(formatNumber(player.line_rebounds_assists, 2))}</strong></span>
          <span>Line STL <strong>${escapeHTML(formatNumber(player.line_steals, 2))}</strong></span>
          <span>Line BLK <strong>${escapeHTML(formatNumber(player.line_blocks, 2))}</strong></span>
          <span>Line TOV <strong>${escapeHTML(formatNumber(player.line_turnovers, 2))}</strong></span>
          <span>Line SB <strong>${escapeHTML(formatNumber(player.line_steals_blocks, 2))}</strong></span>
        </div>
        <div class="detail-grid">
          <span>Live MIN <strong>${escapeHTML(formatNumber(player.live_minutes_played, 2))}</strong></span>
          <span>Live PTS <strong>${escapeHTML(formatNumber(player.live_points_current, 2))}</strong></span>
          <span>Live REB <strong>${escapeHTML(formatNumber(player.live_rebounds_current, 2))}</strong></span>
          <span>Live AST <strong>${escapeHTML(formatNumber(player.live_assists_current, 2))}</strong></span>
          <span>Live STL <strong>${escapeHTML(formatNumber(player.live_steals_current, 2))}</strong></span>
          <span>Live BLK <strong>${escapeHTML(formatNumber(player.live_blocks_current, 2))}</strong></span>
          <span>Live TOV <strong>${escapeHTML(formatNumber(player.live_turnovers_current, 2))}</strong></span>
          <span>Live 3PM <strong>${escapeHTML(formatNumber(player.live_three_points_current, 2))}</strong></span>
          <span>3PM <strong>${escapeHTML(formatNumber(player.projected_three_points_made, 2))}</strong></span>
          <span>STL <strong>${escapeHTML(formatNumber(player.projected_steals, 2))}</strong></span>
          <span>BLK <strong>${escapeHTML(formatNumber(player.projected_blocks, 2))}</strong></span>
          <span>TOV <strong>${escapeHTML(formatNumber(player.projected_turnovers, 2))}</strong></span>
          <span>Injury risk <strong>${escapeHTML(formatPct(Number(player.injury_risk_score || 0) * 100))}</strong></span>
          <span>Injury min mult <strong>${escapeHTML(formatNumber(player.injury_minutes_multiplier, 2))}</strong></span>
          <span>Pregame lock <strong>${escapeHTML(formatPct(Number(player.pregame_lock_confidence || 0) * 100))}</strong></span>
          <span>Lock tier <strong>${escapeHTML(player.pregame_lock_tier || "n/a")}</strong></span>
          <span>Lock window stage <strong>${escapeHTML(lockWindowStage)}</strong></span>
          <span>Minutes to tipoff <strong>${escapeHTML(lockWindowMinutes)}</strong></span>
          <span>Lock window weight <strong>${escapeHTML(formatNumber(player.pregame_lock_window_weight, 3))}</strong></span>
          <span>Pregame line freshness <strong>${escapeHTML(formatPct(Number(player.pregame_line_freshness_score || 0) * 100))}</strong></span>
          <span>Min line age (min) <strong>${escapeHTML(formatNumber(player.pregame_min_line_age_minutes, 1))}</strong></span>
          <span>Minutes err est <strong>${escapeHTML(formatNumber(player.minutes_projection_error_estimate, 2))}</strong></span>
          <span>PTS std (L10) <strong>${escapeHTML(formatNumber(player.points_std_last_10, 2))}</strong></span>
          <span>MIN std (L10) <strong>${escapeHTML(formatNumber(player.minutes_std_last_10, 2))}</strong></span>
          <span>Starter rate (L10) <strong>${escapeHTML(formatPct(Number(player.starter_rate_last_10 || 0) * 100))}</strong></span>
          <span>Home-court pts boost <strong>${escapeHTML(formatSigned(player.home_court_points_boost, 2))}</strong></span>
          <span>Home-court min boost <strong>${escapeHTML(formatSigned(player.home_court_minutes_boost, 2))}</strong></span>
          <span>Hometown game <strong>${player.hometown_game_flag ? "Yes" : "No"}</strong></span>
          <span>Hometown score <strong>${escapeHTML(formatNumber(player.hometown_advantage_score, 2))}</strong></span>
          <span>Teammate vacancy <strong>${escapeHTML(formatNumber(player.teammate_usage_vacancy, 2))}</strong></span>
          <span>Team continuity <strong>${escapeHTML(formatPct(Number(player.teammate_continuity_score || 0) * 100))}</strong></span>
          <span>Star teammate out <strong>${player.teammate_star_out_flag ? "Yes" : "No"}</strong></span>
          <span>Synergy PTS <strong>${escapeHTML(formatSigned(player.teammate_synergy_points, 2))}</strong></span>
          <span>Synergy REB <strong>${escapeHTML(formatSigned(player.teammate_synergy_rebounds, 2))}</strong></span>
          <span>Synergy AST <strong>${escapeHTML(formatSigned(player.teammate_synergy_assists, 2))}</strong></span>
          <span>Arc profile <strong>${escapeHTML(player.shot_style_arc_label || "n/a")} (${escapeHTML(formatNumber(player.shot_style_arc_score, 2))})</strong></span>
          <span>Release profile <strong>${escapeHTML(player.shot_style_release_label || "n/a")} (${escapeHTML(formatNumber(player.shot_style_release_score, 2))})</strong></span>
          <span>Playstyle role <strong>${escapeHTML(player.playstyle_primary_role || "n/a")}</strong></span>
          <span>Scoring mode <strong>${escapeHTML(player.playstyle_scoring_mode || "n/a")}</strong></span>
          <span>Playstyle source <strong>${escapeHTML(player.playstyle_shot_profile_source || "n/a")}</strong></span>
          <span>Playstyle confidence <strong>${escapeHTML(formatPct(Number(player.playstyle_context_confidence || 0) * 100))}</strong></span>
          <span>3PT rate <strong>${escapeHTML(formatPct(Number(player.playstyle_three_rate || 0) * 100))}</strong></span>
          <span>Rim rate <strong>${escapeHTML(formatPct(Number(player.playstyle_rim_rate || 0) * 100))}</strong></span>
          <span>Mid-range rate <strong>${escapeHTML(formatPct(Number(player.playstyle_mid_range_rate || 0) * 100))}</strong></span>
          <span>Catch-and-shoot rate <strong>${escapeHTML(formatPct(Number(player.playstyle_catch_shoot_rate || 0) * 100))}</strong></span>
          <span>Pull-up rate <strong>${escapeHTML(formatPct(Number(player.playstyle_pull_up_rate || 0) * 100))}</strong></span>
          <span>Drive rate <strong>${escapeHTML(formatNumber(player.playstyle_drive_rate, 2))}</strong></span>
          <span>Assist potential <strong>${escapeHTML(formatNumber(player.playstyle_assist_potential, 2))}</strong></span>
          <span>Paint touch rate <strong>${escapeHTML(formatNumber(player.playstyle_paint_touch_rate, 2))}</strong></span>
          <span>Post touch rate <strong>${escapeHTML(formatNumber(player.playstyle_post_touch_rate, 2))}</strong></span>
          <span>Elbow touch rate <strong>${escapeHTML(formatNumber(player.playstyle_elbow_touch_rate, 2))}</strong></span>
          <span>Rebound chance rate <strong>${escapeHTML(formatNumber(player.playstyle_rebound_chance_rate, 2))}</strong></span>
          <span>Off-ball activity <strong>${escapeHTML(formatNumber(player.playstyle_offball_activity_rate, 2))}</strong></span>
          <span>Usage proxy <strong>${escapeHTML(formatNumber(player.playstyle_usage_proxy, 2))}</strong></span>
          <span>Defensive event rate <strong>${escapeHTML(formatNumber(player.playstyle_defensive_event_rate, 2))}</strong></span>
          <span>Shot volume idx <strong>${escapeHTML(formatNumber(player.shot_style_volume_index, 2))}</strong></span>
          <span>Shot miss pressure <strong>${escapeHTML(formatNumber(player.shot_style_miss_pressure, 2))}</strong></span>
          <span>Team miss pressure <strong>${escapeHTML(formatNumber(player.team_shot_miss_pressure, 2))}</strong></span>
          <span>Opponent miss pressure <strong>${escapeHTML(formatNumber(player.opponent_shot_miss_pressure, 2))}</strong></span>
          <span>Opponent avg height <strong>${escapeHTML(formatNumber(player.opponent_avg_height_inches, 2))}</strong></span>
          <span>Height advantage <strong>${escapeHTML(formatSigned(player.opponent_height_advantage_inches, 2))}</strong></span>
          <span>Tall mismatch penalty <strong>${escapeHTML(formatSigned(player.shot_style_tall_mismatch_penalty, 2))}</strong></span>
          <span>Style pace bonus <strong>${escapeHTML(formatSigned(player.shot_style_pace_bonus, 2))}</strong></span>
          <span>Rebound environment <strong>${escapeHTML(formatSigned(player.shot_style_rebound_environment, 2))}</strong></span>
          <span>PTS style factor <strong>${escapeHTML(formatNumber(player.shot_style_points_factor, 3))}</strong></span>
          <span>3PM style factor <strong>${escapeHTML(formatNumber(player.shot_style_three_points_factor, 3))}</strong></span>
          <span>REB style factor <strong>${escapeHTML(formatNumber(player.shot_style_rebounds_factor, 3))}</strong></span>
          <span>AST style factor <strong>${escapeHTML(formatNumber(player.shot_style_assists_factor, 3))}</strong></span>
          <span>TOV style factor <strong>${escapeHTML(formatNumber(player.shot_style_turnovers_factor, 3))}</strong></span>
          <span>News articles 24h <strong>${escapeHTML(formatNumber(player.news_article_count_24h, 0))}</strong></span>
          <span>News risk <strong>${escapeHTML(formatPct(Number(player.news_risk_score || 0) * 100))}</strong></span>
          <span>News confidence <strong>${escapeHTML(formatPct(Number(player.news_confidence_score || 0) * 100))}</strong></span>
        </div>
        <div class="card-note">
          Injury: ${escapeHTML(player.injury_status || "None")} | Health: ${escapeHTML(player.health_status || "None")} | Suspension: ${escapeHTML(player.suspension_status || "None")}
        </div>
        <div class="market-table">
          <div class="card-note"><strong>Lineup / Teammate Scenarios</strong></div>
          ${scenarioRowsHtml}
        </div>
        <div class="market-table">
          <div class="card-note"><strong>Confidence Decomposition</strong></div>
          ${confidenceDecompositionHtml}
        </div>
        <div class="market-table">
          ${buildMarketRows(player.market_references)}
        </div>
      </div>
    </button>
  `;

  const watchToggle = card.querySelector(".watch-toggle");
  watchToggle.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    toggleWatchlist(player);
    renderBoardSummary(boardPayload);
    renderBoardGroups();
  });

  const button = card.querySelector(".player-prop-card-button");
  button.addEventListener("click", () => {
    card.classList.toggle("expanded");
  });
  return card;
}

function applyBoardFilters(cards) {
  const searchTerm = document.getElementById("board-search").value.trim().toLowerCase();
  const viewFilter = document.getElementById("board-view-filter").value;

  return cards.filter((player) => {
    const searchable = `${player.player_name} ${player.team} ${player.opponent}`.toLowerCase();
    if (searchTerm && !searchable.includes(searchTerm)) return false;

    if (viewFilter === "playable" && !player.is_playable) return false;
    if (viewFilter === "starters" && !player.is_starter) return false;
    if (viewFilter === "high_conf" && Number(player.error_pct_estimate) > 10) return false;
    if (viewFilter === "actionable" && player.no_bet) return false;
    if (viewFilter === "no_bet" && !player.no_bet) return false;
    if (viewFilter === "non_starters" && String(player.start_label || "").toLowerCase().startsWith("not starting") === false) return false;
    if (viewFilter === "high_error" && Number(player.error_pct_estimate) <= 10) return false;

    if (boardQuickChip === "live" && !player.live_projection_in_game_flag) return false;
    if (boardQuickChip === "starters" && !player.is_starter) return false;
    if (boardQuickChip === "popular" && !player.popular_high_confidence) return false;
    if (boardQuickChip === "actionable" && player.no_bet) return false;
    if (boardQuickChip === "watchlist" && !watchlist.has(playerKey(player))) return false;

    return true;
  });
}

function sortPlayers(players) {
  const sortMode = document.getElementById("board-sort")?.value || "smart";
  const safe = [...players];
  const byName = (a, b) => String(a.player_name || "").localeCompare(String(b.player_name || ""));

  safe.sort((a, b) => {
    if (sortMode === "name_asc") return byName(a, b);
    if (sortMode === "confidence_desc") return Number(b.confidence_pct || 0) - Number(a.confidence_pct || 0) || byName(a, b);
    if (sortMode === "error_asc") return Number(a.error_pct_estimate || 999) - Number(b.error_pct_estimate || 999) || byName(a, b);
    if (sortMode === "edge_desc") return computeBestEdge(b) - computeBestEdge(a) || byName(a, b);
    if (sortMode === "points_desc") return Number(b.projected_points || 0) - Number(a.projected_points || 0) || byName(a, b);
    if (sortMode === "pra_desc") return Number(b.projected_pra || 0) - Number(a.projected_pra || 0) || byName(a, b);
    if (sortMode === "starter_prob_desc") return Number(b.starter_probability || 0) - Number(a.starter_probability || 0) || byName(a, b);

    // smart: lower error first, then larger edge, then alphabetical
    const errorDiff = Number(a.error_pct_estimate || 999) - Number(b.error_pct_estimate || 999);
    if (errorDiff !== 0) return errorDiff;
    const edgeDiff = computeBestEdge(b) - computeBestEdge(a);
    if (edgeDiff !== 0) return edgeDiff;
    return byName(a, b);
  });

  return safe;
}

function renderBoardGroups() {
  const groupsShell = document.getElementById("board-groups");
  groupsShell.innerHTML = "";

  if (!boardPayload || !boardPayload.cards) {
    groupsShell.textContent = "No player cards available.";
    return;
  }

  const filtered = applyBoardFilters(boardPayload.cards);
  if (!filtered.length) {
    groupsShell.textContent = "No cards match current filters.";
    return;
  }

  const noBetPlayers = sortPlayers(filtered
    .filter((player) => player.no_bet)
  );
  const startersHigh = sortPlayers(filtered.filter((player) => player.is_starter && Number(player.error_pct_estimate) <= 10 && !player.no_bet));
  const startersOther = sortPlayers(filtered.filter((player) => player.is_starter && Number(player.error_pct_estimate) > 10 && !player.no_bet));
  const nonStarters = sortPlayers(filtered
    .filter((player) => !player.is_starter && String(player.start_label || "").toLowerCase().startsWith("not starting"))
  );
  const remaining = sortPlayers(filtered
    .filter(
      (player) =>
        !player.no_bet &&
        !(player.is_starter && Number(player.error_pct_estimate) <= 10) &&
        !(player.is_starter && Number(player.error_pct_estimate) > 10) &&
        !(!player.is_starter && String(player.start_label || "").toLowerCase().startsWith("not starting"))
    ));

  const ordered = [...startersHigh, ...startersOther, ...nonStarters, ...remaining, ...noBetPlayers];
  const visible = ordered.slice(0, boardRenderLimit);
  const visibleSet = new Set(visible);

  const visibleStartersHigh = startersHigh.filter((player) => visibleSet.has(player));
  const visibleStartersOther = startersOther.filter((player) => visibleSet.has(player));
  const visibleNonStarters = nonStarters.filter((player) => visibleSet.has(player));
  const visibleRemaining = remaining.filter((player) => visibleSet.has(player));
  const visibleNoBet = noBetPlayers.filter((player) => visibleSet.has(player));
  const visibleWatchlist = sortPlayers(visible.filter((player) => watchlist.has(playerKey(player))));
  const watchlistKeys = new Set(visibleWatchlist.map((player) => playerKey(player)));
  const maybeExcludeWatchlist = (players) =>
    boardQuickChip === "all" && watchlistKeys.size
      ? players.filter((player) => !watchlistKeys.has(playerKey(player)))
      : players;

  const sections = [
    ["My Watchlist", visibleWatchlist],
    ["Top Starters (High Confidence <=10% error, Actionable)", maybeExcludeWatchlist(visibleStartersHigh)],
    ["Other Starters", maybeExcludeWatchlist(visibleStartersOther)],
    ["Likely Non-Starters", maybeExcludeWatchlist(visibleNonStarters)],
    ["Remaining Players (Alphabetical)", maybeExcludeWatchlist(visibleRemaining)],
    ["No-Bet Flags (Guardrails Triggered)", maybeExcludeWatchlist(visibleNoBet)],
  ];

  sections.forEach(([title, cards]) => {
    if (!cards.length) return;
    const section = document.createElement("section");
    section.className = "board-group";
    section.innerHTML = `<h3>${escapeHTML(title)} <span>(${cards.length})</span></h3>`;
    const grid = document.createElement("div");
    grid.className = "player-card-grid";
    cards.forEach((player) => grid.appendChild(renderPlayerCard(player)));
    section.appendChild(grid);
    groupsShell.appendChild(section);
  });

  if (filtered.length > visible.length) {
    const more = document.createElement("div");
    more.className = "board-load-more";
    const count = document.createElement("span");
    count.textContent = `Showing ${visible.length} of ${filtered.length} players.`;
    const button = document.createElement("button");
    button.className = "button";
    button.textContent = "Load 200 More";
    button.addEventListener("click", () => {
      boardRenderLimit += 200;
      renderBoardGroups();
    });
    more.appendChild(count);
    more.appendChild(button);
    groupsShell.appendChild(more);
  }
}

function setBoardQuickChip(nextChip) {
  boardQuickChip = nextChip || "all";
  document.querySelectorAll("#board-quick-chips .quick-chip").forEach((button) => {
    const active = button.dataset.chip === boardQuickChip;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
}

function resetBoardFilters() {
  const dateSelect = document.getElementById("board-date-select");
  const search = document.getElementById("board-search");
  const view = document.getElementById("board-view-filter");
  const sort = document.getElementById("board-sort");
  if (search) search.value = "";
  if (view) view.value = "all";
  if (sort) sort.value = "smart";
  setBoardLayoutMode("best_of_both");
  setBoardQuickChip("all");
  boardRenderLimit = 200;
  renderBoardGroups();
  setText("board-output", `Filters reset for ${(dateSelect && dateSelect.value) || "current slate"}.`);
}

async function loadPlayerBoard(dateOverride = null) {
  let date = dateOverride;
  if (!date) {
    const selectNode = document.getElementById("board-date-select");
    const selected = selectNode ? selectNode.value : "";
    date = selected || null;
  }
  const query = date ? `?date=${encodeURIComponent(date)}` : "";
  const payload = await fetchJSON(`/api/player-board${query}`);
  boardPayload = payload;
  populateBoardDateSelect(payload);
  renderBoardSummary(payload);
  renderBoardGroups();
  setText("board-output", `Board loaded: ${payload.cards?.length || 0} cards for ${payload.board_date || "N/A"}.`);
}

async function runDailyRefresh() {
  await runBackgroundJob(
    "/api/daily/refresh-async",
    "board-output",
    "Daily refresh pipeline",
    { refreshOnDone: true, maxMs: 40 * 60 * 1000 }
  );
}

function renderLiveSources(payload) {
  latestLiveSourcesPayload = payload;
  const sourceShell = document.getElementById("live-sources-list");
  const statusNode = document.getElementById("live-source-status");
  const localLinksShell = document.getElementById("live-local-links");
  const actionButtonsShell = document.getElementById("live-action-buttons");

  sourceShell.innerHTML = "";
  localLinksShell.innerHTML = "";
  actionButtonsShell.innerHTML = "";

  if (!payload) {
    statusNode.textContent = "Live source catalog not available.";
    return;
  }

  const lastUpdated = payload.updated_at || "Never";
  statusNode.textContent = snapshotModeEnabled
    ? `Web snapshot mode (read-only) | Snapshot generated: ${lastUpdated}`
    : `Live sync ${payload.running ? "running" : "stopped"} | Last updated: ${lastUpdated}`;

  (payload.sources || []).forEach((source) => {
    const card = document.createElement("div");
    card.className = "source-item";
    const docsLink = source.docs_url
      ? `<a href="${escapeHTML(source.docs_url)}" target="_blank" rel="noreferrer noopener">docs</a>`
      : "";
    const pageLink = source.page_url
      ? `<a href="${escapeHTML(source.page_url)}" target="_blank" rel="noreferrer noopener">page</a>`
      : "";
    const primaryLink = source.url
      ? `<a href="${escapeHTML(source.url)}" target="_blank" rel="noreferrer noopener">${escapeHTML(source.url)}</a>`
      : "Not configured";
    const note = source.note ? `<div class="source-note">Note: ${escapeHTML(source.note)}</div>` : "";
    const error = source.last_error
      ? `<div class="source-error">Last error: ${escapeHTML(source.last_error)}</div>`
      : "";

    card.innerHTML = `
      <div class="source-head">
        <strong>${escapeHTML(source.name || "Unnamed source")}</strong>
        <span class="status-chip ${source.enabled ? "enabled" : "disabled"}">${source.enabled ? "enabled" : "disabled"}</span>
      </div>
      <div class="source-description">${escapeHTML(source.description || "")}</div>
      <div class="source-link">${primaryLink}</div>
      <div class="source-meta">
        <span>Rows: ${escapeHTML(source.rows ?? "n/a")}</span>
        ${docsLink}
        ${pageLink}
      </div>
      ${note}
      ${error}
    `;
    sourceShell.appendChild(card);
  });

  (payload.local_links || []).forEach((link) => {
    const card = document.createElement("div");
    card.className = "source-item local-link-item";
    card.innerHTML = `
      <strong>${escapeHTML(link.name || "Local file")}</strong>
      <div class="source-description">${escapeHTML(link.description || "")}</div>
      <div class="source-link"><a href="${escapeHTML(link.url)}">Open ${escapeHTML(link.url)}</a></div>
    `;
    localLinksShell.appendChild(card);
  });

  (payload.actions || []).forEach((action) => {
    if (snapshotModeEnabled) {
      return;
    }
    if (action.developer_only && !developerModeEnabled) {
      return;
    }
    const button = document.createElement("button");
    button.className = "button";
    button.textContent = action.label || `${action.method || "GET"} ${action.endpoint || ""}`;
    button.addEventListener("click", async () => {
      await invokeDashboardAction(
        action.endpoint || "/api/status",
        (action.method || "GET").toUpperCase(),
        `Running ${action.label || action.endpoint}...`
      );
    });
    actionButtonsShell.appendChild(button);
  });
}

function renderStatus(status) {
  const training = status.training_dataset;
  const upcoming = status.upcoming_dataset;
  const context = status.context_dataset;
  const seasonPriors = status.season_priors_dataset;
  const prizepicksLines = status.prizepicks_lines_dataset;
  const prizepicksEdges = status.prizepicks_edges;
  const metrics = status.metrics;
  const predictions = status.predictions;
  const live = status.live_sync;

  setText(
    "training-status",
    training ? `${training.rows} rows loaded from ${training.path}` : "No training dataset uploaded yet."
  );
  setText(
    "upcoming-status",
    upcoming ? `${upcoming.rows} rows loaded from ${upcoming.path}` : "No upcoming slate uploaded yet."
  );
  setText(
    "context-status",
    context ? `${context.rows} rows loaded from ${context.path}` : "No context feed uploaded yet."
  );
  setText(
    "season-priors-status",
    seasonPriors ? `${seasonPriors.rows} rows loaded from ${seasonPriors.path}` : "No season priors imported yet."
  );
  setText(
    "prizepicks-status",
    prizepicksLines ? `${prizepicksLines.rows} lines loaded from ${prizepicksLines.path}` : "No PrizePicks lines imported yet."
  );
  setText("pipeline-output", status.data_pipeline ? prettyJSON(status.data_pipeline) : "Pipeline diagnostics unavailable.");
  setText(
    "drift-output",
    status.data_pipeline && status.data_pipeline.drift_audit
      ? prettyJSON(status.data_pipeline.drift_audit)
      : "Drift audit not loaded. Use 'Run Drift Audit' for a full contract check."
  );
  setText("automation-output", status.automation ? prettyJSON(status.automation) : "Automation status unavailable.");
  setText("train-output", metrics ? prettyJSON(metrics) : "No training run yet.");
  setText(
    "predict-output",
    predictions ? `${predictions.rows} projected rows saved to ${predictions.path}` : "No predictions generated yet."
  );
  setText(
    "prizepicks-edges-output",
    prizepicksEdges ? `${prizepicksEdges.rows} edge rows saved to ${prizepicksEdges.path}` : "No PrizePicks edge file yet."
  );
  setText("live-output", live ? prettyJSON(live.state) : "No live sync state yet.");

  renderDownloads(status);
  renderTableInto("predictions-table", predictions ? predictions.preview : [], "No predictions yet.", {
    searchPlaceholder: "Search players, teams, opponents, or dates",
  });
  renderTableInto("prizepicks-edges-table", prizepicksEdges ? prizepicksEdges.preview : [], "No PrizePicks edges yet.", {
    searchPlaceholder: "Search players, teams, markets, or recommendations",
  });
  renderLiveSummary(live);
  renderV05Rubric(status);
  renderCoverage(status);
}

function setAccountStatus(message) {
  setText("account-status", message);
}

function renderAccountStatus(payload) {
  accountStatusPayload = payload || null;
  const account = payload?.account || null;
  const monetization = payload?.monetization || {};
  const usageNode = document.getElementById("account-usage");
  const monthlyButton = document.getElementById("account-upgrade-monthly");
  const yearlyButton = document.getElementById("account-upgrade-yearly");
  const portalButton = document.getElementById("account-manage-billing");

  if (!account) {
    const paywallMode = monetization?.paywall_enforcement ? "ON" : "OFF";
    setAccountStatus(`Guest mode. Login required for metering/subscriptions. Paywall enforcement: ${paywallMode}.`);
    if (usageNode) usageNode.textContent = "No authenticated account.";
    if (monthlyButton) monthlyButton.disabled = true;
    if (yearlyButton) yearlyButton.disabled = true;
    if (portalButton) portalButton.disabled = true;
    return;
  }

  const subscription = account.subscription || {};
  const usage = account.usage || {};
  const planName = subscription.plan_name || subscription.plan_code || "Unknown";
  const planStatus = subscription.status || "inactive";
  const paywallMode = monetization?.paywall_enforcement ? "ON" : "OFF";

  setAccountStatus(
    `Logged in as ${account.email} | Plan: ${planName} (${planStatus}) | Paywall enforcement: ${paywallMode}`
  );

  const usagePayload = {
    period_start: usage.period_start || null,
    events: usage.events || {},
  };
  if (usageNode) usageNode.textContent = prettyJSON(usagePayload);

  const isPro = String(subscription.plan_code || "").toLowerCase() === "pro";
  if (monthlyButton) monthlyButton.disabled = isPro || snapshotModeEnabled;
  if (yearlyButton) yearlyButton.disabled = isPro || snapshotModeEnabled;
  if (portalButton) portalButton.disabled = snapshotModeEnabled;
}

async function loadAccountStatus() {
  if (snapshotModeEnabled) {
    setAccountStatus("Account and billing actions are disabled in web snapshot mode.");
    setText("account-usage", "Snapshot mode: account data unavailable.");
    return;
  }
  const payload = await fetchJSON("/api/account/status");
  renderAccountStatus(payload);
}

function accountCredentials() {
  return {
    email: String(document.getElementById("account-email")?.value || "").trim(),
    password: String(document.getElementById("account-password")?.value || ""),
  };
}

async function accountRegister() {
  if (snapshotModeEnabled) return;
  const creds = accountCredentials();
  if (!creds.email || !creds.password) {
    setAccountStatus("Email and password are required.");
    return;
  }
  setAccountStatus("Creating account...");
  await fetchJSON("/api/account/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(creds),
  });
  document.getElementById("account-password").value = "";
  await loadAccountStatus();
  await refresh();
}

async function accountLogin() {
  if (snapshotModeEnabled) return;
  const creds = accountCredentials();
  if (!creds.email || !creds.password) {
    setAccountStatus("Email and password are required.");
    return;
  }
  setAccountStatus("Logging in...");
  await fetchJSON("/api/account/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(creds),
  });
  document.getElementById("account-password").value = "";
  await loadAccountStatus();
  await refresh();
}

async function accountLogout() {
  if (snapshotModeEnabled) return;
  setAccountStatus("Logging out...");
  await fetchJSON("/api/account/logout", { method: "POST" });
  await loadAccountStatus();
  await refresh();
}

async function accountUpgrade(interval = "monthly") {
  if (snapshotModeEnabled) return;
  setAccountStatus(`Creating Stripe checkout session (${interval})...`);
  const payload = await fetchJSON("/api/account/checkout-session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ interval }),
  });
  if (payload?.checkout_url) {
    window.location.href = String(payload.checkout_url);
    return;
  }
  setAccountStatus("Checkout session did not return a URL.");
}

async function accountManageBilling() {
  if (snapshotModeEnabled) return;
  setAccountStatus("Opening billing portal...");
  const payload = await fetchJSON("/api/account/portal-session", { method: "POST" });
  if (payload?.portal_url) {
    window.location.href = String(payload.portal_url);
    return;
  }
  setAccountStatus("Billing portal did not return a URL.");
}

async function loadLiveSources() {
  const payload = await fetchJSON("/api/live/sources");
  renderLiveSources(payload);
}

async function loadPipelineStatus() {
  const payload = await fetchJSON("/api/data/pipeline");
  setText("pipeline-output", prettyJSON(payload));
}

async function loadDriftAudit() {
  const payload = await fetchJSON("/api/data/drift-audit");
  setText("drift-output", prettyJSON(payload));
}

function setFridayStatus(message) {
  setText("friday-status", message);
}

function setFridayBusy(nextBusy) {
  fridayBusy = Boolean(nextBusy);
  const sendButton = document.getElementById("friday-send");
  const input = document.getElementById("friday-input");
  if (sendButton) {
    sendButton.disabled = fridayBusy || snapshotModeEnabled;
    sendButton.textContent = fridayBusy ? "Friday is thinking..." : "Ask Friday";
  }
  if (input) {
    input.disabled = fridayBusy;
  }
}

function appendFridayMessage(role, content) {
  const log = document.getElementById("friday-chat-log");
  if (!log) return;
  const normalizedRole = String(role || "").toLowerCase() === "user" ? "user" : "assistant";
  const item = document.createElement("article");
  item.className = `friday-message ${normalizedRole}`;
  const roleLabel = normalizedRole === "user" ? "You" : "Friday";
  const safeContent = escapeHTML(String(content || "")).replace(/\n/g, "<br>");
  item.innerHTML = `
    <div class="friday-message-role">${escapeHTML(roleLabel)}</div>
    <div class="friday-message-body">${safeContent}</div>
  `;
  log.appendChild(item);
  log.scrollTop = log.scrollHeight;
}

function getFridayBoardDate() {
  const fridayDate = document.getElementById("friday-board-date")?.value;
  if (fridayDate) return fridayDate;
  const boardDate = document.getElementById("board-date-select")?.value;
  if (boardDate) return boardDate;
  return null;
}

function setFridayModelSelect(modelName) {
  const modelSelect = document.getElementById("friday-model");
  if (!modelSelect) return;
  const next = String(modelName || "").trim();
  if (!next) return;
  if (![...modelSelect.options].some((opt) => opt.value === next)) {
    const option = document.createElement("option");
    option.value = next;
    option.textContent = next;
    modelSelect.appendChild(option);
  }
  modelSelect.value = next;
}

async function loadFridayStatus() {
  const boardDate = getFridayBoardDate();
  const query = boardDate ? `?date=${encodeURIComponent(boardDate)}` : "";
  const payload = await fetchJSON(`/api/assistant/status${query}`);
  fridayStatusPayload = payload;
  const assistant = payload?.assistant || {};
  const mode = assistant.mode || "local_fallback";
  const model = assistant.model || "n/a";
  const keySource = assistant.key_source || "none";
  const keyMasked = assistant.api_key_masked || null;
  setFridayModelSelect(model);
  if (mode === "live") {
    const suffix = keyMasked ? ` • key ${keySource} (${keyMasked})` : "";
    setFridayStatus(`Friday online • Live model: ${model}${suffix}`);
  } else {
    setFridayStatus("Friday online • Local fallback mode (connect API key below to enable live LLM replies).");
  }
  if (!fridayInitialized) {
    appendFridayMessage(
      "assistant",
      "I’m Friday. Ask me about projections, confidence, no-bet reasons, or starter context for the current slate.",
    );
    fridayInitialized = true;
  }
  if (snapshotModeEnabled) {
    setFridayStatus("Friday chat is disabled in web snapshot mode.");
  }
  setFridayBusy(false);
}

async function connectFridayLiveLLM() {
  if (snapshotModeEnabled) {
    setFridayStatus("Friday live connection is disabled in web snapshot mode.");
    return;
  }
  const keyInput = document.getElementById("friday-api-key");
  const modelSelect = document.getElementById("friday-model");
  const apiKey = String(keyInput?.value || "").trim();
  const model = String(modelSelect?.value || "").trim();
  const currentMode = String(fridayStatusPayload?.assistant?.mode || "local_fallback");

  if (!apiKey) {
    if (currentMode === "live") {
      setFridayStatus("Updating Friday model...");
      try {
        await fetchJSON("/api/assistant/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            openai_model: model || null,
          }),
        });
        await loadFridayStatus();
      } catch (error) {
        setFridayStatus(`Friday model update error: ${error.message}`);
      }
      return;
    }
    setFridayStatus("Enter an OpenAI API key to enable live LLM mode.");
    return;
  }

  setFridayStatus("Connecting Friday to live LLM...");
  try {
    const payload = await fetchJSON("/api/assistant/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        openai_api_key: apiKey,
        openai_model: model || null,
        test_connection: true,
      }),
    });
    keyInput.value = "";
    const test = payload?.connection_test || {};
    if (test.ok === false) {
      setFridayStatus(`Friday connection test failed: ${test.error || "unknown error"}`);
    } else {
      setFridayStatus("Friday live connection established.");
    }
    await loadFridayStatus();
  } catch (error) {
    setFridayStatus(`Friday connection error: ${error.message}`);
  }
}

async function disconnectFridayLiveLLM() {
  if (snapshotModeEnabled) {
    setFridayStatus("Friday live connection is disabled in web snapshot mode.");
    return;
  }
  setFridayStatus("Disconnecting Friday live key...");
  try {
    await fetchJSON("/api/assistant/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clear_api_key: true }),
    });
    setFridayStatus("Friday disconnected. Local fallback mode active.");
    await loadFridayStatus();
  } catch (error) {
    setFridayStatus(`Friday disconnect error: ${error.message}`);
  }
}

async function sendFridayMessage() {
  if (snapshotModeEnabled) {
    setFridayStatus("Friday chat is disabled in web snapshot mode.");
    return;
  }
  if (fridayBusy) return;
  const input = document.getElementById("friday-input");
  if (!input) return;
  const message = String(input.value || "").trim();
  if (!message) return;

  appendFridayMessage("user", message);
  input.value = "";
  fridayConversation.push({ role: "user", content: message });
  fridayConversation = fridayConversation.slice(-12);
  setFridayBusy(true);
  setFridayStatus("Friday is analyzing the latest board context...");

  const requestBody = {
    message,
    board_date: getFridayBoardDate(),
    conversation: fridayConversation.slice(-8),
    agent_mode: Boolean(document.getElementById("friday-agent-mode")?.checked),
  };

  try {
    const payload = await fetchJSON("/api/assistant/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody),
    });
    const reply = String(payload?.reply || "No reply was returned.");
    appendFridayMessage("assistant", reply);
    fridayConversation.push({ role: "assistant", content: reply });
    fridayConversation = fridayConversation.slice(-12);
    fridayStatusPayload = payload;
    const assistant = payload?.assistant || {};
    if (assistant.mode === "live") {
      setFridayStatus(`Friday ready • Live model: ${assistant.model || "unknown"}`);
    } else {
      setFridayStatus("Friday ready • Local fallback mode");
    }
  } catch (error) {
    appendFridayMessage("assistant", `I hit an error while processing that request: ${error.message}`);
    setFridayStatus(`Friday error: ${error.message}`);
  } finally {
    setFridayBusy(false);
  }
}

function clearFridayChat() {
  fridayConversation = [];
  const log = document.getElementById("friday-chat-log");
  if (log) {
    log.innerHTML = "";
  }
  fridayInitialized = false;
  loadFridayStatus().catch((error) => {
    setFridayStatus(`Friday error: ${error.message}`);
  });
}

function scheduleAutoRefresh() {
  if (autoRefreshTimerHandle) {
    window.clearTimeout(autoRefreshTimerHandle);
  }
  autoUiRefreshNextAt = Date.now() + autoUiRefreshTimerMs;
  renderAutoRefreshStatus();
  autoRefreshTimerHandle = window.setTimeout(() => {
    refresh().catch((error) => {
      setText("live-source-status", `Auto-refresh error: ${error.message}`);
      scheduleAutoRefresh();
    });
  }, autoUiRefreshTimerMs);
}

async function refresh() {
  const [schema, status] = await Promise.all([fetchJSON("/api/schema"), fetchJSON("/api/status")]);
  latestStatus = status;
  autoUiRefreshTimerMs = resolveUiRefreshIntervalMs(status);
  const live = status?.live_sync || {};
  const liveConfig = live.config || {};
  if (!snapshotModeEnabled && !live.running && liveConfig.enabled && !liveAutoStartAttempted) {
    liveAutoStartAttempted = true;
    try {
      await fetchJSON("/api/live/start", { method: "POST" });
    } catch (_error) {
      // Keep UI responsive even if auto-start fails; next refresh will show status.
    }
  }
  if (live.running) {
    liveAutoStartAttempted = false;
  }
  scheduleAutoRefresh();
  renderSchema(schema);
  renderStatus(status);
  setBoardQuickChip(boardQuickChip);
  try {
    await loadLiveSources();
  } catch (error) {
    setText("live-source-status", `Unable to load live source catalog: ${error.message}`);
  }
  try {
    await loadPlayerBoard();
  } catch (error) {
    setText("board-output", `Unable to load player board: ${error.message}`);
  }
  try {
    await loadFridayStatus();
  } catch (error) {
    setFridayStatus(`Unable to load Friday assistant status: ${error.message}`);
  }
  try {
    await loadAccountStatus();
  } catch (error) {
    setAccountStatus(`Unable to load account status: ${error.message}`);
  }
}

async function uploadFile(endpoint, inputId, statusId) {
  const input = document.getElementById(inputId);
  if (!input.files.length) {
    setText(statusId, "Choose a CSV first.");
    return;
  }

  const formData = new FormData();
  formData.append("file", input.files[0]);
  setText(statusId, "Uploading...");
  const payload = await fetchJSON(endpoint, { method: "POST", body: formData });
  setText(statusId, prettyJSON(payload));
  await refresh();
}

async function uploadLegacy(kind, inputId, statusId) {
  const input = document.getElementById(inputId);
  if (!input.files.length) {
    setText(statusId, "Choose a CSV first.");
    return;
  }

  const formData = new FormData();
  formData.append("kind", kind);
  formData.append("file", input.files[0]);
  setText(statusId, "Uploading...");
  await fetchJSON("/api/upload", { method: "POST", body: formData });
  await refresh();
}

async function importText(endpoint, textareaId, statusId) {
  const text = document.getElementById(textareaId).value.trim();
  if (!text) {
    setText(statusId, "Paste text first.");
    return;
  }

  setText(statusId, "Importing...");
  const payload = await fetchJSON(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text }),
  });
  setText(statusId, prettyJSON(payload));
  await refresh();
}

async function train() {
  await runBackgroundJob("/api/train-async", "train-output", "Model training", {
    refreshOnDone: true,
    maxMs: 30 * 60 * 1000,
  });
}

async function predict() {
  setText("predict-output", "Generating predictions...");
  const payload = await fetchJSON("/api/predict", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ predict_all: document.getElementById("predict-all").checked }),
  });
  setText("predict-output", prettyJSON(payload));
  await refresh();
}

async function recheckPredictions() {
  setText("recheck-output", "Running recheck against training history...");
  const lookbackRaw = document.getElementById("recheck-lookback-days").value.trim();
  const sampleRowsRaw = document.getElementById("recheck-sample-rows").value.trim();
  const body = {};
  if (lookbackRaw) {
    body.lookback_days = Number(lookbackRaw);
  }
  if (sampleRowsRaw) {
    body.sample_rows = Number(sampleRowsRaw);
  }
  const payload = await fetchJSON("/api/recheck", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (payload?.job?.job_id) {
    await pollBackgroundJob(payload.job.job_id, "recheck-output", "Recheck", {
      refreshOnDone: true,
      maxMs: 30 * 60 * 1000,
    });
    return;
  }
  setText("recheck-output", prettyJSON(payload));
}

async function generatePrizePicksEdges() {
  setText("prizepicks-edges-output", "Generating edges...");
  const slateDate = document.getElementById("prizepicks-slate-date").value;
  const payload = await fetchJSON("/api/prizepicks/edges", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ slate_date: slateDate || null }),
  });
  setText("prizepicks-edges-output", prettyJSON(payload));
  await refresh();
}

async function liveAction(url, outputMessage) {
  setText("live-output", outputMessage);
  if (url === "/api/live/sync" || url === "/api/live/sync-async") {
    await runBackgroundJob("/api/live/sync-async", "live-output", "Live feed sync", {
      refreshOnDone: true,
      maxMs: 30 * 60 * 1000,
    });
    return;
  }
  const payload = await fetchJSON(url, { method: "POST" });
  if (payload?.job?.job_id) {
    await pollBackgroundJob(payload.job.job_id, "live-output", "Background action", {
      refreshOnDone: true,
    });
    return;
  }
  setText("live-output", prettyJSON(payload));
  await refresh();
}

async function invokeDashboardAction(endpoint, method = "GET", outputMessage = "Running action...") {
  setText("live-action-output", outputMessage);
  const payload = await fetchJSON(endpoint, { method });
  if (payload?.job?.job_id) {
    await pollBackgroundJob(payload.job.job_id, "live-action-output", "Dashboard action", {
      refreshOnDone: true,
      maxMs: 30 * 60 * 1000,
    });
    return;
  }
  setText("live-action-output", prettyJSON(payload));
  await refresh();
}

bindElementEvent("developer-mode-toggle", "click", () => {
  toggleDeveloperMode();
}, "board-output");
bindElementEvent("upload-training", "click", () => uploadFile("/api/import/historical", "training-file", "training-status"), "training-status");
bindElementEvent("upload-upcoming", "click", () => uploadLegacy("upcoming", "upcoming-file", "upcoming-status"), "upcoming-status");
bindElementEvent("upload-context", "click", () => uploadLegacy("context", "context-file", "context-status"), "context-status");
bindElementEvent(
  "upload-season-priors",
  "click",
  () => uploadFile("/api/import/season-priors", "season-priors-file", "season-priors-status"),
  "season-priors-status",
);
bindElementEvent(
  "paste-season-priors",
  "click",
  () => importText("/api/import/season-priors", "season-priors-text", "season-priors-status"),
  "season-priors-status",
);
bindElementEvent(
  "upload-prizepicks",
  "click",
  () => uploadFile("/api/prizepicks/lines", "prizepicks-file", "prizepicks-status"),
  "prizepicks-status",
);
bindElementEvent(
  "paste-prizepicks",
  "click",
  () => importText("/api/prizepicks/lines", "prizepicks-text", "prizepicks-status"),
  "prizepicks-status",
);
bindElementEvent("train-engine", "click", train, "train-output");
bindElementEvent("run-predictions", "click", predict, "predict-output");
bindElementEvent("run-recheck", "click", recheckPredictions, "recheck-output");
bindElementEvent("generate-prizepicks-edges", "click", generatePrizePicksEdges, "prizepicks-edges-output");
bindElementEvent("live-start", "click", () => liveAction("/api/live/start", "Starting live sync..."), "live-output");
bindElementEvent("live-stop", "click", () => liveAction("/api/live/stop", "Stopping live sync..."), "live-output");
bindElementEvent(
  "live-sync-now",
  "click",
  () => liveAction("/api/live/sync-async", "Running live sync in background..."),
  "live-output",
);
bindElementEvent(
  "live-in-game-sync-now",
  "click",
  () => liveAction("/api/live/in-game-sync", "Refreshing in-game projections from live boxscores..."),
  "live-output",
);
bindElementEvent(
  "refresh-live-sources",
  "click",
  async () => {
    setText("live-source-status", "Refreshing source status...");
    await loadLiveSources();
  },
  "live-source-status",
);
bindElementEvent(
  "refresh-pipeline-status",
  "click",
  async () => {
    setText("pipeline-output", "Refreshing pipeline diagnostics...");
    await loadPipelineStatus();
  },
  "pipeline-output",
);
bindElementEvent(
  "refresh-drift-audit",
  "click",
  async () => {
    setText("drift-output", "Running contract drift audit...");
    await loadDriftAudit();
  },
  "drift-output",
);
bindElementEvent(
  "sync-and-refresh-live",
  "click",
  () => invokeDashboardAction("/api/live/sync-async", "POST", "Syncing feeds and refreshing links..."),
  "live-action-output",
);
bindElementEvent(
  "board-refresh",
  "click",
  async () => {
    setText("board-output", "Refreshing projection board...");
    boardRenderLimit = 200;
    await loadPlayerBoard();
  },
  "board-output",
);
bindElementEvent("board-daily-refresh", "click", runDailyRefresh, "board-output");
bindElementEvent(
  "board-date-select",
  "change",
  async () => {
    boardRenderLimit = 200;
    await loadPlayerBoard(document.getElementById("board-date-select").value);
  },
  "board-output",
);
bindElementEvent("board-search", "input", () => {
  boardRenderLimit = 200;
  renderBoardGroups();
}, "board-output");
bindElementEvent("board-view-filter", "change", () => {
  boardRenderLimit = 200;
  renderBoardGroups();
}, "board-output");
bindElementEvent("board-sort", "change", () => {
  boardRenderLimit = 200;
  renderBoardGroups();
}, "board-output");
bindElementEvent("board-layout-mode", "change", () => {
  const mode = document.getElementById("board-layout-mode")?.value || "best_of_both";
  setBoardLayoutMode(mode);
  renderBoardGroups();
}, "board-output");
bindElementEvent("board-reset-filters", "click", resetBoardFilters, "board-output");
bindElementEvent("board-quick-chips", "click", (event) => {
  const chip = event.target.closest(".quick-chip");
  if (!chip) return;
  setBoardQuickChip(chip.dataset.chip || "all");
  boardRenderLimit = 200;
  renderBoardGroups();
}, "board-output");
bindElementEvent("friday-send", "click", sendFridayMessage, "friday-status");
bindElementEvent("friday-connect", "click", connectFridayLiveLLM, "friday-status");
bindElementEvent("friday-disconnect", "click", disconnectFridayLiveLLM, "friday-status");
bindElementEvent("friday-clear", "click", clearFridayChat, "friday-status");
bindElementEvent(
  "friday-input",
  "keydown",
  (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      return sendFridayMessage();
    }
    return undefined;
  },
  "friday-status",
);
bindElementEvent(
  "friday-board-date",
  "change",
  () => loadFridayStatus(),
  "friday-status",
);
bindElementEvent(
  "friday-model",
  "change",
  () => connectFridayLiveLLM(),
  "friday-status",
);
bindElementEvent("account-register", "click", accountRegister, "account-status");
bindElementEvent("account-login", "click", accountLogin, "account-status");
bindElementEvent("account-logout", "click", accountLogout, "account-status");
bindElementEvent("account-upgrade-monthly", "click", () => accountUpgrade("monthly"), "account-status");
bindElementEvent("account-upgrade-yearly", "click", () => accountUpgrade("yearly"), "account-status");
bindElementEvent("account-manage-billing", "click", accountManageBilling, "account-status");

loadWatchlist();
loadDeveloperMode();
loadBoardLayoutMode();
renderDeveloperMode();
renderBoardLayoutMode();
setBoardQuickChip("all");

refresh().catch((error) => {
  reportUiError(error, "board-output");
  setText("train-output", (error && error.message) ? error.message : String(error));
});

setInterval(() => {
  renderAutoRefreshStatus();
}, 1000);
