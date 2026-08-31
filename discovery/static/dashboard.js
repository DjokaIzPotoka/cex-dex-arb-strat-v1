// ZORA MEV Discovery -- Ops Dashboard frontend logic.
//
// Polls GET /api/status every REFRESH_INTERVAL_MS and re-renders the
// dynamic parts of the already-loaded page in place (item 3: "don't
// reload the whole page -- update table rows, the status strip, and
// the log panel in place"). Every value rendered here comes straight
// from the JSON payload build_status_payload() (ops_dashboard_server.py)
// assembles -- this file does no health/status computation of its own,
// only display, matching the same "reuse all existing validated logic,
// just display it differently" boundary the whole rest of this
// dashboard's history has kept.

const REFRESH_INTERVAL_MS = 30_000;

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.textContent;
}

// One severity value from the payload drives BOTH the strip's tint
// (via the sev-* class) and its label word, so they can't disagree --
// see build_status_payload()'s own comment for why the earlier binary
// flag + separate `level` word could read "GREEN" on amber.
const SEVERITIES = ["green", "amber", "red"];

function renderStatusStrip(payload) {
  const strip = document.getElementById("status-strip");
  const severity = payload.health.severity;
  for (const s of SEVERITIES) strip.classList.toggle("sev-" + s, s === severity);
  strip.classList.remove("problem");

  const dot = document.getElementById("status-text");
  dot.textContent = "● " + severity.toUpperCase() + " — " + payload.health.summary;

  document.getElementById("cycle-age").textContent =
    payload.cycle_age_min !== null ? payload.cycle_age_min.toFixed(1) + "m" : "n/a";
  document.getElementById("uptime").textContent = payload.uptime !== null ? payload.uptime : "n/a";
  document.getElementById("new-entities").textContent = payload.new_entity_count;

  const rpcEl = document.getElementById("rpc-errors");
  const rpcCount = payload.rpc_error_count;
  rpcEl.textContent = rpcCount + (payload.rpc_error_count_is_lower_bound ? "+" : "");
  rpcEl.style.color = rpcCount > 0 ? "var(--red)" : "var(--green)";

  document.getElementById("meteora-cursor").textContent = payload.meteora_cursor || "n/a";
}

function renderSourcesTable(payload) {
  const tbody = document.getElementById("sources-body");
  tbody.innerHTML = "";
  for (const src of payload.sources) {
    const tr = document.createElement("tr");
    tr.className = "status-" + src.status;
    tr.innerHTML =
      "<td>" + escapeHtml(src.tag) + "</td>" +
      "<td>" + escapeHtml(src.kind) + "</td>" +
      "<td class=\"status-cell\">" + escapeHtml(src.status) + "</td>" +
      "<td class=\"num\">" + src.rows_this_cycle.toLocaleString() + "</td>";
    tbody.appendChild(tr);
  }
}

function renderTier2Table(payload) {
  const tbody = document.getElementById("tier2-body");
  tbody.innerHTML = "";
  for (const row of payload.tier2.rows) {
    const tr = document.createElement("tr");
    tr.innerHTML =
      "<td><span class=\"reason-swatch\" style=\"background:" + row.color + "\"></span>" +
      "<span style=\"color:" + row.color + "\">" + escapeHtml(row.reason) + "</span></td>" +
      "<td class=\"num\">" + row.count.toLocaleString() + "</td>";
    tbody.appendChild(tr);
  }
  const totalTr = document.createElement("tr");
  totalTr.className = "tier2-total";
  totalTr.innerHTML = "<td>TOTAL</td><td class=\"num\">" + payload.tier2.total.toLocaleString() + "</td>";
  tbody.appendChild(totalTr);
}

function renderSparkline(payload) {
  const durations = payload.cycle_durations_s;
  const n = durations.length;
  const avgTxt = n ? Math.round(durations.reduce((a, b) => a + b, 0) / n) + "s" : "n/a";
  document.getElementById("sparkline-caption").textContent =
    "cycle duration, last " + n + "  ·  avg " + avgTxt;

  const container = document.getElementById("sparkline-bars");
  container.innerHTML = "";
  if (!n) return;

  // Scale between the window's own min and max, not against max alone.
  // Cycle durations cluster tightly in practice (a real window looked
  // like 465-532s), so max-only scaling rendered every bar at 87-100%
  // height -- visually identical blocks carrying no information. A
  // 30% floor keeps the shortest bar visibly a bar rather than a
  // sliver, while the remaining 70% of the range shows the actual
  // spread. When every value is identical (or there's one sample),
  // all bars sit at a flat mid height -- honest, rather than
  // manufacturing variation from a zero range.
  const max = Math.max(...durations);
  const min = Math.min(...durations);
  const span = max - min;
  for (const d of durations) {
    const bar = document.createElement("div");
    bar.className = "sparkline-bar";
    const pct = span > 0 ? 30 + ((d - min) / span) * 70 : 60;
    bar.style.height = pct + "%";
    bar.title = d.toFixed(1) + "s";
    container.appendChild(bar);
  }
}

function renderLogPanel(payload) {
  const log = payload.log;
  document.getElementById("log-scan-lines").textContent = log.scan_lines.toLocaleString();

  const errChip = document.getElementById("log-err-chip");
  errChip.textContent = "ERR " + log.error_count;
  errChip.className = "chip" + (log.error_count > 0 ? " chip-red" : "");

  const warnChip = document.getElementById("log-warn-chip");
  warnChip.textContent = "WARN " + log.warning_count;
  warnChip.className = "chip" + (log.warning_count > 0 ? " chip-amber" : "");

  const panel = document.getElementById("log-panel");
  panel.innerHTML = "";
  if (log.failure_reason) {
    const div = document.createElement("div");
    div.className = "log-error";
    div.textContent = log.failure_reason;
    panel.appendChild(div);
    return;
  }
  if (!log.lines.length) {
    const div = document.createElement("div");
    div.className = "log-empty";
    div.textContent = "no log lines in scanned window";
    panel.appendChild(div);
    return;
  }
  for (const line of log.lines) {
    const div = document.createElement("div");
    div.className = "log-line level-" + line.level;
    div.textContent = line.ts + "  " + line.level.padEnd(5) + "  " + line.src + "  " + line.message;
    panel.appendChild(div);
  }
}

function renderError(message) {
  // Unreachable backend / failed query is a red state, same severity
  // vocabulary as everything else on the strip.
  const strip = document.getElementById("status-strip");
  for (const s of SEVERITIES) strip.classList.toggle("sev-" + s, s === "red");
  document.getElementById("status-text").textContent = "● RED — " + message;
}

async function refresh() {
  try {
    const resp = await fetch("/api/status", { cache: "no-store" });
    const payload = await resp.json();
    if (payload.error) {
      renderError(payload.error);
      return;
    }
    renderStatusStrip(payload);
    renderSourcesTable(payload);
    renderTier2Table(payload);
    renderSparkline(payload);
    renderLogPanel(payload);
  } catch (exc) {
    renderError("failed to reach /api/status: " + exc);
  }
}

refresh();
setInterval(refresh, REFRESH_INTERVAL_MS);
