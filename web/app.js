const state = {
  paused: false,
  timer: null,
  lastData: null,
  failures: 0,
};

const $ = (id) => document.getElementById(id);

function badge(text, kind) {
  return `<span class="badge ${kind}">${escapeHtml(text)}</span>`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function rangeText(region) {
  const start = region.start_key === "" ? "-inf" : region.start_key;
  const end = region.end_key === "" ? "+inf" : region.end_key;
  return `[${start}, ${end})`;
}

function renderSummary(data) {
  const summary = data.summary || {};
  const healthKind = data.ok && summary.healthy ? "ok" : data.ok ? "warn" : "err";
  const healthText = data.ok && summary.healthy ? "Healthy" : data.ok ? "Warning" : "Error";
  const refreshed = data.timestamp_ms ? new Date(data.timestamp_ms).toLocaleTimeString() : "-";
  $("summary").innerHTML = [
    metric("Health", badge(healthText, healthKind)),
    metric("Stores", summary.store_count || "0"),
    metric("Regions", summary.region_count || "0"),
    metric("Route Cache", summary.route_cache_size || "0"),
    metric("Refresh", `${data.refresh_cost_ms ?? 0} ms`),
  ].join("");
  $("subtitle").textContent = `Last refresh ${refreshed}`;
}

function metric(label, value) {
  return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div>`;
}

function renderStores(data) {
  const stores = data.pd?.stores || [];
  $("storeCount").textContent = `${stores.length} nodes`;
  $("stores").innerHTML = stores
    .map((store) => {
      const kind = store.state === "Up" ? "ok" : "err";
      return `
        <article class="store-card">
          <div class="store-title">
            <span>Store ${escapeHtml(store.store_id)}</span>
            ${badge(store.state || "Unknown", kind)}
          </div>
          <div class="kv">
            <span>Client</span><span>${escapeHtml(store.client_endpoint)}</span>
            <span>Raft</span><span>${escapeHtml(store.raft_endpoint)}</span>
            <span>Heartbeat</span><span>${escapeHtml(store.last_heartbeat_ms)}</span>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderRegions(data) {
  const regions = data.pd?.regions || [];
  $("regionCount").textContent = `${regions.length} regions`;
  $("regionRows").innerHTML = regions
    .map((region) => {
      const peers = (region.peers || [])
        .map((peer) => `p${escapeHtml(peer.peer_id)}@s${escapeHtml(peer.store_id)}:${escapeHtml(peer.role)}`)
        .join(", ");
      return `
        <tr>
          <td>${escapeHtml(region.region_id)}</td>
          <td>${escapeHtml(rangeText(region))}</td>
          <td>c${escapeHtml(region.conf_ver)} / v${escapeHtml(region.version)}</td>
          <td>${badge(`Peer ${region.leader_peer_id}`, "info")}</td>
          <td>${escapeHtml(peers)}</td>
        </tr>
      `;
    })
    .join("");
}

function roleBadge(role) {
  if (role === "Leader") return badge(role, "ok");
  if (role === "Candidate") return badge(role, "warn");
  if (role === "Follower") return badge(role, "info");
  return badge(role || "-", "warn");
}

function renderRuntime(data) {
  const runtime = data.store?.runtime_regions || [];
  $("runtimeCount").textContent = `${runtime.length} rows`;
  $("runtimeRows").innerHTML = runtime
    .map((region) => `
      <tr>
        <td>${escapeHtml(region.region_id)}</td>
        <td>${escapeHtml(region.local_store_id || "-")}</td>
        <td>${escapeHtml(region.local_peer_id || "-")}</td>
        <td>${roleBadge(region.runtime_role)}</td>
        <td>${escapeHtml(region.runtime_commit_index || "-")}</td>
        <td>${escapeHtml(region.runtime_last_applied || "-")}</td>
        <td>${escapeHtml(region.runtime_last_log_index || "-")}</td>
      </tr>
    `)
    .join("");
}

function renderWarnings(data) {
  const warnings = data.summary?.warnings || [];
  $("warningCount").textContent = `${warnings.length} items`;
  if (warnings.length === 0) {
    $("warnings").innerHTML = `<div class="warning-item">No warnings</div>`;
    return;
  }
  $("warnings").innerHTML = warnings
    .map((warning) => `<div class="warning-item">${escapeHtml(warning)}</div>`)
    .join("");
}

function renderRaw(data) {
  $("rawFields").textContent = JSON.stringify(data.raw || {}, null, 2);
}

function render(data) {
  state.lastData = data;
  renderSummary(data);
  renderStores(data);
  renderRegions(data);
  renderRuntime(data);
  renderWarnings(data);
  renderRaw(data);
}

async function fetchStatus() {
  if (state.paused) return;
  try {
    const response = await fetch("/api/status", { cache: "no-store" });
    const data = await response.json();
    state.failures = response.ok ? 0 : state.failures + 1;
    render(data);
  } catch (error) {
    state.failures += 1;
    render({
      ok: false,
      timestamp_ms: Date.now(),
      refresh_cost_ms: 0,
      summary: {
        healthy: false,
        store_count: "0",
        region_count: "0",
        route_cache_size: "0",
        warnings: [`Dashboard refresh failed: ${error.message}`],
      },
      pd: { stores: [], regions: [] },
      store: { runtime_regions: [] },
      raw: {},
    });
  }
}

$("refreshButton").addEventListener("click", fetchStatus);
$("pauseButton").addEventListener("click", () => {
  state.paused = !state.paused;
  $("pauseButton").textContent = state.paused ? "Resume" : "Pause";
});

fetchStatus();
state.timer = setInterval(fetchStatus, 1000);
