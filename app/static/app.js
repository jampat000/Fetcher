function qs(name) {
  const u = new URL(window.location.href);
  return u.searchParams.get(name);
}

function showToast(text) {
  const el = document.getElementById("toast");
  if (!el) return;
  el.textContent = text;
  el.classList.add("show");
  window.setTimeout(() => el.classList.remove("show"), 2500);
}

function bindRevealButtons() {
  document.querySelectorAll("[data-reveal]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      const id = btn.getAttribute("data-reveal");
      const input = document.getElementById(id);
      if (!input) return;
      const isPw = input.getAttribute("type") === "password";
      input.setAttribute("type", isPw ? "text" : "password");
      btn.textContent = isPw ? "Hide" : "Show";
    });
  });
}

function scrollMainEl() {
  return document.querySelector("main.main");
}

/** Same logical page whether or not URL has a trailing slash. */
function normPathname(path) {
  if (!path || path === "/") return path || "/";
  return path.endsWith("/") ? path.slice(0, -1) : path;
}

function grabScrollY() {
  const main = scrollMainEl();
  return main ? main.scrollTop : window.scrollY;
}

function applyScrollY(y) {
  const top = Math.max(0, Number(y) || 0);
  const m = scrollMainEl();
  if (m) m.scrollTop = top;
  else window.scrollTo(0, top);
}

function persistScrollForAfterRedirect() {
  try {
    sessionStorage.setItem(
      "fetcher_restore_scroll",
      JSON.stringify({
        path: normPathname(window.location.pathname),
        y: grabScrollY(),
      }),
    );
  } catch (_) {
    /* ignore quota / private mode */
  }
}

function shouldRestoreAfterRedirect() {
  const sp = new URLSearchParams(window.location.search);
  const base =
    sp.get("saved") === "1" ||
    sp.get("test") === "sonarr_ok" ||
    sp.get("test") === "sonarr_fail" ||
    sp.get("test") === "radarr_ok" ||
    sp.get("test") === "radarr_fail" ||
    sp.get("test") === "emby_ok" ||
    sp.get("test") === "emby_fail";
  if (!base) return false;
  const path = normPathname(window.location.pathname || "");
  if (path === "/settings" && sp.get("tab")) return false;
  if (path === "/trimmer/settings" && (window.location.hash || "").trim()) return false;
  if (path === "/refiner/settings" && (window.location.hash || "").trim()) return false;
  return true;
}

/** Keeps main-column scroll across redirect + late layout (pageshow / fonts). */
let fetcherPendingMainScroll = null;

/** Remember scroll when saving (303 redirect reloads at top). */
function bindScrollRestoreOnFormSubmit() {
  document.querySelectorAll('form[method="post"]').forEach((form) => {
    if (form.getAttribute("data-fetcher-async-settings") === "1") return;
    if (form.getAttribute("data-fetcher-async-test") === "1") return;
    if (form.getAttribute("data-fetcher-trimmer-async-connection") === "1") return;
    if (form.getAttribute("data-fetcher-trimmer-async-cleaner") === "1") return;
    if (form.getAttribute("data-fetcher-refiner-async") === "1") return;
    form.addEventListener("submit", persistScrollForAfterRedirect, true);
    form
      .querySelectorAll('button[type="submit"], button:not([type]), input[type="submit"]')
      .forEach((el) => {
        el.addEventListener("click", persistScrollForAfterRedirect, true);
      });
  });
}

function restoreScrollAfterFormRedirect() {
  const fromReturn = shouldRestoreAfterRedirect();
  const raw = sessionStorage.getItem("fetcher_restore_scroll");

  if (fromReturn) {
    try {
      if ("scrollRestoration" in history) history.scrollRestoration = "manual";
    } catch (_) {
      /* ignore */
    }
  }

  if (!fromReturn) {
    if (raw) sessionStorage.removeItem("fetcher_restore_scroll");
    fetcherPendingMainScroll = null;
    return;
  }

  if (!raw) {
    if (fetcherPendingMainScroll != null) applyScrollY(fetcherPendingMainScroll);
    return;
  }

  sessionStorage.removeItem("fetcher_restore_scroll");

  let path;
  let y;
  try {
    const o = JSON.parse(raw);
    path = normPathname(o.path);
    y = Math.max(0, Number(o.y) || 0);
  } catch (_) {
    return;
  }
  if (path !== normPathname(window.location.pathname)) return;

  fetcherPendingMainScroll = y;
  const apply = () => applyScrollY(fetcherPendingMainScroll);
  requestAnimationFrame(apply);
  [0, 50, 100, 200, 400, 600].forEach((ms) => window.setTimeout(apply, ms));
  window.setTimeout(() => {
    fetcherPendingMainScroll = null;
  }, 3000);
}

function reapplyPendingScrollAfterPageshow() {
  if (!shouldRestoreAfterRedirect() || fetcherPendingMainScroll == null) return;
  applyScrollY(fetcherPendingMainScroll);
}

/** Helps embedded / webview panels (Simple Browser) where default navigation can stick. */
function bindInternalLinksTargetTop() {
  document.querySelectorAll('a[href^="/"]').forEach((a) => {
    if (a.hasAttribute("download")) return;
    a.setAttribute("target", "_top");
  });
}

function bindDashboardDismissibles() {
  document.querySelectorAll(".dashboard-dismissible[data-dismiss-storage]").forEach((panel) => {
    const key = panel.getAttribute("data-dismiss-storage");
    if (!key) return;
    try {
      if (localStorage.getItem(key) === "1") {
        panel.hidden = true;
        return;
      }
    } catch (_) {
      /* private mode / quota */
    }
    const btn = panel.querySelector("[data-dismiss-btn]");
    if (!btn) return;
    btn.addEventListener("click", () => {
      try {
        localStorage.setItem(key, "1");
      } catch (_) {}
      panel.hidden = true;
    });
  });
}

function injectMeshAndNoise() {
  const existingMesh = document.getElementById("mesh");
  if (existingMesh && existingMesh.querySelector(".orb")) return;
  const mesh = document.createElement("div");
  mesh.id = "mesh";
  mesh.setAttribute("aria-hidden", "true");
  for (let i = 1; i <= 5; i += 1) {
    const orb = document.createElement("div");
    orb.className = `orb orb${i}`;
    mesh.appendChild(orb);
  }
  const noise = document.createElement("div");
  noise.id = "noise";
  noise.setAttribute("aria-hidden", "true");
  const first = document.body.firstChild;
  document.body.insertBefore(mesh, first);
  document.body.insertBefore(noise, mesh.nextSibling);
}

function staggerClass(selector, baseMs, stepMs, className) {
  document.querySelectorAll(selector).forEach((el, i) => {
    window.setTimeout(() => el.classList.add(className), baseMs + i * stepMs);
  });
}

function runHeroCountUp() {
  document.querySelectorAll(".hs-val[data-target]").forEach((el) => {
    const raw = el.getAttribute("data-target");
    const target = Math.max(0, parseInt(raw, 10) || 0);
    const startVal = Math.max(0, parseInt(el.textContent || "0", 10) || 0);
    if (startVal === target) return;
    const start = performance.now();
    const dur = 800;
    const tick = (now) => {
      const t = Math.min(1, (now - start) / dur);
      const eased = 1 - (1 - t) * (1 - t);
      const v = Math.round(startVal + (target - startVal) * eased);
      el.textContent = String(v);
      if (t < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
}

function initActivityDetailExpand() {
  document.querySelectorAll(".activity-detail-toggle").forEach((btn) => {
    btn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const sub = btn.closest(".activity-detail-sub--expandable");
      if (!sub) return;
      const rest = sub.querySelector(".activity-detail-rest");
      const expanded = btn.getAttribute("aria-expanded") === "true";
      const next = !expanded;
      btn.setAttribute("aria-expanded", next ? "true" : "false");
      const more = btn.getAttribute("data-more-count") || "0";
      if (rest) rest.hidden = !next;
      btn.textContent = next ? "Show less" : `+${more} more`;
    });
  });

  document.querySelectorAll(".activity-row--expandable").forEach((row) => {
    row.addEventListener("click", (ev) => {
      if (ev.target.closest(".activity-detail-toggle") || ev.target.closest(".activity-meta")) return;
      const btn = row.querySelector(".activity-detail-toggle");
      if (btn) btn.click();
    });
  });
}

function initActivityFilterPills() {
  document.querySelectorAll("[data-pill-filter]").forEach((pill) => {
    pill.addEventListener("click", () => {
      const filter = pill.getAttribute("data-pill-filter") || "all";
      document.querySelectorAll("[data-pill-filter]").forEach((p) => p.classList.remove("active"));
      pill.classList.add("active");
      document.querySelectorAll(".activity-row[data-activity-app]").forEach((row) => {
        if (filter === "all") {
          row.classList.remove("hidden-filter");
          return;
        }
        const app = row.getAttribute("data-activity-app") || "";
        const ok =
          (filter === "sonarr" && app === "sonarr") ||
          (filter === "radarr" && app === "radarr") ||
          (filter === "emby" && app === "emby") ||
          (filter === "refiner" && app === "refiner");
        row.classList.toggle("hidden-filter", !ok);
      });
    });
  });
}

function initSettingsPageCollapses() {
  const STORAGE_KEYS = {
    sonarr: "fetcher_settings_sonarr_open",
    radarr: "fetcher_settings_radarr_open",
    global: "fetcher_settings_global_open",
    security: "fetcher_settings_security_open",
  };
  const DEFAULTS = { sonarr: true, radarr: true, global: false, security: false };

  function parseForced() {
    const sp = new URLSearchParams(window.location.search);
    const test = sp.get("test") || "";
    if (test === "sonarr_ok" || test === "sonarr_fail") return { sonarr: true };
    if (test === "radarr_ok" || test === "radarr_fail") return { radarr: true };
    if (sp.get("saved") === "1" || sp.get("save") === "fail") {
      const sc = (sp.get("save_scope") || "").trim().toLowerCase();
      if (sc === "sonarr") return { sonarr: true };
      if (sc === "radarr") return { radarr: true };
      if (sc === "global") return { global: true };
      return { openAll: true };
    }
    if (sp.get("sec")) return { security: true };
    return null;
  }

  function setSectionOpen(section, open) {
    const root = document.querySelector(`[data-settings-section="${section}"]`);
    if (!root) return;
    const btn = root.querySelector(".settings-collapse-header");
    const chev = root.querySelector(".settings-collapse-chevron");
    if (open) {
      root.classList.remove("is-collapsed");
      if (btn) btn.setAttribute("aria-expanded", "true");
      if (chev) chev.classList.add("open");
    } else {
      root.classList.add("is-collapsed");
      if (btn) btn.setAttribute("aria-expanded", "false");
      if (chev) chev.classList.remove("open");
    }
    try {
      localStorage.setItem(STORAGE_KEYS[section], open ? "1" : "0");
    } catch (_) {}
  }

  function readStored(section) {
    try {
      const v = localStorage.getItem(STORAGE_KEYS[section]);
      if (v === "1") return true;
      if (v === "0") return false;
    } catch (_) {}
    return DEFAULTS[section];
  }

  if (!document.querySelector(".settings-collapse[data-settings-section]")) return;

  const forced = parseForced();
  const sections = ["sonarr", "radarr", "global", "security"];
  sections.forEach((section) => {
    let open;
    if (forced && forced.openAll) open = true;
    else if (forced && forced[section]) open = true;
    else if (forced && typeof forced === "object") open = readStored(section);
    else open = readStored(section);
    setSectionOpen(section, open);
  });

  document.querySelectorAll(".settings-collapse[data-settings-section]").forEach((root) => {
    const section = root.getAttribute("data-settings-section");
    if (!section) return;
    const btn = root.querySelector(".settings-collapse-header");
    if (!btn) return;
    btn.addEventListener("click", () => {
      const expand = root.classList.contains("is-collapsed");
      setSectionOpen(section, expand);
    });
  });
}

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s == null ? "" : String(s);
  return d.innerHTML;
}

function setMetricTile(metricKey, value) {
  const tile = document.querySelector(`[data-metric="${metricKey}"] .m-val`);
  if (!tile) return;
  const badge = tile.querySelector(".badge");
  if (badge) return;
  tile.textContent = String(value);
}

function fetchDashboardStatusJson() {
  if (document.visibilityState === "hidden") {
    return Promise.resolve(null);
  }
  return fetch("/api/dashboard/status", {
    method: "GET",
    headers: {
      Accept: "application/json",
      "X-CSRF-Token": typeof getCSRFToken === "function" ? getCSRFToken() : "",
    },
    credentials: "same-origin",
    cache: "no-store",
  }).then((r) => (r.ok ? r.json() : null));
}

function applyDashboardHeroMetrics(data) {
  if (!data) return;
  const tv = document.querySelector(".hero-stat.hs-tv .hs-val");
  const mov = document.querySelector(".hero-stat.hs-mov .hs-val");
  const upgTv = document.querySelector(".hero-stat.hs-upg-tv .hs-val");
  const upgMov = document.querySelector(".hero-stat.hs-upg-mov .hs-val");
  if (tv) {
    tv.textContent = String(data.sonarr_missing ?? 0);
    tv.setAttribute("data-target", String(data.sonarr_missing ?? 0));
  }
  if (mov) {
    mov.textContent = String(data.radarr_missing ?? 0);
    mov.setAttribute("data-target", String(data.radarr_missing ?? 0));
  }
  if (upgTv) {
    upgTv.textContent = String(data.sonarr_upgrades ?? 0);
    upgTv.setAttribute("data-target", String(data.sonarr_upgrades ?? 0));
  }
  if (upgMov) {
    upgMov.textContent = String(data.radarr_upgrades ?? 0);
    upgMov.setAttribute("data-target", String(data.radarr_upgrades ?? 0));
  }

  const emM = document.getElementById("dash-emby-matched");
  if (emM) emM.textContent = String(data.emby_matched ?? 0);
  setMetricTile("emby-matched", data.emby_matched ?? 0);
}

const _FETCHER_PHASE_PILL = {
  processing: "status-pill-active",
  idle: "status-pill-idle",
  active: "status-pill-ok",
};

function applyDashboardFetcherPhase(data) {
  if (!data) return;
  const pill = document.getElementById("dash-fetcher-phase-pill");
  const detail = document.getElementById("dash-fetcher-phase-detail");
  if (!pill || !detail) return;
  const id = data.fetcher_phase || "active";
  pill.textContent = data.fetcher_phase_label || "Active";
  detail.textContent = data.fetcher_phase_detail || "";
  const cls = _FETCHER_PHASE_PILL[id] || "status-pill-ok";
  pill.className = `status-pill ${cls}`;
}

function applyDashboardAutomationHints(data) {
  if (!data) return;
  function setHint(elId, text) {
    const el = document.getElementById(elId);
    if (!el) return;
    const t = text == null ? "" : String(text).trim();
    el.textContent = t;
    el.hidden = !t;
  }
  setHint("dash-sonarr-hint", data.sonarr_automation_sub);
  setHint("dash-radarr-hint", data.radarr_automation_sub);
  setHint("dash-trimmer-hint", data.trimmer_automation_sub);
}

function applyDashboardAutomationStatus(data) {
  if (!data) return;
  applyDashboardFetcherPhase(data);
  applyDashboardAutomationHints(data);

  function setNextTick(tickId, relId, localVal, relVal) {
    const tick = document.getElementById(tickId);
    const rel = relId ? document.getElementById(relId) : null;
    if (tick) {
      if (localVal) tick.textContent = localVal;
      else tick.innerHTML = '<span class="muted automation-value-pending">Scheduled</span>';
    }
    if (rel) {
      if (relVal) {
        rel.textContent = `(${relVal})`;
        rel.hidden = false;
      } else {
        rel.textContent = "";
        rel.hidden = true;
      }
    }
  }

  setNextTick("dash-next-sonarr-tick", "dash-next-sonarr-rel", data.next_sonarr_tick_local, data.next_sonarr_relative);
  setNextTick("dash-next-radarr-tick", "dash-next-radarr-rel", data.next_radarr_tick_local, data.next_radarr_relative);
  setNextTick("dash-next-trimmer-tick", "dash-next-trimmer-rel", data.next_trimmer_tick_local, data.next_trimmer_relative);

  const lastContext = document.getElementById("dash-last-context");
  const lastHost = document.getElementById("dash-automation-last");
  if (lastHost && data.latest_system_event) {
    lastHost.className = "automation-card-subline";
    const ev = data.latest_system_event;
    const ok = ev.ok
      ? '<span class="status-pill status-pill-ok">Succeeded</span>'
      : '<span class="status-pill status-pill-fail">Failed</span>';
    if (lastContext) lastContext.innerHTML = escapeHtml(ev.context || "System • Event");
    const rel = escapeHtml(ev.relative || "");
    const clock = escapeHtml(ev.time_local || "");
    lastHost.innerHTML = `<span class="muted" id="dash-last-started-rel">${rel}</span> <span class="small muted">· <span id="dash-last-started">${clock}</span></span> ${ok}`;
  } else if (lastHost && !data.latest_system_event) {
    if (lastContext) lastContext.innerHTML = '<span class="muted">No activity yet</span>';
    lastHost.className = "automation-card-subline muted";
    lastHost.textContent = "No activity yet";
  }

  const lastSonarr = document.getElementById("dash-last-sonarr-run");
  if (lastSonarr) {
    const r = data.last_sonarr_run || {};
    if (r.time_local) {
      const rel = escapeHtml(r.relative || r.time_local || "");
      const ok =
        r.ok === true
          ? ' <span class="status-pill status-pill-ok">Succeeded</span>'
          : r.ok === false
            ? ' <span class="status-pill status-pill-fail">Failed</span>'
            : "";
      lastSonarr.innerHTML = `<span class="muted">Last run:</span> <strong id="dash-last-sonarr-rel">${rel}</strong> <span class="small muted">· ${escapeHtml(r.time_local)}</span>${ok}`;
    } else lastSonarr.innerHTML = '<span class="muted">Not yet run</span>';
  }
  const lastRadarr = document.getElementById("dash-last-radarr-run");
  if (lastRadarr) {
    const r = data.last_radarr_run || {};
    if (r.time_local) {
      const rel = escapeHtml(r.relative || r.time_local || "");
      const ok =
        r.ok === true
          ? ' <span class="status-pill status-pill-ok">Succeeded</span>'
          : r.ok === false
            ? ' <span class="status-pill status-pill-fail">Failed</span>'
            : "";
      lastRadarr.innerHTML = `<span class="muted">Last run:</span> <strong id="dash-last-radarr-rel">${rel}</strong> <span class="small muted">· ${escapeHtml(r.time_local)}</span>${ok}`;
    } else lastRadarr.innerHTML = '<span class="muted">Not yet run</span>';
  }
  const lastTrimmer = document.getElementById("dash-last-trimmer-run");
  if (lastTrimmer) {
    const r = data.last_trimmer_run || {};
    if (r.time_local) {
      const rel = escapeHtml(r.relative || r.time_local || "");
      const ok =
        r.ok === true
          ? ' <span class="status-pill status-pill-ok">Succeeded</span>'
          : r.ok === false
            ? ' <span class="status-pill status-pill-fail">Failed</span>'
            : "";
      lastTrimmer.innerHTML = `<span class="muted">Last run:</span> <strong id="dash-last-trimmer-rel">${rel}</strong> <span class="small muted">· ${escapeHtml(r.time_local)}</span>${ok}`;
    } else lastTrimmer.innerHTML = '<span class="muted">Not yet run</span>';
  }
}

function applyDashboardStatusPayload(data) {
  applyDashboardHeroMetrics(data);
  applyDashboardAutomationStatus(data);
}

const _heroMetricsPoll = {
  minGapMs: 3500,
  lastStart: 0,
  inFlight: false,
  queued: false,
  rescheduleTimer: null,
};

function runHeroMetricsFetchFromServer() {
  if (_heroMetricsPoll.inFlight) {
    _heroMetricsPoll.queued = true;
    return;
  }
  _heroMetricsPoll.inFlight = true;
  fetchDashboardStatusJson()
    .then((payload) => {
      if (payload) applyDashboardHeroMetrics(payload);
    })
    .catch(() => {})
    .finally(() => {
      _heroMetricsPoll.inFlight = false;
      if (_heroMetricsPoll.queued) {
        _heroMetricsPoll.queued = false;
        runHeroMetricsFetchFromServer();
      }
    });
}

function requestHeroMetricsRefresh() {
  if (!document.getElementById("dashboard-hero-stats")) return;
  const now = Date.now();
  const gap = now - _heroMetricsPoll.lastStart;
  if (gap < _heroMetricsPoll.minGapMs) {
    if (_heroMetricsPoll.rescheduleTimer !== null) {
      window.clearTimeout(_heroMetricsPoll.rescheduleTimer);
    }
    _heroMetricsPoll.rescheduleTimer = window.setTimeout(() => {
      _heroMetricsPoll.rescheduleTimer = null;
      requestHeroMetricsRefresh();
    }, _heroMetricsPoll.minGapMs - gap);
    return;
  }
  _heroMetricsPoll.lastStart = Date.now();
  runHeroMetricsFetchFromServer();
}

function startHeroMetricsPolling() {
  const root = document.getElementById("dashboard-hero-stats");
  if (!root) return;
  const tiles = root.querySelectorAll(".hero-stat[data-hero-poll-ms]");
  tiles.forEach((tile) => {
    const raw = tile.getAttribute("data-hero-poll-ms");
    const ms = parseInt(raw, 10);
    if (!Number.isFinite(ms) || ms < 8000) return;
    window.setInterval(requestHeroMetricsRefresh, ms);
  });
  window.fetcherHeroMetricsPollNow = requestHeroMetricsRefresh;
  requestHeroMetricsRefresh();
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") requestHeroMetricsRefresh();
  });
}

function startDashboardStatusPolling() {
  if (!document.getElementById("dashboard-hero-stats")) return;
  const intervalMs = 60000;
  let timerId = null;
  const pollAutomation = () => {
    if (document.visibilityState === "hidden") return;
    fetchDashboardStatusJson()
      .then((data) => {
        if (data) applyDashboardAutomationStatus(data);
      })
      .catch(() => {});
  };
  const pollFull = () => {
    if (document.visibilityState === "hidden") return;
    fetchDashboardStatusJson()
      .then((data) => {
        if (data) applyDashboardStatusPayload(data);
      })
      .catch(() => {});
  };
  window.fetcherDashboardPollNow = pollFull;
  const arm = () => {
    if (timerId !== null) {
      clearInterval(timerId);
      timerId = null;
    }
    timerId = window.setInterval(pollAutomation, intervalMs);
  };
  pollAutomation();
  arm();
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      if (timerId !== null) {
        clearInterval(timerId);
        timerId = null;
      }
    } else {
      pollAutomation();
      arm();
    }
  });
}

function replaceLiveRegionFromUrl(targetSelector, sourceSelector, url, afterSwap) {
  const target = document.querySelector(targetSelector);
  if (!target) return Promise.resolve();
  return fetch(url, {
    method: "GET",
    headers: { Accept: "text/html" },
    credentials: "same-origin",
    cache: "no-store",
  })
    .then((r) => (r.ok ? r.text() : null))
    .then((html) => {
      if (!html) return;
      const doc = new DOMParser().parseFromString(html, "text/html");
      const src = doc.querySelector(sourceSelector);
      if (!src) return;
      target.outerHTML = src.outerHTML;
      if (typeof afterSwap === "function") afterSwap();
    })
    .catch(() => {});
}

function startLiveTilePolling() {
  const intervalMs = 60000;
  let timerId = null;
  const isDashboard = Boolean(document.getElementById("dashboard-hero-stats"));
  const isActivityPage = Boolean(document.getElementById("activity-live-root"));
  const isLogsPage = Boolean(document.getElementById("logs-live-root"));
  if (!isDashboard && !isActivityPage && !isLogsPage) return;

  const poll = () => {
    if (document.visibilityState === "hidden") return;
    if (isDashboard) {
      replaceLiveRegionFromUrl(
        "#dashboard-activity-live-root",
        "#dashboard-activity-live-root",
        "/",
        () => {
          initActivityDetailExpand();
          document.querySelectorAll("#dashboard-activity-live-root .activity-row").forEach((el) => {
            el.classList.add("anim-in");
          });
        }
      );
    }
    if (isActivityPage) {
      replaceLiveRegionFromUrl("#activity-live-root", "#activity-live-root", "/activity", () => {
        initActivityFilterPills();
        initActivityDetailExpand();
        document.querySelectorAll("#activity-live-root .activity-row").forEach((el) => {
          el.classList.add("anim-in");
        });
      });
    }
    if (isLogsPage) {
      replaceLiveRegionFromUrl("#logs-live-root", "#logs-live-root", "/logs", () => {
        document.querySelectorAll("#logs-live-root .log-entry").forEach((el) => {
          el.classList.add("anim-in");
        });
      });
    }
  };
  window.fetcherLiveTilesPollNow = poll;

  const arm = () => {
    if (timerId !== null) {
      clearInterval(timerId);
      timerId = null;
    }
    timerId = window.setInterval(poll, intervalMs);
  };

  poll();
  arm();
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      if (timerId !== null) {
        clearInterval(timerId);
        timerId = null;
      }
    } else {
      poll();
      arm();
    }
  });
}

/** Section Save + Arr Test POSTs: ask server for JSON instead of 303 (transport only; same routes as full form POST). */
const FETCHER_SETTINGS_INPLACE_JSON_HEADER = "X-Fetcher-Settings-Async";
const FETCHER_SETTINGS_SAVE_REASON_TEXT = {
  db_busy: "The database was busy — try again in a moment.",
  db_error: "Save failed — try again.",
  invalid: "Save failed — check this form and try again.",
  invalid_scope: "Save failed — reload the page and try again.",
  sonarr_retry_delay_min: "Save failed — Sonarr Retry Delay must be at least 1 minute.",
  radarr_retry_delay_min: "Save failed — Radarr Retry Delay must be at least 1 minute.",
  error: "Save failed — try again.",
};
const FETCHER_SETTINGS_SAVE_PENDING_LABEL = "Saving…";
const FETCHER_SETTINGS_SAVE_SUCCESS_BANNER_BY_TAB = {
  global: "Global settings saved.",
  sonarr: "Sonarr settings saved.",
  radarr: "Radarr settings saved.",
};
const FETCHER_SETTINGS_TEST_PENDING_LABEL = "Testing…";
const FETCHER_SETTINGS_TEST_RESULT_TEXT = {
  sonarr_ok: "Sonarr connection succeeded.",
  sonarr_fail: "Sonarr connection failed — check URL, API key, and that Sonarr is reachable.",
  radarr_ok: "Radarr connection succeeded.",
  radarr_fail: "Radarr connection failed — check URL, API key, and that Radarr is reachable.",
};

const FETCHER_SETUP_INPLACE_JSON_HEADER = "X-Fetcher-Setup-Async";
const FETCHER_SETUP_SAVE_SUCCESS_FLASH = "Saved.";
const FETCHER_SETUP_ERROR_TEXT = {
  short_password: "Password must be at least 8 characters.",
  account_required: "Create an account password to continue (cannot skip this step).",
};

function clearSettingsSaveFeedbackTimer(form) {
  const t = form._fetcherSaveFeedbackTimer;
  if (t) {
    window.clearTimeout(t);
    form._fetcherSaveFeedbackTimer = null;
  }
}

function clearTrimmerSettingsSaveFeedbackTimer(form) {
  const t = form._trimmerSaveFeedbackTimer;
  if (t) {
    window.clearTimeout(t);
    form._trimmerSaveFeedbackTimer = null;
  }
}

function clearSetupWizardSaveFeedbackTimer(form) {
  const t = form._fetcherSetupFeedbackTimer;
  if (t) {
    window.clearTimeout(t);
    form._fetcherSetupFeedbackTimer = null;
  }
}

/** Section-local banner strip for Fetcher /settings in-place Save + Test only (not other pages). */
function setFetcherSettingsInPlaceFeedback(feedbackEl, kind, text) {
  if (!feedbackEl) return;
  if (!text) {
    feedbackEl.textContent = "";
    feedbackEl.hidden = true;
    feedbackEl.className = "settings-async-feedback";
    feedbackEl.removeAttribute("role");
    feedbackEl.removeAttribute("aria-busy");
    return;
  }
  feedbackEl.textContent = text;
  feedbackEl.hidden = false;
  if (kind === "ok") {
    feedbackEl.className = "settings-async-feedback banner banner-ok";
    feedbackEl.setAttribute("role", "status");
    feedbackEl.removeAttribute("aria-busy");
  } else if (kind === "err") {
    feedbackEl.className = "settings-async-feedback banner banner-warn";
    feedbackEl.setAttribute("role", "alert");
    feedbackEl.removeAttribute("aria-busy");
  } else if (kind === "pending") {
    feedbackEl.className = "settings-async-feedback settings-async-feedback--pending";
    feedbackEl.setAttribute("role", "status");
    feedbackEl.setAttribute("aria-busy", "true");
  }
}

function stashSaveButtonLabels(saveButtons) {
  saveButtons.forEach((btn) => {
    if (btn.dataset.fetcherSaveLabel == null || btn.dataset.fetcherSaveLabel === "") {
      btn.dataset.fetcherSaveLabel = (btn.textContent || "").trim();
    }
  });
}

function restoreSaveButtonLabels(saveButtons) {
  saveButtons.forEach((btn) => {
    const orig = btn.dataset.fetcherSaveLabel;
    if (orig != null && orig !== "") btn.textContent = orig;
    btn.removeAttribute("aria-busy");
  });
}

function setSaveButtonsPending(saveButtons, pending) {
  saveButtons.forEach((btn) => {
    if (pending) {
      btn.textContent = FETCHER_SETTINGS_SAVE_PENDING_LABEL;
      btn.setAttribute("aria-busy", "true");
    }
  });
}

function syncFetcherSettingsUrlAfterInPlacePost(tabKey) {
  try {
    const u = new URL(window.location.href);
    if (tabKey) u.searchParams.set("tab", tabKey);
    ["saved", "save", "reason", "test", "import", "sec"].forEach((k) => u.searchParams.delete(k));
    const h = window.location.hash || "";
    history.replaceState(null, "", u.pathname + (u.search ? u.search : "") + h);
  } catch (_) {
    /* ignore */
  }
}

function fetcherScopeLabelFromTab(tabKey) {
  const t = String(tabKey || "").toLowerCase();
  if (t === "global") return "Global";
  if (t === "sonarr") return "Sonarr";
  if (t === "radarr") return "Radarr";
  return "Settings";
}

const TRIMMER_SETTINGS_INPLACE_JSON_HEADER = "X-Fetcher-Trimmer-Settings-Async";
const REFINER_SETTINGS_INPLACE_JSON_HEADER = "X-Fetcher-Refiner-Settings-Async";

/** Trimmer cleaner + connection: message must match the independently saved scope (never a vague “rules” blob). */
function trimmerSaveSuccessMessage(section, saveScope) {
  const sec = String(section || "").toLowerCase();
  const sc = String(saveScope || "").toLowerCase();
  if (sec === "connection" || sc === "connection") {
    return "Trimmer settings saved (Emby connection).";
  }
  if (sec === "schedule" && sc === "schedule") {
    return "Trimmer settings saved (Schedule & limits).";
  }
  if (sec === "rules" && sc === "tv") {
    return "Trimmer settings saved (TV rules).";
  }
  if (sec === "rules" && sc === "movies") {
    return "Trimmer settings saved (Movie rules).";
  }
  if (sec === "people" && sc === "tv") {
    return "Trimmer settings saved (TV people rules).";
  }
  if (sec === "people" && sc === "movies") {
    return "Trimmer settings saved (Movie people rules).";
  }
  return "Trimmer settings saved.";
}

const TRIMMER_EMBY_TEST_RESULT_TEXT = {
  emby_ok: "Emby connection succeeded.",
  emby_fail: "Emby connection failed — check URL, API key, and that the server is reachable.",
};

/** Trimmer /trimmer/settings: strip flash query params; keep hash on the active section (independent from Fetcher `tab=`). */
function syncTrimmerSettingsUrlAfterInPlacePost(sectionKey) {
  try {
    const u = new URL(window.location.href);
    ["saved", "save", "reason", "test", "trimmer_saved", "refiner_saved"].forEach((k) =>
      u.searchParams.delete(k),
    );
    const sk = (sectionKey || "").trim().toLowerCase();
    const frag =
      sk === "connection"
        ? "trimmer-connection"
        : sk === "schedule"
          ? "trimmer-schedule"
          : sk === "rules"
            ? "trimmer-rules"
            : sk === "people"
              ? "trimmer-people"
              : "";
    const h = frag ? "#" + frag : window.location.hash || "";
    history.replaceState(null, "", u.pathname + (u.search ? u.search : "") + h);
  } catch (_) {
    /* ignore */
  }
}

function refinerSaveScopeLabel(section) {
  const s = String(section || "").toLowerCase();
  if (s === "processing") return "Processing";
  if (s === "folders") return "Folders";
  if (s === "audio") return "Audio";
  if (s === "subtitles") return "Subtitles";
  if (s === "schedule") return "Schedule & limits";
  return "Refiner";
}

function refinerSaveSuccessMessage(section) {
  const s = String(section || "").toLowerCase();
  if (s === "processing") return "Refiner settings saved (Processing).";
  if (s === "folders") return "Refiner settings saved (Folders).";
  if (s === "audio") return "Refiner settings saved (Audio).";
  if (s === "subtitles") return "Refiner settings saved (Subtitles).";
  if (s === "schedule") return "Refiner settings saved (Schedule & limits).";
  return "Refiner settings saved.";
}

function refinerSaveFailMessage(section, reason, message) {
  const scope = refinerSaveScopeLabel(section);
  const msg = message && String(message).trim();
  if (msg) return `Could not save (${scope}) — ${msg}`;
  const r = String(reason || "error");
  return `Could not save (${scope}) — try again. (${r})`;
}

function syncRefinerSettingsUrlAfterInPlacePost(sectionKey) {
  try {
    const u = new URL(window.location.href);
    ["saved", "save", "reason", "refiner_saved", "refiner_section"].forEach((k) => u.searchParams.delete(k));
    const sk = (sectionKey || "").trim().toLowerCase();
    const fragMap = {
      processing: "refiner-processing",
      folders: "refiner-folders",
      audio: "refiner-audio",
      subtitles: "refiner-subtitles",
      schedule: "refiner-schedule",
    };
    const fragId = fragMap[sk] || "";
    const h = fragId ? "#" + fragId : window.location.hash || "";
    history.replaceState(null, "", u.pathname + (u.search ? u.search : "") + h);
  } catch (_) {
    /* ignore */
  }
}

function clearRefinerSettingsSaveFeedbackTimer(form) {
  const t = form._refinerSaveFeedbackTimer;
  if (t) {
    window.clearTimeout(t);
    form._refinerSaveFeedbackTimer = null;
  }
}

/** In-place save + Emby test for Trimmer connection form (separate header from Fetcher settings). */
function initTrimmerSettingsAsyncConnection() {
  document.querySelectorAll('form[data-fetcher-trimmer-async-connection="1"]').forEach((form) => {
    form.addEventListener("submit", (e) => {
      if (form.dataset.fetcherTrimmerSaving === "1") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      const sub = e.submitter;
      let action = form.getAttribute("action") || "/trimmer/settings/connection";
      let isTest = false;
      if (sub instanceof HTMLButtonElement && sub.getAttribute("formaction")) {
        const fa = sub.getAttribute("formaction") || "";
        action = fa;
        isTest = fa.indexOf("emby-form") >= 0;
      }
      const feedback = form.querySelector(".settings-async-feedback");
      const saveButtons = Array.from(form.querySelectorAll('button[type="submit"]')).filter(
        (b) => !b.getAttribute("form"),
      );

      function setBusy(on) {
        form.dataset.fetcherTrimmerSaving = on ? "1" : "";
        form.setAttribute("aria-busy", on ? "true" : "false");
        saveButtons.forEach((btn) => {
          btn.disabled = on;
        });
      }

      clearTrimmerSettingsSaveFeedbackTimer(form);
      form._fetcherTrimmerSaveGen = (form._fetcherTrimmerSaveGen || 0) + 1;
      const saveGen = form._fetcherTrimmerSaveGen;

      stashSaveButtonLabels(saveButtons);
      setBusy(true);

      if (isTest) {
        saveButtons.forEach((btn) => {
          if (btn.dataset.fetcherTrimmerTestLabel == null || btn.dataset.fetcherTrimmerTestLabel === "") {
            btn.dataset.fetcherTrimmerTestLabel = (btn.textContent || "").trim();
          }
          btn.textContent = FETCHER_SETTINGS_TEST_PENDING_LABEL;
          btn.setAttribute("aria-busy", "true");
        });
        setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_TEST_PENDING_LABEL);
        const fd =
          sub instanceof HTMLButtonElement || sub instanceof HTMLInputElement
            ? new FormData(form, sub)
            : new FormData(form);
        fetch(action, {
          method: "POST",
          body: fd,
          credentials: "same-origin",
          headers: { [TRIMMER_SETTINGS_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
        })
          .then((res) => {
            if (res.status === 403) {
              return Promise.reject(
                new Error("Your session may have expired — reload the page and sign in again."),
              );
            }
            const ct = (res.headers.get("content-type") || "").toLowerCase();
            if (ct.indexOf("application/json") >= 0) {
              return res.json().then((data) => ({ res, data }));
            }
            return res.text().then((text) => ({ res, data: null, text }));
          })
          .then((out) => {
            const { res, data, text } = out;
            if (!res.ok && !data) {
              throw new Error(text || "Connection test failed — try again.");
            }
            if (data && typeof data.ok === "boolean" && data.test) {
              const k = String(data.test);
              const msg = TRIMMER_EMBY_TEST_RESULT_TEXT[k];
              if (data.ok) {
                setFetcherSettingsInPlaceFeedback(feedback, "ok", msg || "Connection succeeded.");
                syncTrimmerSettingsUrlAfterInPlacePost(data.section || "connection");
                clearTrimmerSettingsSaveFeedbackTimer(form);
                form._trimmerSaveFeedbackTimer = window.setTimeout(() => {
                  form._trimmerSaveFeedbackTimer = null;
                  if (form._fetcherTrimmerSaveGen !== saveGen) return;
                  if (feedback && msg && feedback.textContent === msg) {
                    setFetcherSettingsInPlaceFeedback(feedback, "ok", "");
                  }
                }, 3200);
              } else {
                syncTrimmerSettingsUrlAfterInPlacePost(data.section || "connection");
                setFetcherSettingsInPlaceFeedback(feedback, "err", msg || "Connection failed.");
              }
              return;
            }
            throw new Error("Connection test failed — try again.");
          })
          .catch((err) => {
            const m = err && err.message ? err.message : "Connection test failed — try again.";
            setFetcherSettingsInPlaceFeedback(feedback, "err", m);
          })
          .finally(() => {
            saveButtons.forEach((btn) => {
              btn.removeAttribute("aria-busy");
              const o = btn.dataset.fetcherTrimmerTestLabel;
              if (o != null && o !== "") btn.textContent = o;
            });
            restoreSaveButtonLabels(saveButtons);
            setBusy(false);
          });
        return;
      }

      setSaveButtonsPending(saveButtons, true);
      setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_SAVE_PENDING_LABEL);
      const fd =
        sub instanceof HTMLButtonElement || sub instanceof HTMLInputElement
          ? new FormData(form, sub)
          : new FormData(form);
      fetch(action, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
        headers: { [TRIMMER_SETTINGS_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
      })
        .then((res) => {
          if (res.status === 403) {
            return Promise.reject(
              new Error("Your session may have expired — reload the page and sign in again."),
            );
          }
          const ct = (res.headers.get("content-type") || "").toLowerCase();
          if (ct.indexOf("application/json") >= 0) {
            return res.json().then((data) => ({ res, data }));
          }
          return res.text().then((text) => ({ res, data: null, text }));
        })
        .then((out) => {
          const { res, data, text } = out;
          if (!res.ok && !data) {
            throw new Error(text || "Save failed — try again.");
          }
          if (data && typeof data.ok === "boolean") {
            if (data.ok) {
              const section = String(data.section || "connection");
              const saveScope = String(data.save_scope || "");
              const okMsg = trimmerSaveSuccessMessage(section, saveScope);
              setFetcherSettingsInPlaceFeedback(feedback, "ok", okMsg);
              syncTrimmerSettingsUrlAfterInPlacePost(section);
              clearTrimmerSettingsSaveFeedbackTimer(form);
              form._trimmerSaveFeedbackTimer = window.setTimeout(() => {
                form._trimmerSaveFeedbackTimer = null;
                if (form._fetcherTrimmerSaveGen !== saveGen) return;
                if (feedback && feedback.textContent === okMsg) {
                  setFetcherSettingsInPlaceFeedback(feedback, "ok", "");
                }
              }, 2600);
              return;
            }
            syncTrimmerSettingsUrlAfterInPlacePost(data.section || "connection");
            const r = data.reason ? String(data.reason) : "error";
            const baseMsg = FETCHER_SETTINGS_SAVE_REASON_TEXT[r] || FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
            const msg =
              r === "invalid_scope"
                ? baseMsg
                : `Trimmer save failed — ${baseMsg.replace(/^Save failed —\s*/i, "")}`;
            setFetcherSettingsInPlaceFeedback(feedback, "err", msg);
            return;
          }
          throw new Error(FETCHER_SETTINGS_SAVE_REASON_TEXT.error);
        })
        .catch((err) => {
          const m = err && err.message ? err.message : FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
          setFetcherSettingsInPlaceFeedback(feedback, "err", m);
        })
        .finally(() => {
          restoreSaveButtonLabels(saveButtons);
          setBusy(false);
        });
    });
  });
}

/** In-place save for Trimmer cleaner form (schedule / rules / people scopes — separate routes from Fetcher). */
function initTrimmerSettingsAsyncCleaner() {
  document.querySelectorAll('form[data-fetcher-trimmer-async-cleaner="1"]').forEach((form) => {
    form.addEventListener("submit", (e) => {
      if (form.dataset.fetcherTrimmerSaving === "1") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      let sub = e.submitter;
      if (!(sub instanceof HTMLButtonElement) && !(sub instanceof HTMLInputElement)) {
        const ae = document.activeElement;
        if (ae instanceof HTMLButtonElement && ae.form === form) {
          sub = ae;
        }
      }
      let action =
        sub instanceof HTMLButtonElement && sub.getAttribute("formaction")
          ? sub.getAttribute("formaction")
          : form.getAttribute("action") || "/trimmer/settings/cleaner";
      const anchor =
        (sub &&
          sub.closest &&
          sub.closest(".trimmer-settings-anchor")) ||
        form.querySelector(".trimmer-settings-anchor");
      const feedback = anchor ? anchor.querySelector(".settings-async-feedback") : null;
      const saveButtons = Array.from(form.querySelectorAll('button[type="submit"]')).filter(
        (b) => !b.getAttribute("form"),
      );

      function setBusy(on) {
        form.dataset.fetcherTrimmerSaving = on ? "1" : "";
        form.setAttribute("aria-busy", on ? "true" : "false");
        saveButtons.forEach((btn) => {
          btn.disabled = on;
        });
      }

      clearTrimmerSettingsSaveFeedbackTimer(form);
      form._fetcherTrimmerSaveGen = (form._fetcherTrimmerSaveGen || 0) + 1;
      const saveGen = form._fetcherTrimmerSaveGen;

      stashSaveButtonLabels(saveButtons);
      setBusy(true);
      setSaveButtonsPending(saveButtons, true);
      setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_SAVE_PENDING_LABEL);

      let fd;
      if (sub instanceof HTMLButtonElement || sub instanceof HTMLInputElement) {
        fd = new FormData(form, sub);
      } else {
        fd = new FormData(form);
      }
      if (
        sub instanceof HTMLButtonElement &&
        sub.getAttribute("name") === "save_scope" &&
        sub.getAttribute("value")
      ) {
        fd.set("save_scope", sub.getAttribute("value") || "");
        const fa = sub.getAttribute("formaction");
        if (fa) action = fa;
      }
      fetch(action, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
        headers: { [TRIMMER_SETTINGS_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
      })
        .then((res) => {
          if (res.status === 403) {
            return Promise.reject(
              new Error("Your session may have expired — reload the page and sign in again."),
            );
          }
          const ct = (res.headers.get("content-type") || "").toLowerCase();
          if (ct.indexOf("application/json") >= 0) {
            return res.json().then((data) => ({ res, data }));
          }
          return res.text().then((text) => ({ res, data: null, text }));
        })
        .then((out) => {
          const { res, data, text } = out;
          if (!res.ok && !data) {
            throw new Error(text || "Save failed — try again.");
          }
          if (data && typeof data.ok === "boolean") {
            if (data.ok) {
              const section = String(data.section || "connection");
              const saveScope = String(data.save_scope || "");
              const okMsg = trimmerSaveSuccessMessage(section, saveScope);
              setFetcherSettingsInPlaceFeedback(feedback, "ok", okMsg);
              syncTrimmerSettingsUrlAfterInPlacePost(section);
              clearTrimmerSettingsSaveFeedbackTimer(form);
              form._trimmerSaveFeedbackTimer = window.setTimeout(() => {
                form._trimmerSaveFeedbackTimer = null;
                if (form._fetcherTrimmerSaveGen !== saveGen) return;
                if (feedback && feedback.textContent === okMsg) {
                  setFetcherSettingsInPlaceFeedback(feedback, "ok", "");
                }
              }, 2600);
              return;
            }
            syncTrimmerSettingsUrlAfterInPlacePost(data.section || "");
            const r = data.reason ? String(data.reason) : "error";
            const baseMsg = FETCHER_SETTINGS_SAVE_REASON_TEXT[r] || FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
            const msg =
              r === "invalid_scope"
                ? baseMsg
                : `Trimmer save failed — ${baseMsg.replace(/^Save failed —\s*/i, "")}`;
            setFetcherSettingsInPlaceFeedback(feedback, "err", msg);
            return;
          }
          throw new Error(FETCHER_SETTINGS_SAVE_REASON_TEXT.error);
        })
        .catch((err) => {
          const m = err && err.message ? err.message : FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
          setFetcherSettingsInPlaceFeedback(feedback, "err", m);
        })
        .finally(() => {
          restoreSaveButtonLabels(saveButtons);
          setBusy(false);
        });
    });
  });
}

/**
 * Sync top Refiner banners from GET /api/refiner/readiness-brief only.
 * Phase: !enabled → off; enabled && issues.length → not_ready; else → ready.
 * Optional form + generation counter drops stale responses when saves overlap.
 */
function syncRefinerTopBannersFromServerBrief(form) {
  let gen = null;
  if (form) {
    form._refinerBriefGen = (form._refinerBriefGen || 0) + 1;
    gen = form._refinerBriefGen;
  }
  return fetch("/api/refiner/readiness-brief", {
    method: "GET",
    credentials: "same-origin",
    headers: { Accept: "application/json" },
  })
    .then((res) => {
      if (!res.ok) return null;
      return res.json();
    })
    .then((data) => {
      if (form && gen !== null && form._refinerBriefGen !== gen) return;
      if (!data || typeof data.enabled !== "boolean") return;
      const off = document.getElementById("refiner-banner-off");
      const readyBanner = document.getElementById("refiner-banner-readiness");
      const list = document.getElementById("refiner-readiness-list");
      if (!off || !readyBanner) return;
      const issues = Array.isArray(data.issues) ? data.issues : [];
      let phase;
      if (!data.enabled) phase = "off";
      else if (issues.length > 0) phase = "not_ready";
      else phase = "ready";
      off.hidden = phase !== "off";
      readyBanner.hidden = phase !== "not_ready";
      if (list) {
        if (phase === "not_ready") {
          const onSettings = (window.location.pathname || "").indexOf("/refiner/settings") >= 0;
          list.innerHTML = issues
            .map((it) => {
              const msg = escapeHtml(String((it && it.message) || ""));
              const a = String((it && it.anchor) || "");
              if (onSettings && a) {
                return `<li><a href="#${escapeHtml(a)}">${msg}</a></li>`;
              }
              return `<li>${msg}</li>`;
            })
            .join("");
        } else {
          list.innerHTML = "";
        }
      }
    })
    .catch(() => {});
}

/** In-place save for Refiner settings (same fetch + JSON contract as Trimmer cleaner). */
function initRefinerSettingsAsyncSave() {
  document.querySelectorAll('form[data-fetcher-refiner-async="1"]').forEach((form) => {
    form.addEventListener("submit", (e) => {
      if (form.dataset.fetcherRefinerSaving === "1") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      let sub = e.submitter;
      if (!(sub instanceof HTMLButtonElement) && !(sub instanceof HTMLInputElement)) {
        const ae = document.activeElement;
        if (ae instanceof HTMLButtonElement && ae.form === form) {
          sub = ae;
        }
      }
      let action =
        sub instanceof HTMLButtonElement && sub.getAttribute("formaction")
          ? sub.getAttribute("formaction")
          : form.getAttribute("action") || "/refiner/settings/save";
      const anchor =
        (sub && sub.closest && sub.closest(".refiner-settings-anchor")) ||
        form.querySelector(".refiner-settings-anchor");
      const feedback = anchor ? anchor.querySelector(".settings-async-feedback") : null;
      const saveButtons = Array.from(form.querySelectorAll('button[type="submit"]')).filter(
        (b) => !b.getAttribute("form"),
      );

      function setBusy(on) {
        form.dataset.fetcherRefinerSaving = on ? "1" : "";
        form.setAttribute("aria-busy", on ? "true" : "false");
        saveButtons.forEach((btn) => {
          btn.disabled = on;
        });
      }

      clearRefinerSettingsSaveFeedbackTimer(form);
      form._fetcherRefinerSaveGen = (form._fetcherRefinerSaveGen || 0) + 1;
      const saveGen = form._fetcherRefinerSaveGen;

      stashSaveButtonLabels(saveButtons);
      setBusy(true);
      setSaveButtonsPending(saveButtons, true);
      setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_SAVE_PENDING_LABEL);

      let fd;
      if (sub instanceof HTMLButtonElement || sub instanceof HTMLInputElement) {
        fd = new FormData(form, sub);
      } else {
        fd = new FormData(form);
      }
      fetch(action, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
        headers: { [REFINER_SETTINGS_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
      })
        .then((res) => {
          if (res.status === 403) {
            return Promise.reject(
              new Error("Your session may have expired — reload the page and sign in again."),
            );
          }
          const ct = (res.headers.get("content-type") || "").toLowerCase();
          if (ct.indexOf("application/json") >= 0) {
            return res.json().then((data) => ({ res, data }));
          }
          return res.text().then((text) => ({ res, data: null, text }));
        })
        .then((out) => {
          const { res, data, text } = out;
          if (!res.ok && !data) {
            throw new Error(text || "Save failed — try again.");
          }
          if (data && typeof data.ok === "boolean") {
            if (data.ok) {
              const section = String(data.section || "processing");
              const okMsg = refinerSaveSuccessMessage(section);
              setFetcherSettingsInPlaceFeedback(feedback, "ok", okMsg);
              syncRefinerSettingsUrlAfterInPlacePost(section);
              syncRefinerTopBannersFromServerBrief(form);
              clearRefinerSettingsSaveFeedbackTimer(form);
              form._refinerSaveFeedbackTimer = window.setTimeout(() => {
                form._refinerSaveFeedbackTimer = null;
                if (form._fetcherRefinerSaveGen !== saveGen) return;
                if (feedback && feedback.textContent === okMsg) {
                  setFetcherSettingsInPlaceFeedback(feedback, "ok", "");
                }
              }, 2600);
              return;
            }
            syncRefinerSettingsUrlAfterInPlacePost(data.section || "");
            const r = data.reason ? String(data.reason) : "error";
            const msg = refinerSaveFailMessage(data.section, r, data.message);
            setFetcherSettingsInPlaceFeedback(feedback, "err", msg);
            return;
          }
          throw new Error(FETCHER_SETTINGS_SAVE_REASON_TEXT.error);
        })
        .catch((err) => {
          const m = err && err.message ? err.message : FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
          setFetcherSettingsInPlaceFeedback(feedback, "err", m);
        })
        .finally(() => {
          restoreSaveButtonLabels(saveButtons);
          setBusy(false);
        });
    });
  });
}

/** Setup wizard: same transport pattern as settings (JSON + feedback); then one navigation to the next step. */
function initSetupWizardAsyncSave() {
  document.querySelectorAll('form[data-fetcher-setup-async="1"]').forEach((form) => {
    form.addEventListener("submit", (e) => {
      if (form.dataset.fetcherSetupSaving === "1") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      const sub = e.submitter;
      const action = form.getAttribute("action") || "/setup/0";
      const feedback = form.querySelector(".settings-async-feedback");
      const saveButtons = Array.from(form.querySelectorAll('button[type="submit"]'));

      function setBusy(on) {
        form.dataset.fetcherSetupSaving = on ? "1" : "";
        form.setAttribute("aria-busy", on ? "true" : "false");
        saveButtons.forEach((btn) => {
          btn.disabled = on;
        });
      }

      clearSetupWizardSaveFeedbackTimer(form);
      form._fetcherSetupSaveGen = (form._fetcherSetupSaveGen || 0) + 1;
      const saveGen = form._fetcherSetupSaveGen;

      stashSaveButtonLabels(saveButtons);
      setBusy(true);
      setSaveButtonsPending(saveButtons, true);
      setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_SAVE_PENDING_LABEL);

      const fd =
        sub instanceof HTMLButtonElement || sub instanceof HTMLInputElement
          ? new FormData(form, sub)
          : new FormData(form);

      fetch(action, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
        headers: { [FETCHER_SETUP_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
      })
        .then((res) => {
          if (res.status === 403) {
            return Promise.reject(
              new Error("Your session may have expired — reload the page and try again."),
            );
          }
          const ct = (res.headers.get("content-type") || "").toLowerCase();
          if (ct.indexOf("application/json") >= 0) {
            return res.json().then((data) => ({ res, data }));
          }
          return res.text().then((text) => ({ res, data: null, text }));
        })
        .then((out) => {
          const { res, data, text } = out;
          if (!res.ok && !data) {
            throw new Error(text || "Save failed — try again.");
          }
          if (data && typeof data.ok === "boolean") {
            if (data.ok && data.redirect) {
              setFetcherSettingsInPlaceFeedback(feedback, "ok", FETCHER_SETUP_SAVE_SUCCESS_FLASH);
              clearSetupWizardSaveFeedbackTimer(form);
              form._fetcherSetupFeedbackTimer = window.setTimeout(() => {
                form._fetcherSetupFeedbackTimer = null;
                if (form._fetcherSetupSaveGen !== saveGen) return;
                window.location.assign(String(data.redirect));
              }, 280);
              return;
            }
            if (data.error) {
              const k = String(data.error);
              const msg = FETCHER_SETUP_ERROR_TEXT[k] || FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
              setFetcherSettingsInPlaceFeedback(feedback, "err", msg);
              return;
            }
            if (data.reason) {
              const r = String(data.reason);
              const msg = FETCHER_SETTINGS_SAVE_REASON_TEXT[r] || FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
              setFetcherSettingsInPlaceFeedback(feedback, "err", msg);
              return;
            }
          }
          throw new Error(FETCHER_SETTINGS_SAVE_REASON_TEXT.error);
        })
        .catch((err) => {
          const m = err && err.message ? err.message : FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
          setFetcherSettingsInPlaceFeedback(feedback, "err", m);
        })
        .finally(() => {
          restoreSaveButtonLabels(saveButtons);
          setBusy(false);
        });
    });
  });
}

/** In-place save for Fetcher /settings section forms (Global / Sonarr / Radarr); normal POST remains if JS disabled. */
function initFetcherSettingsAsyncSave() {
  const forms = document.querySelectorAll('form[data-fetcher-async-settings="1"]');
  if (!forms.length) return;

  forms.forEach((form) => {
    form.addEventListener("submit", (e) => {
      if (form.dataset.fetcherSettingsSaving === "1") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      const action = form.getAttribute("action") || "/settings";
      const feedback = form.querySelector(".settings-async-feedback");
      const saveButtons = Array.from(form.querySelectorAll('button[type="submit"]')).filter(
        (b) => !b.getAttribute("form"),
      );

      function setBusy(on) {
        form.dataset.fetcherSettingsSaving = on ? "1" : "";
        form.setAttribute("aria-busy", on ? "true" : "false");
        saveButtons.forEach((btn) => {
          btn.disabled = on;
        });
      }

      clearSettingsSaveFeedbackTimer(form);
      form._fetcherSaveGen = (form._fetcherSaveGen || 0) + 1;
      const saveGen = form._fetcherSaveGen;

      stashSaveButtonLabels(saveButtons);
      setBusy(true);
      setSaveButtonsPending(saveButtons, true);
      setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_SAVE_PENDING_LABEL);

      const sub = e.submitter;
      const fd =
        sub instanceof HTMLButtonElement || sub instanceof HTMLInputElement
          ? new FormData(form, sub)
          : new FormData(form);
      fetch(action, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
        headers: { [FETCHER_SETTINGS_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
      })
        .then((res) => {
          if (res.status === 403) {
            return Promise.reject(
              new Error("Your session may have expired — reload the page and sign in again."),
            );
          }
          const ct = (res.headers.get("content-type") || "").toLowerCase();
          if (ct.indexOf("application/json") >= 0) {
            return res.json().then((data) => ({ res, data }));
          }
          return res.text().then((text) => ({ res, data: null, text }));
        })
        .then((out) => {
          const { res, data, text } = out;
          if (!res.ok && !data) {
            throw new Error(text || "Save failed — try again.");
          }
          if (data && typeof data.ok === "boolean") {
            if (data.ok) {
              const tab = String(data.tab || "");
              const okMsg = FETCHER_SETTINGS_SAVE_SUCCESS_BANNER_BY_TAB[tab] || "Settings saved.";
              setFetcherSettingsInPlaceFeedback(feedback, "ok", okMsg);
              syncFetcherSettingsUrlAfterInPlacePost(tab);
              clearSettingsSaveFeedbackTimer(form);
              form._fetcherSaveFeedbackTimer = window.setTimeout(() => {
                form._fetcherSaveFeedbackTimer = null;
                if (form._fetcherSaveGen !== saveGen) return;
                if (feedback && feedback.textContent === okMsg) {
                  setFetcherSettingsInPlaceFeedback(feedback, "ok", "");
                }
              }, 2600);
              return;
            }
            if (data.tab) syncFetcherSettingsUrlAfterInPlacePost(data.tab);
            const r = data.reason ? String(data.reason) : "error";
            const baseMsg = FETCHER_SETTINGS_SAVE_REASON_TEXT[r] || FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
            const scopeLabel = fetcherScopeLabelFromTab(data.tab);
            const msg =
              r === "invalid_scope" ? baseMsg : `${scopeLabel} save failed — ${baseMsg.replace(/^Save failed —\s*/i, "")}`;
            setFetcherSettingsInPlaceFeedback(feedback, "err", msg);
            return;
          }
          throw new Error(FETCHER_SETTINGS_SAVE_REASON_TEXT.error);
        })
        .catch((err) => {
          const m = err && err.message ? err.message : FETCHER_SETTINGS_SAVE_REASON_TEXT.error;
          setFetcherSettingsInPlaceFeedback(feedback, "err", m);
        })
        .finally(() => {
          restoreSaveButtonLabels(saveButtons);
          setBusy(false);
        });
    });
  });
}

/** In-place Sonarr/Radarr connection tests (same CSRF + header contract as async save; no full-page navigation). */
function initFetcherSettingsAsyncTest() {
  document.querySelectorAll('form[data-fetcher-async-test="1"]').forEach((form) => {
    const panel = (form.getAttribute("data-fetcher-test-panel") || "").trim();
    if (panel !== "sonarr" && panel !== "radarr") return;
    const saveFormId = panel === "sonarr" ? "fetcher-settings-sonarr" : "fetcher-settings-radarr";
    const saveForm = document.getElementById(saveFormId);
    const feedback = saveForm ? saveForm.querySelector(".settings-async-feedback") : null;
    const fid = form.getAttribute("id") || "";
    const triggerBtns = fid ? Array.from(document.querySelectorAll(`button[form="${fid}"]`)) : [];
    let okFadeTimer = null;

    form.addEventListener("submit", (e) => {
      if (form.dataset.fetcherTestPending === "1") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      const action = form.getAttribute("action") || "/settings";

      triggerBtns.forEach((b) => {
        if (b.dataset.fetcherTestLabel == null || b.dataset.fetcherTestLabel === "") {
          b.dataset.fetcherTestLabel = (b.textContent || "").trim();
        }
        b.disabled = true;
        b.setAttribute("aria-busy", "true");
        b.textContent = FETCHER_SETTINGS_TEST_PENDING_LABEL;
      });
      form.dataset.fetcherTestPending = "1";
      setFetcherSettingsInPlaceFeedback(feedback, "pending", FETCHER_SETTINGS_TEST_PENDING_LABEL);

      const fd = new FormData(form);
      fetch(action, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
        headers: { [FETCHER_SETTINGS_INPLACE_JSON_HEADER]: "1", Accept: "application/json" },
      })
        .then((res) => {
          if (res.status === 403) {
            return Promise.reject(
              new Error("Your session may have expired — reload the page and sign in again."),
            );
          }
          const ct = (res.headers.get("content-type") || "").toLowerCase();
          if (ct.indexOf("application/json") >= 0) {
            return res.json().then((data) => ({ res, data }));
          }
          return res.text().then((text) => ({ res, data: null, text }));
        })
        .then((out) => {
          const { res, data, text } = out;
          if (!res.ok && !data) {
            throw new Error(text || "Connection test failed — try again.");
          }
          if (data && typeof data.ok === "boolean" && data.test) {
            const k = String(data.test);
            const msg = FETCHER_SETTINGS_TEST_RESULT_TEXT[k];
            if (data.ok) {
              setFetcherSettingsInPlaceFeedback(feedback, "ok", msg || "Connection succeeded.");
              syncFetcherSettingsUrlAfterInPlacePost(data.tab || panel);
              if (okFadeTimer) window.clearTimeout(okFadeTimer);
              okFadeTimer = window.setTimeout(() => {
                okFadeTimer = null;
                if (feedback && msg && feedback.textContent === msg) {
                  setFetcherSettingsInPlaceFeedback(feedback, "ok", "");
                }
              }, 3200);
            } else {
              if (data.tab) syncFetcherSettingsUrlAfterInPlacePost(data.tab);
              setFetcherSettingsInPlaceFeedback(feedback, "err", msg || "Connection failed.");
            }
            return;
          }
          throw new Error("Connection test failed — try again.");
        })
        .catch((err) => {
          const m = err && err.message ? err.message : "Connection test failed — try again.";
          setFetcherSettingsInPlaceFeedback(feedback, "err", m);
        })
        .finally(() => {
          form.dataset.fetcherTestPending = "";
          triggerBtns.forEach((b) => {
            b.disabled = false;
            b.removeAttribute("aria-busy");
            const o = b.dataset.fetcherTestLabel;
            if (o != null && o !== "") b.textContent = o;
          });
        });
    });
  });
}

function initSettingsTabs() {
  const tabButtons = document.querySelectorAll(".settings-tab-btn[data-settings-tab]");
  const panels = document.querySelectorAll(
    ".settings-tab-target[data-settings-panel], .settings-panel-slice[data-settings-panel]",
  );
  if (!tabButtons.length || !panels.length) return;

  const validKeys = new Set(
    Array.from(panels)
      .map((p) => p.getAttribute("data-settings-panel"))
      .filter(Boolean)
  );

  function panelKeyExists(key) {
    return Boolean(key && validKeys.has(key));
  }

  function getInitialTabKey() {
    try {
      const q = new URLSearchParams(window.location.search);
      const tab = (q.get("tab") || "").toLowerCase();
      if (tab === "global" || tab === "security" || tab === "sonarr" || tab === "radarr") {
        return tab;
      }
      if ((q.get("sec") || "").trim()) return "security";
      if ((q.get("import") || "").trim()) return "global";
      const test = (q.get("test") || "").toLowerCase();
      if (test.indexOf("sonarr") === 0) return "sonarr";
      if (test.indexOf("radarr") === 0) return "radarr";
    } catch (e) {
      /* ignore */
    }

    const raw = (window.location.hash || "").slice(1);
    if (!raw) return null;
    const h = raw.toLowerCase();

    if (h.startsWith("section-")) {
      const key = h.slice("section-".length);
      if (key === "global" || key === "security" || key === "sonarr" || key === "radarr") {
        return key;
      }
    }
    if (h === "global" || h === "security" || h === "sonarr" || h === "radarr") return h;
    if (h.includes("section-security") || h.includes("security-")) return "security";
    if (h.includes("section-sonarr")) return "sonarr";
    if (h.includes("section-radarr")) return "radarr";
    if (h.includes("section-global")) return "global";

    return null;
  }

  function showTab(key, { updateHash } = { updateHash: true }) {
    if (!panelKeyExists(key)) return;

    tabButtons.forEach((btn) => {
      const k = btn.getAttribute("data-settings-tab");
      const on = k === key;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });

    panels.forEach((panel) => {
      const pk = panel.getAttribute("data-settings-panel");
      const on = pk === key;
      panel.classList.toggle("is-active", on);
      panel.hidden = !on;
    });

    if (updateHash) {
      const url = `${window.location.pathname}${window.location.search}#section-${key}`;
      history.replaceState(null, "", url);
    }
  }

  let initial = getInitialTabKey();
  if (!initial || !panelKeyExists(initial)) {
    initial = tabButtons[0].getAttribute("data-settings-tab") || "global";
  }
  showTab(initial, { updateHash: false });

  tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.getAttribute("data-settings-tab");
      if (key) showTab(key, { updateHash: true });
    });
  });
}

function initTrimmerSettingsSectionTabs() {
  const tabButtons = Array.from(
    document.querySelectorAll(
      ".trimmer-settings-section-tabs .settings-tab[href^='#'], .refiner-settings-section-tabs .settings-tab[href^='#']",
    ),
  );
  const anchorNodes = document.querySelectorAll(".trimmer-settings-anchor[id], .refiner-settings-anchor[id]");
  const anchors = Array.from(anchorNodes).filter((el, i, arr) => arr.indexOf(el) === i);
  if (!tabButtons.length || !anchors.length) return;

  const byId = new Map(anchors.map((el) => [el.id, el]));

  function showSection(sectionId, opts) {
    const targetId = byId.has(sectionId) ? sectionId : anchors[0].id;
    anchors.forEach((el) => {
      const active = el.id === targetId;
      el.hidden = !active;
      el.setAttribute("aria-hidden", active ? "false" : "true");
    });
    tabButtons.forEach((btn) => {
      const id = (btn.getAttribute("href") || "").replace(/^#/, "");
      const active = id === targetId;
      btn.classList.toggle("is-active", active);
      btn.setAttribute("aria-current", active ? "page" : "false");
    });
    if (opts && opts.updateHash) {
      const url = `${window.location.pathname}${window.location.search}#${targetId}`;
      history.replaceState(null, "", url);
    }
  }

  let initial = (window.location.hash || "").replace(/^#/, "").trim();
  if (!initial || !byId.has(initial)) initial = anchors[0].id;
  showSection(initial, { updateHash: false });

  tabButtons.forEach((btn) => {
    btn.addEventListener("click", (ev) => {
      ev.preventDefault();
      const id = (btn.getAttribute("href") || "").replace(/^#/, "");
      showSection(id, { updateHash: true });
    });
  });
}

window.addEventListener("DOMContentLoaded", () => {
  injectMeshAndNoise();
  bindInternalLinksTargetTop();
  initSetupWizardAsyncSave();
  initFetcherSettingsAsyncSave();
  initFetcherSettingsAsyncTest();
  initTrimmerSettingsAsyncConnection();
  initTrimmerSettingsAsyncCleaner();
  initRefinerSettingsAsyncSave();
  bindScrollRestoreOnFormSubmit();
  restoreScrollAfterFormRedirect();
  bindRevealButtons();
  bindDashboardDismissibles();
  initActivityFilterPills();
  initActivityDetailExpand();
  initSettingsTabs();
  initTrimmerSettingsSectionTabs();
  initSettingsPageCollapses();

  staggerClass(".hero-stat", 0, 60, "anim-in");
  staggerClass(".card.gc, .gc.card", 0, 80, "anim-in");
  staggerClass(".activity-row", 150, 50, "anim-in");
  staggerClass(".log-entry", 120, 60, "anim-in");

  const loginCard = document.querySelector(".login-card");
  if (loginCard) {
    window.setTimeout(() => loginCard.classList.add("anim-in"), 50);
  }

  runHeroCountUp();

  startHeroMetricsPolling();
  startDashboardStatusPolling();
  startLiveTilePolling();

  /* Settings saves (Fetcher / Trimmer / Refiner): success uses inline banners only — no duplicate toast. */
  if (qs("ran") === "1") showToast("Run triggered");
  if (qs("test") === "sonarr_ok") showToast("Sonarr connection succeeded");
  if (qs("test") === "sonarr_fail") showToast("Sonarr connection failed");
  if (qs("test") === "radarr_ok") showToast("Radarr connection succeeded");
  if (qs("test") === "radarr_fail") showToast("Radarr connection failed");
  if (qs("test") === "emby_ok") showToast("Emby connection succeeded");
  if (qs("test") === "emby_fail") showToast("Emby connection failed");
});

window.addEventListener("pageshow", reapplyPendingScrollAfterPageshow);
