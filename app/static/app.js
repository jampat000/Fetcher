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
  return true;
}

/** Keeps main-column scroll across redirect + late layout (pageshow / fonts). */
let fetcherPendingMainScroll = null;

/** Remember scroll when saving (303 redirect reloads at top). */
function bindScrollRestoreOnFormSubmit() {
  document.querySelectorAll('form[method="post"]').forEach((form) => {
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
    const start = performance.now();
    const dur = 800;
    const tick = (now) => {
      const t = Math.min(1, (now - start) / dur);
      const eased = 1 - (1 - t) * (1 - t);
      const v = Math.round(target * eased);
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
          (filter === "emby" && app === "emby");
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
      return "all";
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
    if (forced === "all") open = true;
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

function applyDashboardStatusPayload(data) {
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

  const nextSonarr = document.getElementById("dash-next-sonarr-tick");
  if (nextSonarr) {
    const t = data.next_sonarr_tick_local;
    if (t) nextSonarr.textContent = t;
    else nextSonarr.innerHTML = '<span class="muted">—</span>';
  }
  const nextRadarr = document.getElementById("dash-next-radarr-tick");
  if (nextRadarr) {
    const t = data.next_radarr_tick_local;
    if (t) nextRadarr.textContent = t;
    else nextRadarr.innerHTML = '<span class="muted">—</span>';
  }
  const nextTrimmer = document.getElementById("dash-next-trimmer-tick");
  if (nextTrimmer) {
    const t = data.next_trimmer_tick_local;
    if (t) nextTrimmer.textContent = t;
    else nextTrimmer.innerHTML = '<span class="muted">—</span>';
  }

  const emM = document.getElementById("dash-emby-matched");
  if (emM) emM.textContent = String(data.emby_matched ?? 0);
  setMetricTile("emby-matched", data.emby_matched ?? 0);

  const lastHost = document.getElementById("dash-automation-last");
  if (lastHost && data.last_run) {
    lastHost.className = "automation-spec-value";
    const lr = data.last_run;
    const ok = lr.ok
      ? '<span class="status-pill status-pill-ok">Succeeded</span>'
      : '<span class="status-pill status-pill-fail">Failed</span>';
    lastHost.innerHTML = `<span id="dash-last-started">${escapeHtml(lr.started_local)}</span> ${ok}`;
  } else if (lastHost && !data.last_run) {
    lastHost.className = "automation-spec-value muted";
    lastHost.textContent = "—";
  }
}

function startDashboardStatusPolling() {
  if (!document.getElementById("dashboard-hero-stats")) return;
  const intervalMs = 60000;
  let timerId = null;
  const poll = () => {
    if (document.visibilityState === "hidden") return;
    fetch("/api/dashboard/status", {
      method: "GET",
      headers: {
        Accept: "application/json",
        "X-CSRF-Token": typeof getCSRFToken === "function" ? getCSRFToken() : "",
      },
      credentials: "same-origin",
      cache: "no-store",
    })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (data) applyDashboardStatusPayload(data);
      })
      .catch(() => {});
  };
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

function initSettingsTabs() {
  const tabButtons = document.querySelectorAll(".settings-tab-btn[data-settings-tab]");
  const panels = document.querySelectorAll(".settings-tab-target[data-settings-panel]");
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
      panel.removeAttribute("hidden");
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

window.addEventListener("DOMContentLoaded", () => {
  injectMeshAndNoise();
  bindInternalLinksTargetTop();
  bindScrollRestoreOnFormSubmit();
  restoreScrollAfterFormRedirect();
  bindRevealButtons();
  bindDashboardDismissibles();
  initActivityFilterPills();
  initActivityDetailExpand();
  initSettingsTabs();
  initSettingsPageCollapses();

  staggerClass(".hero-stat", 0, 60, "anim-in");
  staggerClass(".card.gc, .gc.card", 0, 80, "anim-in");
  staggerClass(".activity-row", 150, 50, "anim-in");
  staggerClass(".log-entry", 120, 60, "anim-in");

  const loginCard = document.querySelector(".login-card");
  if (loginCard) {
    window.setTimeout(() => loginCard.classList.add("anim-in"), 50);
  }

  window.setTimeout(runHeroCountUp, 200);

  startDashboardStatusPolling();

  if (qs("saved") === "1") showToast("Settings saved");
  if (qs("ran") === "1") showToast("Run triggered");
  if (qs("test") === "sonarr_ok") showToast("Sonarr connection succeeded");
  if (qs("test") === "sonarr_fail") showToast("Sonarr connection failed");
  if (qs("test") === "radarr_ok") showToast("Radarr connection succeeded");
  if (qs("test") === "radarr_fail") showToast("Radarr connection failed");
  if (qs("test") === "emby_ok") showToast("Emby connection succeeded");
  if (qs("test") === "emby_fail") showToast("Emby connection failed");
});

window.addEventListener("pageshow", reapplyPendingScrollAfterPageshow);
