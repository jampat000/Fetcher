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
      "grabby_restore_scroll",
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
  return (
    sp.get("saved") === "1" ||
    sp.get("test") === "sonarr_ok" ||
    sp.get("test") === "sonarr_fail" ||
    sp.get("test") === "radarr_ok" ||
    sp.get("test") === "radarr_fail" ||
    sp.get("test") === "emby_ok" ||
    sp.get("test") === "emby_fail"
  );
}

/** Keeps main-column scroll across redirect + late layout (pageshow / fonts). */
let grabbyPendingMainScroll = null;

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
  const raw = sessionStorage.getItem("grabby_restore_scroll");

  // Only take over scroll restoration when we're actually restoring after a save/test redirect.
  // Setting "manual" on every page breaks some embedded browsers (e.g. VS Code / Cursor Simple Browser).
  if (fromReturn) {
    try {
      if ("scrollRestoration" in history) history.scrollRestoration = "manual";
    } catch (_) {
      /* ignore */
    }
  }

  if (!fromReturn) {
    if (raw) sessionStorage.removeItem("grabby_restore_scroll");
    grabbyPendingMainScroll = null;
    return;
  }

  if (!raw) {
    if (grabbyPendingMainScroll != null) applyScrollY(grabbyPendingMainScroll);
    return;
  }

  sessionStorage.removeItem("grabby_restore_scroll");

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

  grabbyPendingMainScroll = y;
  const apply = () => applyScrollY(grabbyPendingMainScroll);
  requestAnimationFrame(apply);
  [0, 50, 100, 200, 400, 600].forEach((ms) => window.setTimeout(apply, ms));
  window.setTimeout(() => {
    grabbyPendingMainScroll = null;
  }, 3000);
}

function reapplyPendingScrollAfterPageshow() {
  if (!shouldRestoreAfterRedirect() || grabbyPendingMainScroll == null) return;
  applyScrollY(grabbyPendingMainScroll);
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

window.addEventListener("DOMContentLoaded", () => {
  bindInternalLinksTargetTop();
  bindScrollRestoreOnFormSubmit();
  restoreScrollAfterFormRedirect();
  bindRevealButtons();
  bindDashboardDismissibles();
  if (qs("saved") === "1") showToast("Settings saved");
  if (qs("ran") === "1") showToast("Run triggered");
  if (qs("test") === "sonarr_ok") showToast("Sonarr OK");
  if (qs("test") === "sonarr_fail") showToast("Sonarr failed");
  if (qs("test") === "radarr_ok") showToast("Radarr OK");
  if (qs("test") === "radarr_fail") showToast("Radarr failed");
  if (qs("test") === "emby_ok") showToast("Emby OK");
  if (qs("test") === "emby_fail") showToast("Emby failed");
});

window.addEventListener("pageshow", reapplyPendingScrollAfterPageshow);

