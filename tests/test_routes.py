from fastapi.testclient import TestClient

from app.main import app
from app.models import AppSettings


def _build_client(monkeypatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_healthz_ok(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["app"] == "Fetcher"
    assert "version" in data and len(data["version"]) > 0


def test_api_version_ok(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/api/version")
    assert resp.status_code == 200
    body = resp.json()
    assert body["app"] == "Fetcher"
    assert "version" in body and len(body["version"]) > 0


def test_api_dashboard_status_ok(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/api/dashboard/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "last_run" in data
    assert "latest_system_event" in data
    assert "last_sonarr_run" in data
    assert "last_radarr_run" in data
    assert "last_trimmer_run" in data
    assert "time_local" in data["last_sonarr_run"]
    assert "ok" in data["last_sonarr_run"]
    assert "next_sonarr_tick_local" in data
    assert "next_radarr_tick_local" in data
    assert "next_trimmer_tick_local" in data
    assert "fetcher_phase" in data
    assert "fetcher_phase_label" in data
    assert "sonarr_automation_sub" in data
    assert "last_sonarr_run" in data and "relative" in data["last_sonarr_run"]
    assert data["sonarr_missing"] >= 0
    assert data["radarr_missing"] >= 0
    assert data["sonarr_upgrades"] >= 0
    assert data["radarr_upgrades"] >= 0
    assert data["emby_matched"] >= 0


def test_dashboard_route_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert b"Sonarr" in resp.content
    assert b"data-automation-card=\"refiner\"" in resp.content
    assert b"automation-cards" in resp.content
    assert b"Sonarr" in resp.content
    assert b"Radarr" in resp.content
    assert b"Trimmer" in resp.content
    assert b"Refiner" in resp.content
    assert resp.content.count(b"automation-footer-shell") >= 4
    assert resp.content.count(b"automation-footer-note-slot") >= 4
    assert resp.content.count(b"automation-footer-action-slot") >= 4


def test_dashboard_hero_does_not_fetch_live_totals_before_render(monkeypatch) -> None:
    """Navigation to the Dashboard must not block on live Arr totals."""

    async def _boom(_settings):
        raise RuntimeError("live totals must not run during dashboard HTML render")

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _boom)
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert b"dashboard-overview" in resp.content


def test_api_dashboard_status_uses_live_totals(monkeypatch) -> None:
    async def _live(_settings):
        return {
            "sonarr_missing": 11,
            "sonarr_upgrades": 12,
            "radarr_missing": 13,
            "radarr_upgrades": 14,
        }

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _live)
    with _build_client(monkeypatch) as client:
        resp = client.get("/api/dashboard/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["sonarr_missing"] == 11
    assert data["sonarr_upgrades"] == 12
    assert data["radarr_missing"] == 13
    assert data["radarr_upgrades"] == 14


def test_dashboard_route_renders_per_app_success_failure_badges(monkeypatch) -> None:
    async def _fake_status(_session, _tz, *, snapshots=None, include_live: bool | None = None):  # noqa: ARG001
        return {
            "last_run": {"started_local": "24-03-2026 11:00 AM", "ok": True},
            "latest_system_event": {
                "context": "Radarr | Upgrade search",
                "time_local": "24-03-2026 11:00 AM",
                "ok": True,
                "relative": "1 minute ago",
            },
            "last_sonarr_run": {"time_local": "24-03-2026 10:00 AM", "ok": True, "relative": "1 hour ago"},
            "last_radarr_run": {"time_local": "24-03-2026 10:30 AM", "ok": False, "relative": "30 minutes ago"},
            "last_trimmer_run": {"time_local": "24-03-2026 10:45 AM", "ok": True, "relative": "15 minutes ago"},
            "next_sonarr_tick_local": "24-03-2026 11:30 AM",
            "next_radarr_tick_local": "24-03-2026 11:45 AM",
            "next_trimmer_tick_local": "24-03-2026 12:00 PM",
            "next_sonarr_relative": "in 30 minutes",
            "next_radarr_relative": "in 45 minutes",
            "next_trimmer_relative": "in 1 hour",
            "fetcher_phase": "active",
            "fetcher_phase_label": "Active",
            "fetcher_phase_detail": "Test detail.",
            "sonarr_automation_sub": "",
            "radarr_automation_sub": "",
            "trimmer_automation_sub": "",
            "sonarr_missing": 0,
            "sonarr_upgrades": 0,
            "radarr_missing": 0,
            "radarr_upgrades": 0,
            "emby_matched": 0,
            "trimmer_connection_type": "Trimmer",
            "trimmer_connection_status": "Connected",
        }

    monkeypatch.setattr("app.routers.dashboard.build_dashboard_status", _fake_status)
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert b"Sonarr" in resp.content
    assert b"Radarr" in resp.content
    assert b"dash-last-refiner-run" in resp.content
    assert b"Succeeded" in resp.content
    assert b"Failed" in resp.content


def test_dashboard_route_empty_states_are_intentional(monkeypatch) -> None:
    import app.routers.dashboard as dash_mod

    real_get = dash_mod.get_or_create_settings

    async def _settings_sonarr_on(session):
        row = await real_get(session)
        row.sonarr_enabled = True
        return row

    async def _fake_status(_session, _tz, *, snapshots=None, include_live: bool | None = None):  # noqa: ARG001
        return {
            "last_run": None,
            "latest_system_event": None,
            "last_sonarr_run": {"time_local": "", "ok": None, "relative": ""},
            "last_radarr_run": {"time_local": "", "ok": None, "relative": ""},
            "last_trimmer_run": {"time_local": "", "ok": None, "relative": ""},
            "next_sonarr_tick_local": "",
            "next_radarr_tick_local": "",
            "next_trimmer_tick_local": "",
            "next_sonarr_relative": "",
            "next_radarr_relative": "",
            "next_trimmer_relative": "",
            "fetcher_phase": "idle",
            "fetcher_phase_label": "Idle",
            "fetcher_phase_detail": "No jobs.",
            "sonarr_automation_sub": "",
            "radarr_automation_sub": "",
            "trimmer_automation_sub": "",
            "sonarr_missing": 0,
            "sonarr_upgrades": 0,
            "radarr_missing": 0,
            "radarr_upgrades": 0,
            "emby_matched": 0,
            "trimmer_connection_type": "Trimmer",
            "trimmer_connection_status": "Not configured",
        }

    monkeypatch.setattr("app.routers.dashboard.build_dashboard_status", _fake_status)
    monkeypatch.setattr(dash_mod, "get_or_create_settings", _settings_sonarr_on)
    monkeypatch.setattr(
        "app.routers.dashboard.merge_activity_feed",
        lambda *_a, **_kw: [],
    )
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert b"Not yet run" in resp.content
    assert b"Always on" in resp.content
    assert b"No schedule configured" in resp.content
    assert b"No activity yet" in resp.content
    assert b"automation-footer-shell" in resp.content


def test_dashboard_route_disabled_actions_render_inside_shared_footer(monkeypatch) -> None:
    import app.routers.dashboard as dash_mod

    real_get = dash_mod.get_or_create_settings

    async def _settings_disabled(session):
        row = await real_get(session)
        row.refiner_enabled = False
        row.emby_enabled = False
        return row

    async def _fake_status(_session, _tz, *, snapshots=None, include_live: bool | None = None):  # noqa: ARG001
        return {
            "last_run": None,
            "latest_system_event": None,
            "last_sonarr_run": {"time_local": "", "ok": None, "relative": ""},
            "last_radarr_run": {"time_local": "", "ok": None, "relative": ""},
            "last_trimmer_run": {"time_local": "", "ok": None, "relative": ""},
            "last_refiner_run": {"time_local": "", "ok": None, "relative": "", "time_iso": ""},
            "next_sonarr_tick_local": "",
            "next_radarr_tick_local": "",
            "next_trimmer_tick_local": "",
            "next_refiner_tick_local": "",
            "next_sonarr_relative": "",
            "next_radarr_relative": "",
            "next_trimmer_relative": "",
            "next_refiner_relative": "",
            "next_sonarr_display": {"state": "enabled_unscheduled", "primary": "Always on", "secondary": "No schedule configured"},
            "next_radarr_display": {"state": "enabled_unscheduled", "primary": "Always on", "secondary": "No schedule configured"},
            "next_trimmer_display": {"state": "disabled", "primary": "Off", "secondary": "Disabled in settings"},
            "next_refiner_display": {"state": "disabled", "primary": "Off", "secondary": "Disabled in settings"},
            "fetcher_phase": "idle",
            "fetcher_phase_label": "Idle",
            "fetcher_phase_detail": "No jobs.",
            "sonarr_automation_sub": "",
            "radarr_automation_sub": "",
            "trimmer_automation_sub": "",
            "sonarr_missing": 0,
            "sonarr_upgrades": 0,
            "radarr_missing": 0,
            "radarr_upgrades": 0,
            "emby_matched": 0,
            "trimmer_connection_type": "Trimmer",
            "trimmer_connection_status": "Not configured",
            "sonarr_sparkline": [],
            "radarr_sparkline": [],
            "refiner_sparkline": [],
            "trimmer_sparkline": [],
            "refiner_live_total": 0,
            "refiner_live_done": 0,
        }

    monkeypatch.setattr("app.routers.dashboard.build_dashboard_status", _fake_status)
    monkeypatch.setattr(dash_mod, "get_or_create_settings", _settings_disabled)
    monkeypatch.setattr("app.routers.dashboard.merge_activity_feed", lambda *_a, **_kw: [])
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert b'href="/refiner/settings"' in resp.content
    assert b'href="/trimmer/settings"' in resp.content
    assert b"automation-footer-action-slot" in resp.content


def test_cleaner_default_skips_emby_client_when_ready(monkeypatch) -> None:
    """Sidebar /trimmer must not scan Emby until ?scan=1 (fast navigation)."""

    async def _fake_settings(_session):
        return AppSettings(
            emby_url="http://127.0.0.1:8096",
            emby_api_key="test-key",
            emby_rule_movie_watched_rating_below=3,
        )

    def _emby_should_not_construct(*_a, **_kw):
        raise AssertionError("EmbyClient must not run without ?scan=1")

    monkeypatch.setattr("app.routers.trimmer.get_or_create_settings", _fake_settings)
    monkeypatch.setattr("app.routers.trimmer.EmbyClient", _emby_should_not_construct)
    with _build_client(monkeypatch) as client:
        resp = client.get("/trimmer")
    assert resp.status_code == 200
    assert b"No scan loaded" in resp.content
    assert b"Scan for matches" in resp.content
    assert b"trimmer-area-tabs" in resp.content


def test_cleaner_preview_query_does_not_trigger_scan(monkeypatch) -> None:
    """Only ?scan=1 (or truthy scan) loads Emby; ?preview= is ignored."""

    async def _fake_settings(_session):
        return AppSettings(
            emby_url="http://127.0.0.1:8096",
            emby_api_key="test-key",
            emby_rule_movie_watched_rating_below=3,
        )

    constructed: list[bool] = []

    class _StubClient:
        def __init__(self, *_a, **_kw) -> None:
            constructed.append(True)

        async def health(self) -> None:
            return None

        async def users(self) -> list:
            return [{"Id": "u1", "Name": "Tester"}]

        async def items_for_user(self, **kwargs) -> list:
            return []

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr("app.routers.trimmer.get_or_create_settings", _fake_settings)
    monkeypatch.setattr("app.routers.trimmer.EmbyClient", _StubClient)
    with _build_client(monkeypatch) as client:
        resp = client.get("/trimmer?preview=1")
    assert resp.status_code == 200
    assert constructed == []


def test_settings_route_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/settings")
    assert resp.status_code == 200
    assert b"Only check during these hours" in resp.content
    assert b"When this schedule is enabled" in resp.content


def test_setup_redirect_and_wizard_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        r0 = client.get("/setup", follow_redirects=False)
        assert r0.status_code == 302
        assert r0.headers.get("location", "").endswith("/setup/1")
        r1 = client.get("/setup/1")
    assert r1.status_code == 200
    assert b"First-run setup" in r1.content


async def _fake_arr_ok(url: str, api_key: str) -> tuple[bool, str]:
    return True, "ok (test)"


def test_api_setup_test_sonarr_mocked(monkeypatch) -> None:
    monkeypatch.setattr("app.routers.api.test_sonarr_connection", _fake_arr_ok)
    with _build_client(monkeypatch) as client:
        resp = client.post(
            "/api/setup/test-sonarr",
            json={"url": "http://127.0.0.1:8989", "api_key": "x"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert "ok" in body["message"].lower()
