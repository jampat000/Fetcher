from fastapi.testclient import TestClient

from app.main import app
from app.models import AppSettings


def _build_client(monkeypatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown() -> None:
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
    assert "next_scheduler_tick_local" in data
    assert data["sonarr_missing"] == 0
    assert data["radarr_missing"] == 0
    assert data["sonarr_upgrades"] == 0
    assert data["radarr_upgrades"] == 0
    assert data["emby_matched"] == 0


def test_dashboard_route_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200


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

    monkeypatch.setattr("app.main._get_or_create_settings", _fake_settings)
    monkeypatch.setattr("app.main.EmbyClient", _emby_should_not_construct)
    with _build_client(monkeypatch) as client:
        resp = client.get("/trimmer")
    assert resp.status_code == 200
    assert b"No scan yet" in resp.content
    assert b"Scan Emby for matches" in resp.content
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

    monkeypatch.setattr("app.main._get_or_create_settings", _fake_settings)
    monkeypatch.setattr("app.main.EmbyClient", _StubClient)
    with _build_client(monkeypatch) as client:
        resp = client.get("/trimmer?preview=1")
    assert resp.status_code == 200
    assert constructed == []


def test_settings_route_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/settings")
    assert resp.status_code == 200


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
    monkeypatch.setattr("app.main.test_sonarr_connection", _fake_arr_ok)
    with _build_client(monkeypatch) as client:
        resp = client.post(
            "/api/setup/test-sonarr",
            json={"url": "http://127.0.0.1:8989", "api_key": "x"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert "ok" in body["message"].lower()
