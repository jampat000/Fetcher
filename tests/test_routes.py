from fastapi.testclient import TestClient

from app.main import app


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
    assert data["app"] == "Grabby"
    assert "version" in data and len(data["version"]) > 0


def test_api_version_ok(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/api/version")
    assert resp.status_code == 200
    body = resp.json()
    assert body["app"] == "Grabby"
    assert "version" in body and len(body["version"]) > 0


def test_dashboard_route_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/")
    assert resp.status_code == 200


def test_settings_route_smoke(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        resp = client.get("/settings")
    assert resp.status_code == 200
