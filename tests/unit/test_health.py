from fastapi.testclient import TestClient


def test_healthz_healthy_when_db_ok(monkeypatch):
    from argus.common import health

    async def ok():
        return True

    app = health.create_app(db_ping=ok, scheduler_running=True, last_run_iso="2026-07-05T13:00:00Z")
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 200
    body = res.json()
    assert body["db"] is True
    assert body["scheduler"] is True
    assert body["last_run"] == "2026-07-05T13:00:00Z"


def test_healthz_unhealthy_when_db_down(monkeypatch):
    from argus.common import health

    async def bad():
        return False

    app = health.create_app(db_ping=bad, scheduler_running=True, last_run_iso=None)
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 503
