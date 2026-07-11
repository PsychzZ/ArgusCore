import pytest
from fastapi.testclient import TestClient


def test_healthz_healthy_when_db_ok():
    from argus.common import health

    async def ok():
        return True

    tracker = health.LastRunTracker()
    app = health.create_app(db_ping=ok, scheduler_running=True, last_run=tracker)
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 200
    body = res.json()
    assert body["db"] is True
    assert body["scheduler"] is True
    assert body["last_run"] is None


def test_healthz_reflects_last_run_updates():
    from argus.common import health

    async def ok():
        return True

    tracker = health.LastRunTracker()
    app = health.create_app(db_ping=ok, scheduler_running=True, last_run=tracker)
    client = TestClient(app)

    tracker.mark()
    body = client.get("/healthz").json()
    assert body["last_run"] is not None
    assert "T" in body["last_run"]  # ISO timestamp


@pytest.mark.asyncio
async def test_tracked_wraps_job_and_marks_after_run():
    from argus.common import health

    tracker = health.LastRunTracker()
    ran = []

    async def job():
        ran.append(True)
        assert tracker.iso is None  # marked only after the job finishes

    wrapped = health.tracked(job, tracker)
    await wrapped()
    assert ran == [True]
    assert tracker.iso is not None


def test_healthz_unhealthy_when_db_down():
    from argus.common import health

    async def bad():
        return False

    app = health.create_app(db_ping=bad, scheduler_running=True, last_run=health.LastRunTracker())
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 503
