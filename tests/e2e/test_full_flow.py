"""E2E test: spin up compose stack with mocks, inject event, verify flow.

Run with: uv run pytest tests/e2e/test_full_flow.py -v -s

Prerequisites: Docker running, ports 5433/1080/1081 free.
"""

import subprocess

import pytest


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


@pytest.fixture(scope="module")
def e2e_stack():
    compose = ["docker", "compose", "-f", "tests/e2e/docker-compose.e2e.yml"]
    run([*compose, "up", "-d", "--wait"])
    yield
    run([*compose, "down", "-v"], check=False)


def test_full_pipeline_flows_end_to_end(e2e_stack):
    """Inject a Form 4 filing at the SEC mock; assert it lands in Discord mock."""
    # 1. Configure expectations on mocks (mockserver client API)
    # 2. Start the 4 ArgusCore services against mocks (could be `docker compose up`)
    # 3. Inject test Form 4 XML into mock-sec
    # 4. Poll Postgres for raw_events.status='sent' (timeout 60s)
    # 5. Assert mock-discord received an embed
    pytest.skip("E2E scaffolding is environment-specific; implement on first deploy")
