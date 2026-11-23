import subprocess
import sys
from pathlib import Path

import yaml


def test_docker_compose_defines_required_services_and_migrations() -> None:
    compose_path = Path("docker-compose.yml")
    assert compose_path.is_file()

    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    services = compose_data["services"]

    # Required services: postgres, rabbitmq, api, and consumer
    assert set(services.keys()) == {"postgres", "rabbitmq", "api", "consumer"}

    assert services["postgres"]["image"].startswith("postgres:16")
    assert "healthcheck" in services["postgres"]

    assert "rabbitmq" in services["rabbitmq"]["image"]
    assert "healthcheck" in services["rabbitmq"]

    api_cmd = services["api"]["command"]
    assert "alembic upgrade head" in api_cmd
    assert "uvicorn src.main:app" in api_cmd

    consumer_cmd = services["consumer"]["command"]
    assert "faststream run src.workers.consumer:app" in consumer_cmd
    assert set(services["consumer"]["depends_on"].keys()) == {"postgres", "rabbitmq", "api"}


def test_dockerfile_and_dockerignore_exist() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.12-slim" in dockerfile
    assert "uv sync --frozen --no-dev" in dockerfile
    assert "alembic upgrade head" in dockerfile

    dockerignore = Path(".dockerignore").read_text(encoding="utf-8")
    assert ".venv" in dockerignore
    assert ".env" in dockerignore


def test_entrypoints_import_cleanly_in_fresh_process() -> None:
    """Ensure both `src.main` and `src.workers.consumer` import first without circular imports."""
    for module_name in ("src.main", "src.workers.consumer", "src.workers.outbox_relay"):
        result = subprocess.run(
            [sys.executable, "-c", f"import {module_name}"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"Failed importing {module_name}:\n{result.stderr}"
