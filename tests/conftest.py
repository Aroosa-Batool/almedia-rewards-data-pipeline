"""Tests run against a real Postgres (CI: service container; locally: DATABASE_URL in .env.local).
Every test gets its own throwaway schema, dropped afterwards."""
import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

env = ROOT / ".env.local"
if env.exists() and "DATABASE_URL" not in os.environ:
    for line in env.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v.strip().strip('"'))

from pipeline.db import connect, database_url  # noqa: E402


@pytest.fixture
def schema():
    name = f"test_{uuid.uuid4().hex[:10]}"
    yield name
    with psycopg.connect(database_url(), autocommit=True) as c:
        c.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')


@pytest.fixture
def conn(schema):
    c = connect(schema)
    yield c
    c.close()
