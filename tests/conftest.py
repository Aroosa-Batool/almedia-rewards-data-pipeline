import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.db import connect  # noqa: E402


@pytest.fixture
def conn(tmp_path):
    return connect(str(tmp_path / "t.db"))
