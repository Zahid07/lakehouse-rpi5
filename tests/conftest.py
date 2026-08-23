"""Repo-wide pytest configuration.

Deliberately almost empty. The unit suites under ``tests/unit`` predate this
file and must keep behaving exactly as they did, so nothing here changes
collection, imports or fixtures for them. What it does provide is the two
things every suite in this repository needs to agree on:

* **which interpreter a subprocess test spawns.** The conformance suite kills
  real processes and drives the real CLI, and both must run under the same
  virtualenv the tests themselves run under -- ``duckdb`` is pinned exactly
  (``CONTEXT.md`` 1.5 and 1.7 are version-sensitive) and a stray system Python
  would silently test a different DuckDB.
* **markers**, so ``-m "not slow"`` is available to anyone iterating on a
  single conformance test. A DuckLake commit costs ~15 ms and a process spawn
  ~235 ms (``CONTEXT.md`` 1.8), so the subprocess-based tests are the expensive
  ones and are worth being able to deselect.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

#: Repository root: the directory holding ``duckstream/`` and ``pyproject.toml``.
REPO_ROOT = Path(__file__).resolve().parents[1]

#: The interpreter subprocess tests spawn. ``sys.executable`` is the venv
#: interpreter pytest is running under, which is the one that has the pinned
#: ``duckdb`` and the ducklake extension installed.
PYTHON = Path(sys.executable)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "conformance: runs against a real DuckLake catalog on disk",
    )
    config.addinivalue_line(
        "markers",
        "slow: spawns a real subprocess (~235 ms of process start each)",
    )


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """The repository root, as an absolute path."""
    return REPO_ROOT


@pytest.fixture(scope="session")
def python_exe() -> Path:
    """The interpreter a subprocess test should spawn."""
    return PYTHON
