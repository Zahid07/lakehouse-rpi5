"""Sinks — idempotent destinations for a batch's output rows.

A sink is any object matching :class:`duckstream.protocols.Sink`; there is no
base class to inherit. Built-in sinks are re-exported here under the registry
names the YAML loader uses (``table``), and user sinks are resolved by dotted
path (``my_pkg.sinks:MySink``).

Only :class:`~duckstream.sinks.table.TableSink` ships in phase 1. Its two modes
divide the idempotency problem cleanly: ``update`` merges on the model's key so
a replayed batch converges to the same rows, and ``append`` does not deduplicate
at all and leans entirely on the engine committing output and offset in one
transaction. Both are correct; only one of them is correct on its own.
"""

from __future__ import annotations

from duckstream.sinks.table import TableSink

__all__ = ["TableSink"]
