"""Sources — replayable origins of rows.

A source is any object matching :class:`duckstream.protocols.Source`; there is no
base class to inherit. Built-in sources are re-exported here under the registry
names the YAML loader uses (``file``), and user sources are resolved by dotted
path (``my_pkg.sources:MySource``).

Only :class:`~duckstream.sources.files.FileSource` ships in phase 1, and that
ordering is deliberate rather than incidental: exactly-once needs a replayable
source, so the file source is the foundation every other ingestion path lands
into. The MQTT connector (phase 5) is a *landing writer* onto this source, not a
source of its own — once an MQTT message is acked it is gone, so it can never be
replayed directly.
"""

from __future__ import annotations

from duckstream.sources.files import FileSource

__all__ = ["FileSource"]
