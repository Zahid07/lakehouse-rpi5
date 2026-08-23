"""``python -m duckstream`` — the entry point cron actually calls.

``PLAN.md`` asks for both this and the ``duckstream`` console script, and the
reason is practical: cron inside a virtualenv almost always invokes the
interpreter by absolute path rather than relying on the script being on
``PATH``, so::

    * * * * * cd /opt/pipeline && ./venv/bin/python -m duckstream run \
        --config models.yaml >> logs/duckstream.log 2>&1

has to work without the package being installed onto ``PATH`` at all. Both
routes call the same :func:`duckstream.cli.main`, which returns the exit code
rather than raising ``SystemExit``, so a test can call it directly.
"""

from __future__ import annotations

import sys

from duckstream.cli import main

if __name__ == "__main__":
    sys.exit(main())
