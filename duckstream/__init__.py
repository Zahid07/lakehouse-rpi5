"""duckstream — a micro-batch streaming framework for DuckDB and DuckLake.

Sources with durable offsets, a trigger loop, correct incremental aggregation
and idempotent sinks, in one Python process over an embedded DuckDB, writing a
real lakehouse: parquet data files, a SQL catalog, snapshots and time travel.

The Python front door::

    import duckdb
    from duckstream import Engine, Model, FileSource, TableSink, AvailableNow

    con = duckdb.connect()
    engine = Engine(con, catalog="ducklake:catalog.ducklake",
                    data_path="lake_data")
    engine.add(Model(
        name="hourly_counts",
        source=FileSource("landing/", marker="_READY",
                          max_files_per_trigger=10),
        time_column="event_ts",
        grain="hour",
        key=["window_ts", "sensor_id"],
        aggregates={"n": "count(*)", "total": "sum(value)"},
        sink=TableSink("marts.hourly_counts", mode="update"),
    ))
    engine.run(trigger=AvailableNow())

and the config front door, over the same canonical ``Model``::

    python -m duckstream run --config models.yaml

Why the imports below are lazy
------------------------------

Every public name here is resolved on first use through a module-level
``__getattr__`` (PEP 562), so ``import duckstream`` costs almost nothing and
each submodule's dependencies are paid for only by the code that needs them —
``yaml`` when a config is loaded, ``duckdb`` when a connection is opened.

That is not a micro-optimisation, it is a property the package already relies
on. The registry resolves sources, sinks and UDFs by dotted path and
deliberately imports none of them at load time, which is what lets
``duckstream validate`` check a document on a deploy box where a user's UDF
package is not installed. Re-exporting everything eagerly here would undo half
of that, and the target device is a Raspberry Pi where interpreter start is
already the largest term in a cron tick (``CONTEXT.md`` 1.8: ~235 ms of process
start against ~17 ms of commit).

``from duckstream import Engine`` works exactly as if the imports were eager;
only the cost moves.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

#: Public name -> the submodule it lives in. The single place the surface is
#: declared: ``__all__``, ``__dir__`` and ``__getattr__`` are all derived from
#: it, so a name cannot be exported from one and missing from another.
_EXPORTS: dict[str, str] = {
    # the engine and its loop
    "Engine": "duckstream.engine",
    "BatchResult": "duckstream.engine",
    "RunReport": "duckstream.engine",
    "FaultHooks": "duckstream.engine",
    "FaultEvent": "duckstream.engine",
    "FAULT_POINTS": "duckstream.engine",
    "AvailableNow": "duckstream.trigger",
    "Once": "duckstream.trigger",
    "ProcessingTime": "duckstream.daemon",
    "Trigger": "duckstream.trigger",
    # the canonical declaration
    "Model": "duckstream.model",
    "Tier": "duckstream.aggregates",
    "GRAINS": "duckstream.model",
    "WINDOW_COLUMN": "duckstream.model",
    "WatermarkPolicy": "duckstream.watermark",
    "parse_lateness": "duckstream.watermark",
    "format_lateness": "duckstream.watermark",
    # protocols and the shapes they pass around
    "BatchLimits": "duckstream.protocols",
    "BatchPlan": "duckstream.protocols",
    "BatchContext": "duckstream.protocols",
    "Offset": "duckstream.protocols",
    "Source": "duckstream.protocols",
    "Sink": "duckstream.protocols",
    "StateStore": "duckstream.protocols",
    # built-in source and sink
    "FileSource": "duckstream.sources.files",
    "TableSink": "duckstream.sinks.table",
    # state
    "DuckLakeStateStore": "duckstream.state",
    "MemoryStateStore": "duckstream.state",
    # the config front door
    "load_config": "duckstream.config",
    "parse_yaml": "duckstream.config",
    "ConfigDocument": "duckstream.config",
    # the registry: how config names reach user code
    "register_source": "duckstream.registry",
    "register_sink": "duckstream.registry",
    "register_udf": "duckstream.registry",
    "available_sources": "duckstream.registry",
    "available_sinks": "duckstream.registry",
    "available_udfs": "duckstream.registry",
    # errors
    "ArrowUDF": "duckstream.udf",
    "arrow_udf": "duckstream.udf",
    "RunLock": "duckstream.lock",
    "LockError": "duckstream.lock",
    "BatchFailed": "duckstream.errors",
    "DuckstreamError": "duckstream.errors",
    "ModelValidationError": "duckstream.errors",
    "ConfigError": "duckstream.errors",
}

__all__ = sorted(_EXPORTS)


if TYPE_CHECKING:  # pragma: no cover - for type checkers and IDEs only
    from duckstream.aggregates import Tier
    from duckstream.config import ConfigDocument, load_config, parse_yaml
    from duckstream.engine import (
        FAULT_POINTS,
        BatchResult,
        Engine,
        FaultEvent,
        FaultHooks,
        RunReport,
    )
    from duckstream.errors import ConfigError, DuckstreamError, ModelValidationError
    from duckstream.model import Model
    from duckstream.protocols import (
        BatchContext,
        BatchLimits,
        BatchPlan,
        Offset,
        Sink,
        Source,
        StateStore,
    )
    from duckstream.registry import (
        available_sinks,
        available_sources,
        available_udfs,
        register_sink,
        register_source,
        register_udf,
    )
    from duckstream.sinks.table import TableSink
    from duckstream.sources.files import FileSource
    from duckstream.state import DuckLakeStateStore, MemoryStateStore
    from duckstream.daemon import ProcessingTime
    from duckstream.trigger import AvailableNow, Once, Trigger


def __getattr__(name: str) -> Any:
    """Import the submodule owning ``name`` on first access. See the docstring."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module(module_name), name)
    globals()[name] = value  # resolved once; later lookups skip __getattr__
    return value


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
