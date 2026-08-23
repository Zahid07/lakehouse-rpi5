"""The registry: short names, dotted paths, and the messages when they fail.

The registry is the only place a config document can reach code, so two things
are load-bearing here and are asserted on directly rather than incidentally.

**Discoverability.** An unknown name must list the names that *are* available.
There is no other way for a config user to find out what ``type:`` accepts, so
that message is the whole documentation surface for the built-ins.

**No bare tracebacks.** Resolution happens during ``duckstream validate`` at
deploy time. A ``ModuleNotFoundError`` escaping to the console tells an operator
almost nothing; a ``ConfigError`` naming the dotted path, the module, and the
underlying exception tells them what to fix.

The other invariant pinned below is negative: the registry must never use
``issubclass`` against the protocols, because that raises ``TypeError`` on a
protocol with non-method members. :func:`test_issubclass_against_the_protocol_is
_a_TypeError` asserts the failure mode exists, so nobody "simplifies" the
structural check back into an ``issubclass`` call.
"""

from __future__ import annotations

import sys
from typing import Any, ClassVar

import pytest

from duckstream import registry as reg
from duckstream.errors import ConfigError
from duckstream.model import _SINK_METHODS, _SOURCE_METHODS
from duckstream.protocols import BatchLimits, BatchPlan, Offset, Sink, Source
from duckstream.sources.files import FileSource

try:  # W2c's sink; the tests that need it skip if it has not landed.
    from duckstream.sinks.table import TableSink
except Exception:  # pragma: no cover - only while W2c is in flight
    TableSink = None

requires_table_sink = pytest.mark.skipif(
    TableSink is None, reason="duckstream.sinks.table (W2c) is not available"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def isolated_registries():
    """Registering is global; every test gets the namespaces back as it found them."""
    saved = (reg.SOURCES.snapshot(), reg.SINKS.snapshot(), reg.UDFS.snapshot())
    yield
    reg.SOURCES.restore(saved[0])
    reg.SINKS.restore(saved[1])
    reg.UDFS.restore(saved[2])


PLUGIN = '''
"""A user plugin, written to a temp dir and imported by dotted path."""

from typing import ClassVar

IMPORTED = True


class MySource:
    type_name: ClassVar[str] = "my_pkg.sources:MySource"

    def __init__(self, path, *, flavour="plain"):
        self.path = path
        self.flavour = flavour

    def latest_offset(self):
        return {"seq": 0}

    def plan(self, start, end, limits):
        return {"start": start, "end": end}

    def bind(self, con, plan):
        return "a_view"

    def to_config(self):
        return {"type": self.type_name, "path": self.path, "flavour": self.flavour}


class Incomplete:
    """Looks like a source but cannot be bound."""

    def __init__(self, path):
        self.path = path

    def latest_offset(self):
        return {}

    def plan(self, start, end, limits):
        return {}

    def to_config(self):
        return {}


def make_source(path):
    """A plain function factory -- deliberately not a class."""
    return MySource(path)


NOT_CALLABLE = 42


class Nested:
    class Inner:
        def __init__(self, **kwargs):
            self.kwargs = kwargs


def arrow_fft(values):
    return values
'''

BROKEN_PLUGIN = 'raise RuntimeError("this plugin explodes on import")\n'


@pytest.fixture
def plugin(tmp_path, monkeypatch):
    """A user module on ``sys.path``, importable as ``duckstream_test_plugin``."""
    (tmp_path / "duckstream_test_plugin.py").write_text(PLUGIN, encoding="utf-8")
    (tmp_path / "duckstream_broken_plugin.py").write_text(
        BROKEN_PLUGIN, encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    yield "duckstream_test_plugin"
    for name in ("duckstream_test_plugin", "duckstream_broken_plugin"):
        sys.modules.pop(name, None)


# ---------------------------------------------------------------------------
# Built-in names
# ---------------------------------------------------------------------------


def test_file_is_a_built_in_source():
    assert reg.resolve_source("file") is FileSource


@requires_table_sink
def test_table_is_a_built_in_sink():
    assert reg.resolve_sink("table") is TableSink


def test_built_ins_are_declared_as_dotted_paths_not_imports():
    """``import duckstream.registry`` must not drag in every connector.

    Cold start is ~235 ms per cron tick before any work happens
    (``CONTEXT.md`` 1.8), and a half-installed optional connector must degrade
    to a ConfigError rather than breaking ``import duckstream``. Both follow
    from entries being strings until first use, which is what this asserts --
    on a private snapshot, because another test may already have resolved the
    module-level ones.
    """
    namespace = reg.Registry("source", methods=reg.SOURCE_METHODS)
    namespace.register("late", "duckstream.sources.files:FileSource")

    assert namespace.snapshot()["late"] == "duckstream.sources.files:FileSource"
    assert namespace.resolve("late") is FileSource
    assert namespace.snapshot()["late"] is FileSource  # cached after first use


def test_resolution_is_what_triggers_the_import(plugin, tmp_path, monkeypatch):
    (tmp_path / "duckstream_lazy_probe.py").write_text(
        "MARKER = object()\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("duckstream_lazy_probe", None)

    namespace = reg.Registry("udf")
    namespace.register("probe", "duckstream_lazy_probe:MARKER")
    assert "duckstream_lazy_probe" not in sys.modules

    with pytest.raises(ConfigError):
        namespace.resolve("probe")  # MARKER is not callable, but it *was* imported
    assert "duckstream_lazy_probe" in sys.modules
    sys.modules.pop("duckstream_lazy_probe", None)


def test_mqtt_is_reserved_for_phase_five():
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("mqtt")
    message = str(caught.value)
    assert "phase 5" in message
    assert "landing writer" in message


def test_mqtt_is_listed_as_available_but_annotated():
    assert "mqtt" in reg.available_sources()
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("nope")
    assert "'mqtt' (not implemented until phase 5)" in str(caught.value)


# ---------------------------------------------------------------------------
# Unknown names
# ---------------------------------------------------------------------------


def test_unknown_name_lists_the_available_names():
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("kafka")
    message = str(caught.value)
    assert "unknown source type 'kafka'" in message
    for name in reg.available_sources():
        assert repr(name) in message
    assert "my_pkg.sources:MySource" in message  # the dotted-path escape hatch


def test_unknown_name_suggests_a_close_match():
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("fyle")
    assert "Did you mean 'file'?" in str(caught.value)


def test_namespaces_do_not_leak_into_each_other():
    with pytest.raises(ConfigError, match="unknown source type 'table'"):
        reg.resolve_source("table")
    with pytest.raises(ConfigError, match="unknown sink type 'file'"):
        reg.resolve_sink("file")

    reg.register_udf("fft", len)
    assert reg.resolve_udf("fft") is len
    with pytest.raises(ConfigError, match="unknown source type 'fft'"):
        reg.resolve_source("fft")


def test_empty_name_is_refused_with_the_available_names():
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("")
    assert "'file'" in str(caught.value)


# ---------------------------------------------------------------------------
# Dotted paths
# ---------------------------------------------------------------------------


def test_dotted_path_to_a_user_class_resolves(plugin):
    resolved = reg.resolve_source(f"{plugin}:MySource")
    assert resolved.__name__ == "MySource"

    built = reg.build_source(f"{plugin}:MySource", {"path": "landing/"})
    assert built.path == "landing/"
    assert built.to_config()["type"] == "my_pkg.sources:MySource"


def test_dotted_path_reaches_a_nested_attribute(plugin):
    resolved = reg.resolve_udf(f"{plugin}:Nested.Inner")
    assert resolved.__qualname__ == "Nested.Inner"


def test_dotted_path_to_a_plain_function_factory_is_accepted(plugin):
    """No ``issubclass``, so a factory function is as good as a class."""
    built = reg.build_source(f"{plugin}:make_source", {"path": "landing/"})
    assert built.path == "landing/"


def test_missing_module_reports_a_config_error_naming_the_path():
    path = "definitely_not_installed_pkg.mod:Thing"
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source(path)
    message = str(caught.value)
    assert path in message
    assert "definitely_not_installed_pkg" in message
    assert "ModuleNotFoundError" in message
    assert "sys.path" in message


def test_missing_attribute_reports_a_config_error_naming_the_path():
    path = "duckstream.sources.files:Nope"
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source(path)
    message = str(caught.value)
    assert path in message
    assert "no attribute 'Nope'" in message
    assert "AttributeError" in message
    assert "FileSource" in message  # what it *does* define


def test_a_module_that_raises_on_import_reports_a_config_error(plugin):
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("duckstream_broken_plugin:Anything")
    message = str(caught.value)
    assert "duckstream_broken_plugin" in message
    assert "RuntimeError" in message
    assert "this plugin explodes on import" in message


@pytest.mark.parametrize("path", ["my_pkg.mod:", ":Thing", "  :  "])
def test_malformed_dotted_paths_are_refused(path):
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source(path)
    assert "pkg.module:object" in str(caught.value)


def test_a_name_without_a_colon_is_a_short_name_not_a_path():
    """``my_pkg.sources`` is not a path, so it must be reported as a name."""
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source("my_pkg.sources")
    assert "unknown source type 'my_pkg.sources'" in str(caught.value)


def test_a_non_callable_target_is_refused(plugin):
    with pytest.raises(ConfigError) as caught:
        reg.resolve_source(f"{plugin}:NOT_CALLABLE")
    assert "not callable" in str(caught.value)
    assert "int" in str(caught.value)


# ---------------------------------------------------------------------------
# Structural checking, and why it is not issubclass
# ---------------------------------------------------------------------------


def test_issubclass_against_the_protocol_is_a_TypeError():
    """Pins the reason the registry checks methods instead.

    ``Source`` is ``runtime_checkable`` and carries ``type_name``, a non-method
    member. If this ever stops raising, the registry could be simplified -- but
    while it does, an ``issubclass`` check would turn every resolution into a
    ``TypeError`` an operator cannot act on.
    """
    with pytest.raises(TypeError, match="non-method members"):
        issubclass(FileSource, Source)
    with pytest.raises(TypeError, match="non-method members"):
        issubclass(FileSource, Sink)


def test_an_instance_missing_protocol_methods_is_refused(plugin):
    with pytest.raises(ConfigError) as caught:
        reg.build_source(f"{plugin}:Incomplete", {"path": "landing/"})
    message = str(caught.value)
    assert "does not implement the Source protocol" in message
    assert "missing bind" in message
    assert "structural" in message


def test_the_method_lists_match_the_ones_model_validation_uses():
    """Two checks of the same property must not drift apart."""
    assert reg.SOURCE_METHODS == _SOURCE_METHODS
    assert reg.SINK_METHODS == _SINK_METHODS


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_build_passes_the_block_through_as_keyword_arguments():
    source = reg.build_source(
        "file", {"path": "landing/", "marker": "_DONE", "max_files_per_trigger": 3}
    )
    assert isinstance(source, FileSource)
    assert source.marker == "_DONE"
    assert source.max_files_per_trigger == 3


def test_an_unknown_keyword_is_refused_with_a_suggestion():
    with pytest.raises(ConfigError) as caught:
        reg.build_source("file", {"path": "landing/", "max_files_per_triger": 3})
    message = str(caught.value)
    assert "unknown key 'max_files_per_triger'" in message
    assert "Did you mean 'max_files_per_trigger'?" in message
    assert "silently does nothing" in message


def test_a_factory_taking_kwargs_accepts_anything(plugin):
    """``**kwargs`` means the loader cannot know the keys, so it must not guess."""
    path = f"{plugin}:Nested.Inner"
    assert reg.UDFS.accepted_keys(path) is None
    assert reg.resolve_udf(path)(anything="goes").kwargs == {"anything": "goes"}


def test_a_missing_required_argument_is_a_config_error():
    with pytest.raises(ConfigError) as caught:
        reg.build_source("file", {})
    assert "could not construct source 'file'" in str(caught.value)


def test_a_source_that_validates_its_own_arguments_keeps_its_message():
    """``FileSource`` already raises ConfigError; the registry must not bury it."""
    with pytest.raises(ConfigError) as caught:
        reg.build_source("file", {"path": "landing/", "format": "avro"})
    assert "'format' must be one of" in str(caught.value)


def test_accepted_keys_is_none_when_the_factory_takes_kwargs(plugin):
    assert reg.SOURCES.accepted_keys("file") is not None
    assert "marker" in reg.SOURCES.accepted_keys("file")


# ---------------------------------------------------------------------------
# Registering from Python
# ---------------------------------------------------------------------------


class LibrarySource:
    type_name: ClassVar[str] = "library"

    def __init__(self, path: str) -> None:
        self.path = path

    def latest_offset(self) -> Offset:
        return {}

    def plan(self, start, end, limits: BatchLimits) -> BatchPlan:
        return BatchPlan.empty(start, end or {})

    def bind(self, con, plan: BatchPlan) -> str:
        return "v"

    def to_config(self) -> dict[str, Any]:
        return {"type": self.type_name, "path": self.path}


def test_register_source_adds_a_usable_name():
    reg.register_source("library", LibrarySource)
    assert "library" in reg.available_sources()
    assert reg.resolve_source("library") is LibrarySource
    assert reg.build_source("library", {"path": "x"}).path == "x"


def test_register_sink_and_udf_add_usable_names():
    reg.register_sink("library_sink", "duckstream.sources.files:FileSource")
    assert "library_sink" in reg.available_sinks()

    reg.register_udf("mean", len)
    assert reg.available_udfs() == ["mean"]
    assert reg.resolve_udf("mean") is len


def test_registering_over_a_built_in_needs_replace():
    with pytest.raises(ConfigError) as caught:
        reg.register_source("file", LibrarySource)
    assert "already registered" in str(caught.value)
    assert "replace=True" in str(caught.value)

    reg.register_source("file", LibrarySource, replace=True)
    assert reg.resolve_source("file") is LibrarySource


@pytest.mark.parametrize("name", ["my_pkg:Thing", "two words", "", "   "])
def test_names_that_would_be_read_as_paths_are_refused(name):
    with pytest.raises(ConfigError):
        reg.register_source(name, LibrarySource)


def test_registering_a_non_callable_is_refused():
    with pytest.raises(ConfigError, match="must be registered as a callable"):
        reg.register_source("thing", 42)


def test_registering_a_string_without_a_colon_is_refused():
    with pytest.raises(ConfigError, match="not a dotted path"):
        reg.register_source("thing", "duckstream.sources.files.FileSource")


def test_unregister_removes_a_name_and_never_raises():
    reg.register_source("library", LibrarySource)
    reg.SOURCES.unregister("library")
    reg.SOURCES.unregister("library")  # again: still fine
    assert "library" not in reg.available_sources()


# ---------------------------------------------------------------------------
# The generic entry point
# ---------------------------------------------------------------------------


def test_resolve_dispatches_on_kind():
    assert reg.resolve("file", "source") is FileSource
    with pytest.raises(ConfigError, match="unknown source type 'table'"):
        reg.resolve("table", "source")


def test_resolve_refuses_an_unknown_namespace():
    with pytest.raises(ConfigError, match="unknown registry namespace"):
        reg.resolve("file", "sauce")
