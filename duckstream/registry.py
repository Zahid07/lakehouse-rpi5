"""The capability boundary between config and code.

A YAML document can express declarative structure -- a path, a grain, a merge
key -- but it cannot express a *function*. Sources, sinks and UDFs are code.
This module is the seam between the two: a short built-in name (``file``,
``table``) or a dotted path to anything importable (``my_pkg.sources:MySource``),
so config stays fully capable without turning into a programming language
(``CONTEXT.md`` section 4, "Callables in config").

Three things here are deliberate rather than incidental.

**Built-ins are registered as dotted-path strings, resolved on first use.**
Nothing is imported when ``duckstream.registry`` is imported. On a Raspberry Pi
the cold start of a cron tick is ~235 ms of process overhead before any work
happens (``CONTEXT.md`` section 1.8), and a registry that eagerly imports every
connector adds to exactly that number. It also means a broken or half-installed
optional connector degrades to a clear :class:`~duckstream.errors.ConfigError`
naming it, instead of breaking ``import duckstream`` outright.

**Namespaces are separate.** ``resolve_source("table")`` fails. A sink name
accepted where a source belongs would produce a ``Model`` that fails much later,
somewhere less obvious, with a much worse message.

**Nothing here uses ``issubclass``.** :class:`duckstream.protocols.Source` is a
``runtime_checkable`` ``Protocol`` carrying a non-method member (``type_name``),
and ``issubclass`` against such a protocol raises ``TypeError: Protocols with
non-method members don't support issubclass()``. More importantly, a structural
protocol is the point: a user source is *any* object of the right shape, with no
base class to inherit. So the checks are the same ones
:meth:`duckstream.model.Model.validate` makes -- the resolved object must be
callable, and the instance it produces must have the required methods.

Every failure in this module raises ``ConfigError`` naming the path or name that
failed, with the underlying ``ImportError``/``AttributeError`` quoted. This code
runs during ``duckstream validate`` at deploy time, and that message is the
entire diagnosis an operator gets.
"""

from __future__ import annotations

import difflib
import importlib
import inspect
from collections.abc import Iterable, Mapping
from typing import Any

from duckstream.errors import ConfigError, DuckstreamError

__all__ = [
    "Registry",
    "SOURCES",
    "SINKS",
    "UDFS",
    "SOURCE_METHODS",
    "SINK_METHODS",
    "KINDS",
    "resolve",
    "resolve_source",
    "resolve_sink",
    "resolve_udf",
    "register_source",
    "register_sink",
    "register_udf",
    "available_sources",
    "available_sinks",
    "available_udfs",
    "build_source",
    "build_sink",
    "unknown_key_message",
]


#: Methods an object must have to be usable as a source. Mirrors
#: ``duckstream.model._SOURCE_METHODS`` on purpose -- the registry rejects a
#: malformed source at *resolution* time, ``Model.validate`` rejects one that
#: arrived through the Python front door. ``tests/unit/test_registry.py``
#: asserts the two lists have not drifted apart.
SOURCE_METHODS: tuple[str, ...] = ("latest_offset", "plan", "bind", "to_config")

#: Methods an object must have to be usable as a sink.
SINK_METHODS: tuple[str, ...] = ("ensure", "write", "to_config")

#: The three namespaces.
KINDS: tuple[str, ...] = ("source", "sink", "udf")

_DOTTED_HINT = {
    "source": "my_pkg.sources:MySource",
    "sink": "my_pkg.sinks:MySink",
    "udf": "my_pkg.signal:arrow_fft",
}


class _Pending:
    """A name that is reserved but not implemented yet.

    Registered rather than omitted so that ``mqtt`` appears in the list of
    available names and produces "not implemented until phase 5" instead of
    "unknown source type", which would send an operator hunting for a typo.
    """

    __slots__ = ("note", "message")

    def __init__(self, note: str, message: str) -> None:
        self.note = note
        self.message = message


class Registry:
    """One namespace of names to callables.

    An entry is either a callable (a class or a factory function), a dotted-path
    string resolved and cached on first use, or a :class:`_Pending` marker.
    """

    def __init__(self, kind: str, *, methods: Iterable[str] = ()) -> None:
        self.kind = kind
        self.methods: tuple[str, ...] = tuple(methods)
        self._entries: dict[str, Any] = {}

    # -- registration ------------------------------------------------------

    def register(self, name: str, target: Any, *, replace: bool = False) -> None:
        """Add ``name`` to this namespace.

        ``target`` may be a callable or a ``"pkg.module:object"`` string, which
        is not imported until the name is first resolved.

        Re-registering an existing name requires ``replace=True``. Shadowing
        ``file`` or ``table`` by accident -- two plugins picking the same short
        name, say -- would change which code a config document runs with no
        diagnostic at all, so it has to be asked for explicitly.
        """
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(
                f"a {self.kind} name must be a non-empty string, got {name!r}"
            )
        name = name.strip()
        if ":" in name or any(c.isspace() for c in name):
            raise ConfigError(
                f"{self.kind} name {name!r} is not usable as a short name: a name "
                f"containing ':' or whitespace would be read as a dotted path "
                f"instead. Register it under a plain name."
            )
        if name in self._entries and not replace:
            raise ConfigError(
                f"{self.kind} name {name!r} is already registered as "
                f"{self._describe(self._entries[name])}. Pass replace=True if "
                f"shadowing it is deliberate; silently taking over a built-in "
                f"name would change what a config document runs."
            )
        if not (callable(target) or isinstance(target, (str, _Pending))):
            raise ConfigError(
                f"{self.kind} {name!r} must be registered as a callable or as a "
                f"'pkg.module:object' string, got {type(target).__name__}"
            )
        if isinstance(target, str) and ":" not in target:
            raise ConfigError(
                f"{self.kind} {name!r} was registered as the string {target!r}, "
                f"which is not a dotted path. Use 'pkg.module:object'."
            )
        self._entries[name] = target

    def unregister(self, name: str) -> None:
        """Remove ``name`` if present. Never raises."""
        self._entries.pop(name, None)

    def names(self) -> list[str]:
        """Registered short names, sorted."""
        return sorted(self._entries)

    def snapshot(self) -> dict[str, Any]:
        """A copy of the current entries, for tests that register temporarily."""
        return dict(self._entries)

    def restore(self, snapshot: Mapping[str, Any]) -> None:
        """Replace the entries with ``snapshot``."""
        self._entries = dict(snapshot)

    # -- resolution --------------------------------------------------------

    def resolve(self, name: str) -> Any:
        """Return the callable for a short name or a ``pkg.module:object`` path.

        Raises:
            ConfigError: for an unknown short name (the message lists what *is*
                available), a malformed path, an unimportable module, a missing
                attribute, or an object that is not callable.
        """
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(
                f"a {self.kind} type must be a non-empty string naming a built-in "
                f"({self._name_list()}) or a dotted path such as "
                f"{_DOTTED_HINT[self.kind]!r}; got {name!r}"
            )
        name = name.strip()

        if ":" in name:
            obj = self._resolve_dotted(name)
            self._require_callable(obj, name)
            return obj

        try:
            entry = self._entries[name]
        except KeyError:
            raise self._unknown(name) from None

        if isinstance(entry, _Pending):
            raise ConfigError(entry.message)
        if isinstance(entry, str):
            entry = self._resolve_dotted(entry, declared_as=name)
            # Cache the resolved object so the import cost is paid once.
            self._entries[name] = entry
        self._require_callable(entry, name)
        return entry

    def accepted_keys(self, name: str) -> frozenset[str] | None:
        """Keyword arguments the factory for ``name`` accepts.

        ``None`` means "anything" -- the factory takes ``**kwargs``, or its
        signature could not be inspected. Used by the loader to reject a typo'd
        key in a source or sink block while it still knows the line number.
        """
        return _accepted_keys(self.resolve(name))

    def parameter_annotations(self, name: str) -> dict[str, Any]:
        """Declared type of each keyword the factory for ``name`` accepts.

        Values are whatever the factory annotated -- a real type, or a string
        under ``from __future__ import annotations``. Absent annotations come
        back as ``inspect.Parameter.empty``.

        The loader uses this to decide what an environment variable should
        become: ``${MAX_FILES}`` targeting ``max_files_per_trigger: int | None``
        is an integer. The declared type is the authority, never the shape of
        the substituted text -- guessing from the text would make
        ``${TABLE_SUFFIX}`` of ``"2024"`` silently numeric.
        """
        return _parameter_annotations(self.resolve(name))

    # -- construction ------------------------------------------------------

    def build(self, name: str, kwargs: Mapping[str, Any] | None = None) -> Any:
        """Resolve ``name``, call it with ``kwargs``, then check the shape.

        The instance check is the one ``Model.validate`` makes -- required
        methods present -- and never ``issubclass``: see the module docstring.
        """
        kwargs = dict(kwargs or {})
        factory = self.resolve(name)
        self._check_kwargs(factory, kwargs, name)
        try:
            instance = factory(**kwargs)
        except DuckstreamError:
            # FileSource and friends validate their own arguments and raise
            # ConfigError with a better message than anything here could.
            raise
        except TypeError as exc:
            raise ConfigError(
                f"could not construct {self.kind} {name!r} from the keys "
                f"{sorted(kwargs)}: {type(exc).__name__}: {exc}"
            ) from exc
        except Exception as exc:
            raise ConfigError(
                f"{self.kind} {name!r} raised while being constructed from the "
                f"keys {sorted(kwargs)}: {type(exc).__name__}: {exc}"
            ) from exc
        self.check_instance(instance, name)
        return instance

    def check_instance(self, instance: Any, label: str) -> Any:
        """Verify ``instance`` structurally implements this namespace's protocol."""
        if not self.methods:
            return instance
        missing = [m for m in self.methods if not callable(getattr(instance, m, None))]
        if missing:
            raise ConfigError(
                f"{self.kind} {label!r} produced a {type(instance).__name__}, "
                f"which does not implement the {self.kind.capitalize()} protocol: "
                f"missing {', '.join(missing)}. duckstream protocols are "
                f"structural -- there is no base class to inherit -- so an object "
                f"is a {self.kind} exactly when it has "
                f"{', '.join(self.methods)}."
            )
        return instance

    # -- internals ---------------------------------------------------------

    def _resolve_dotted(self, path: str, *, declared_as: str | None = None) -> Any:
        via = f" (registered for {declared_as!r})" if declared_as else ""
        module_name, sep, attribute_path = path.partition(":")
        if not sep or not module_name.strip() or not attribute_path.strip():
            raise ConfigError(
                f"{self.kind} {path!r}{via} is not a usable dotted path. The form "
                f"is 'pkg.module:object' -- module on the left of the colon, "
                f"object on the right, for example {_DOTTED_HINT[self.kind]!r}."
            )
        module_name = module_name.strip()
        attribute_path = attribute_path.strip()

        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise ConfigError(
                f"could not import module {module_name!r} for {self.kind} "
                f"{path!r}{via}: {type(exc).__name__}: {exc}. The module has to "
                f"be importable by the interpreter that runs duckstream -- check "
                f"it is installed in the same environment and on sys.path."
            ) from exc
        except Exception as exc:
            raise ConfigError(
                f"module {module_name!r} raised while being imported for "
                f"{self.kind} {path!r}{via}: {type(exc).__name__}: {exc}. "
                f"Importing it must not have side effects that can fail; "
                f"duckstream imports it during `duckstream validate`."
            ) from exc

        obj: Any = module
        walked = module_name
        for part in attribute_path.split("."):
            try:
                obj = getattr(obj, part)
            except AttributeError as exc:
                public = sorted(n for n in dir(obj) if not n.startswith("_"))
                available = ", ".join(repr(n) for n in public) or "nothing public"
                raise ConfigError(
                    f"{self.kind} {path!r}{via} imported {walked!r}, but it has "
                    f"no attribute {part!r}: {type(exc).__name__}: {exc}. "
                    f"It defines: {available}."
                ) from exc
            walked = f"{walked}.{part}"
        return obj

    def _require_callable(self, obj: Any, label: str) -> None:
        if not callable(obj):
            raise ConfigError(
                f"{self.kind} {label!r} resolved to a {type(obj).__name__}, which "
                f"is not callable. A {self.kind} name must resolve to a class or "
                f"a factory that duckstream can call with the block's keys as "
                f"keyword arguments."
            )

    def _check_kwargs(self, factory: Any, kwargs: Mapping[str, Any], name: str) -> None:
        accepted = _accepted_keys(factory)
        if accepted is None:
            return
        unknown = sorted(k for k in kwargs if k not in accepted)
        if not unknown:
            return
        raise ConfigError(
            unknown_key_message(unknown[0], accepted, f"the {name!r} {self.kind} block")
        )

    def _unknown(self, name: str) -> ConfigError:
        suggestion = difflib.get_close_matches(name, self._entries, n=1, cutoff=0.6)
        did_you_mean = f" Did you mean {suggestion[0]!r}?" if suggestion else ""
        return ConfigError(
            f"unknown {self.kind} type {name!r}.{did_you_mean} Available "
            f"{self.kind} types: {self._name_list()}. Anything outside duckstream "
            f"is named by dotted path instead, for example "
            f"{_DOTTED_HINT[self.kind]!r}."
        )

    def _name_list(self) -> str:
        if not self._entries:
            return "none are registered"
        rendered = []
        for entry_name in sorted(self._entries):
            entry = self._entries[entry_name]
            if isinstance(entry, _Pending):
                rendered.append(f"{entry_name!r} ({entry.note})")
            else:
                rendered.append(repr(entry_name))
        return ", ".join(rendered)

    @staticmethod
    def _describe(entry: Any) -> str:
        if isinstance(entry, _Pending):
            return f"a placeholder ({entry.note})"
        if isinstance(entry, str):
            return repr(entry)
        module = getattr(entry, "__module__", "?")
        qualname = getattr(entry, "__qualname__", None) or repr(entry)
        return f"{module}.{qualname}"

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return f"Registry(kind={self.kind!r}, names={self.names()!r})"


def _accepted_keys(factory: Any) -> frozenset[str] | None:
    """Keyword names ``factory`` accepts, or ``None`` if it accepts anything."""
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):  # pragma: no cover - C callables
        return None
    parameters = list(signature.parameters.values())
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters):
        return None
    return frozenset(
        p.name
        for p in parameters
        if p.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    )


def _parameter_annotations(factory: Any) -> dict[str, Any]:
    """Declared type of each keyword ``factory`` accepts, keyed by name.

    Annotations are returned exactly as declared -- a real type, or a string
    where the defining module uses ``from __future__ import annotations``.
    Resolving them is the caller's business, and deliberately so: evaluating a
    user plugin's annotations can raise ``NameError``, which is not a reason to
    refuse an otherwise valid config.
    """
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):  # pragma: no cover - C callables
        return {}
    return {
        p.name: p.annotation
        for p in signature.parameters.values()
        if p.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }


def unknown_key_message(key: str, accepted: Iterable[str], what: str) -> str:
    """The message for a key nothing will ever read.

    Shared with :mod:`duckstream.config` so a typo is reported the same way
    wherever it is caught. Rejecting rather than ignoring is the point: a
    ``max_files_per_triger`` that silently does nothing is precisely the failure
    ``duckstream validate`` exists to catch at deploy time rather than at 03:00
    in a cron log.
    """
    accepted = sorted(accepted)
    suggestion = difflib.get_close_matches(key, accepted, n=1, cutoff=0.6)
    did_you_mean = f" Did you mean {suggestion[0]!r}?" if suggestion else ""
    known = ", ".join(repr(a) for a in accepted) if accepted else "no keys at all"
    return (
        f"unknown key {key!r} in {what}.{did_you_mean} Accepted keys: {known}. "
        f"An unrecognised key is refused rather than ignored, because a typo "
        f"that silently does nothing is the failure this check exists to catch."
    )


# --------------------------------------------------------------------------
# The three namespaces, and the built-in names
# --------------------------------------------------------------------------

SOURCES = Registry("source", methods=SOURCE_METHODS)
SINKS = Registry("sink", methods=SINK_METHODS)
UDFS = Registry("udf")

_REGISTRIES: dict[str, Registry] = {
    "source": SOURCES,
    "sink": SINKS,
    "udf": UDFS,
}

# Dotted-path strings, not imports: see the module docstring.
SOURCES.register("file", "duckstream.sources.files:FileSource")
SOURCES.register(
    "mqtt",
    _Pending(
        "not a source, and never will be",
        "the 'mqtt' source does not exist and is not coming. MQTT cannot be a "
        "source in the exactly-once sense at all: once a message is acked it is "
        "gone from the broker, so there is no offset to resume from and nothing "
        "to replay. It is built instead as a landing writer -- "
        "duckstream.sources.mqtt.MqttLandingWriter -- which subscribes and "
        "writes durably to disk, acknowledging each message only once it is on "
        "disk. Point a 'file' source at the same directory and that source is "
        "replayable, so exactly-once holds from there: "
        "broker -> MqttLandingWriter -> landing/ -> file source -> engine. "
        "So run the landing writer as a daemon, and declare "
        "source: {type: file, path: 'landing/', marker: _READY} on the model. "
        "It needs `pip install duckstream[mqtt]`.",
    ),
)
SINKS.register("table", "duckstream.sinks.table:TableSink")


def _registry(kind: str) -> Registry:
    try:
        return _REGISTRIES[kind]
    except KeyError:
        raise ConfigError(
            f"unknown registry namespace {kind!r}; expected one of "
            f"{', '.join(repr(k) for k in KINDS)}"
        ) from None


def resolve(name: str, kind: str) -> Any:
    """Resolve ``name`` in the ``kind`` namespace (``source``/``sink``/``udf``)."""
    return _registry(kind).resolve(name)


def resolve_source(name: str) -> Any:
    """Resolve a source name or dotted path to its class or factory."""
    return SOURCES.resolve(name)


def resolve_sink(name: str) -> Any:
    """Resolve a sink name or dotted path to its class or factory."""
    return SINKS.resolve(name)


def resolve_udf(name: str) -> Any:
    """Resolve a UDF name or dotted path to the callable itself.

    UDFs have no protocol to satisfy beyond being callable. The engine registers
    them with DuckDB before planning; the loader records only the dotted path,
    so nothing is imported at config-load time.
    """
    return UDFS.resolve(name)


def register_source(name: str, target: Any, *, replace: bool = False) -> None:
    """Add a source name usable from config (``type: <name>``)."""
    SOURCES.register(name, target, replace=replace)


def register_sink(name: str, target: Any, *, replace: bool = False) -> None:
    """Add a sink name usable from config (``type: <name>``)."""
    SINKS.register(name, target, replace=replace)


def register_udf(name: str, target: Any, *, replace: bool = False) -> None:
    """Add a UDF short name usable in place of a dotted path."""
    UDFS.register(name, target, replace=replace)


def available_sources() -> list[str]:
    """Registered source names."""
    return SOURCES.names()


def available_sinks() -> list[str]:
    """Registered sink names."""
    return SINKS.names()


def available_udfs() -> list[str]:
    """Registered UDF names."""
    return UDFS.names()


def build_source(name: str, kwargs: Mapping[str, Any] | None = None) -> Any:
    """Construct a source from a registry name and its keyword arguments."""
    return SOURCES.build(name, kwargs)


def build_sink(name: str, kwargs: Mapping[str, Any] | None = None) -> Any:
    """Construct a sink from a registry name and its keyword arguments."""
    return SINKS.build(name, kwargs)
