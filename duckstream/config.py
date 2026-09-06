"""The YAML front door: a deserialiser into ``Model``, and nothing more.

``PLAN.md`` is emphatic about the shape of this module, because a config layer
is where frameworks usually rot:

1. **``Model`` is the single source of truth.** All validation lives on it.
2. **The loader is only a deserialiser.** It builds the same ``Model`` objects
   the Python API builds and then runs the same :meth:`Model.validate`. There is
   no parallel validation and no parallel execution path.
3. **No merge or precedence semantics.** Config is a constructor, not an
   override layer. Environment differences are expressed with ``${VAR}``
   substitution, or by loading the config and adjusting it in Python.

Because ``${VAR}`` is *the* mechanism for environment differences and there are
no CLI overrides, a substituted value has to be able to become a number or a
boolean -- otherwise ``max_files_per_trigger`` and ``threads`` could not vary
between environments at all. So a value that is *entirely* one reference takes
the declared type of the field it lands in; see :class:`_Substituted` for why
that is drawn as narrowly as it is.

So the only things this module decides are *shape* questions -- is this a
mapping, is this key spelled correctly, which registry name builds this source.
Every *meaning* question -- is ``hour`` a grain, may ``delta_merge`` fold a
median, must the merge key contain ``window_ts`` -- belongs to
:meth:`duckstream.model.Model.validate` and is answered there, identically for
both front doors. The mechanical guard on that split is the round-trip property
test in ``tests/unit/test_config.py``.

**Error quality is this module's deliverable.** ``duckstream validate`` runs at
deploy time and its message is all the operator gets, so every failure names the
file, the model and the field, with a line number wherever the YAML node marks
supply one. Unknown keys are **rejected, never ignored**: a typo'd
``max_files_per_triger`` that silently does nothing is exactly the 03:00 cron
failure this is here to prevent. And where several models are wrong, all of them
are reported at once, because an operator fixing a config wants the whole list
rather than one item per edit-run cycle.

All YAML handling is confined to this module. ``CONTEXT.md`` section 4 keeps
stdlib ``tomllib`` a cheap swap if ``pyyaml`` ever becomes unwelcome on a
constrained device, and that is only true while nothing else imports ``yaml``.
"""

from __future__ import annotations

import inspect
import os
import re
import types
import typing
from collections.abc import Mapping, Sequence
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml

from duckstream.errors import ConfigError, ModelValidationError
from duckstream.model import Model
from duckstream.protocols import BatchLimits
from duckstream.registry import SINKS, SOURCES, unknown_key_message

__all__ = [
    "ConfigDocument",
    "DOCUMENT_KEYS",
    "MODEL_KEYS",
    "LIMIT_KEYS",
    "load_config",
    "parse_config",
    "parse_yaml",
    "substitute_env",
]


#: Keys allowed at the top level of a document.
DOCUMENT_KEYS: tuple[str, ...] = ("catalog", "data_path", "settings", "models")

#: Keys allowed in a model block. Derived from the ``Model`` dataclass rather
#: than written out, so a field added to ``Model`` is accepted by the loader
#: automatically and the round-trip test is what decides whether it round-trips.
MODEL_KEYS: tuple[str, ...] = tuple(f.name for f in fields(Model))

#: Model keys with no default: absence is an error, not a default.
REQUIRED_MODEL_KEYS: tuple[str, ...] = tuple(
    f.name
    for f in fields(Model)
    if f.default is MISSING and f.default_factory is MISSING
)

#: Keys allowed in a model's ``limits`` block.
LIMIT_KEYS: tuple[str, ...] = tuple(f.name for f in fields(BatchLimits))

#: Declared type of each field, used to give a substituted ``${VAR}`` the type
#: its target expects. Taken from the dataclass rather than written out, so a
#: new field is coerced correctly without anyone remembering to add it here.
_MODEL_ANNOTATIONS: dict[str, Any] = {f.name: f.type for f in fields(Model)}
_LIMIT_ANNOTATIONS: dict[str, Any] = {f.name: f.type for f in fields(BatchLimits)}

#: Value types a ``settings`` entry may hold. Mirrors what ``duckstream.lake``
#: is willing to interpolate into ``SET``; anything else is refused here so the
#: failure lands at deploy time rather than on the first connection.
_SETTING_TYPES = (str, int, float, bool)

#: ``${NAME}`` or ``${NAME:-default}``. Nothing else is a reference -- a bare
#: ``$`` is an ordinary character, so an aggregate expression or a regex
#: containing one passes through untouched.
_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")

#: Scalar types a substituted value may be coerced to, in the order a union is
#: resolved. ``bool`` precedes ``int`` because ``bool`` is a subclass of it.
_SCALAR_KINDS: tuple[type, ...] = (bool, int, float, str)

_SCALAR_BY_NAME: dict[str, type] = {k.__name__: k for k in _SCALAR_KINDS}

#: The only boolean spellings accepted, case-insensitively. Deliberately short:
#: anything cleverer starts guessing at intent.
_TRUE_WORDS = frozenset({"true", "yes", "1"})
_FALSE_WORDS = frozenset({"false", "no", "0"})

#: A value that is unambiguously an integer or a float. Used only where the
#: target has no declared type (the ``settings`` block); ``inf`` and ``nan`` are
#: excluded on purpose, since neither is a plausible setting and ``lake.py``
#: refuses them anyway.
_INTEGER_TEXT = re.compile(r"^[+-]?\d+$")
_FLOAT_TEXT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")


class _Substituted(str):
    """A string that came *entirely* from one ``${VAR}`` reference.

    The marker exists so type coercion can be narrow. ``PLAN.md`` makes
    ``${VAR}`` substitution the mechanism for environment differences and
    forbids CLI overrides, so a numeric knob that could not vary between
    environments would gut it: ``max_files_per_trigger: "${MAX_FILES}"`` has to
    end up an ``int``.

    But only that case. A literal ``"10"`` the user typed stays a string and is
    still refused -- quoting a number is a mistake, and silently accepting it
    would teach a habit that breaks the moment the value is not numeric. So
    does ``"landing/${STAGE}"``, where the reference is one part of a larger
    string and the result is obviously text.

    Nothing of this type may escape into a ``Model``: ``yaml.safe_dump`` refuses
    ``str`` subclasses, which would break the round-trip. Every value therefore
    passes through :meth:`_Deserialiser.coerce`, which returns either a coerced
    scalar or a plain ``str``.
    """

    __slots__ = ("variable", "used_default")

    def __new__(cls, value: str, *, variable: str, used_default: bool):
        text = super().__new__(cls, value)
        text.variable = variable
        text.used_default = used_default
        return text


def _target_kind(annotation: Any) -> type | None:
    """The scalar type ``annotation`` declares, or ``None`` if it declares none.

    Handles a real type, a ``X | None`` union, and the *string* annotations that
    ``from __future__ import annotations`` produces -- which is what every
    module in duckstream has, so the string path is the common one.
    """
    if annotation is inspect.Parameter.empty or annotation is None:
        return None
    found: set[type] = set()
    _collect_scalars(annotation, found)
    for kind in _SCALAR_KINDS:
        if kind in found:
            return kind
    return None


def _collect_scalars(annotation: Any, found: set[type]) -> None:
    if isinstance(annotation, str):
        # "str | os.PathLike[str]" -> {str}; "dict[str, str]" -> {} because the
        # subscript is never split apart into a bare token.
        for token in annotation.replace("]", "|").split("|"):
            token = token.strip().removeprefix("builtins.")
            if token in _SCALAR_BY_NAME:
                found.add(_SCALAR_BY_NAME[token])
        return
    if isinstance(annotation, type):
        if annotation in _SCALAR_KINDS:
            found.add(annotation)
        return
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:
        for argument in typing.get_args(annotation):
            _collect_scalars(argument, found)


def _parse_scalar(text: str, kind: type) -> Any:
    """Turn substituted text into ``kind``. Raises ``ValueError`` if it cannot."""
    stripped = text.strip()
    if kind is bool:
        lowered = stripped.lower()
        if lowered in _TRUE_WORDS:
            return True
        if lowered in _FALSE_WORDS:
            return False
        raise ValueError(stripped)
    if kind is int:
        if not _INTEGER_TEXT.match(stripped):
            raise ValueError(stripped)
        return int(stripped)
    if kind is float:
        if not _FLOAT_TEXT.match(stripped):
            raise ValueError(stripped)
        return float(stripped)
    return text


def _free_form_scalar(text: str) -> Any:
    """Coerce where nothing declares a type -- the ``settings`` block.

    Only an unambiguous number or boolean is converted. ``memory_limit: 2GB``
    is not a number and must stay a string, and so must anything else the
    engine will hand to ``SET`` as a quoted literal.
    """
    stripped = text.strip()
    lowered = stripped.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if _INTEGER_TEXT.match(stripped):
        return int(stripped)
    if _FLOAT_TEXT.match(stripped) and not _INTEGER_TEXT.match(stripped):
        return float(stripped)
    return str(text)


def _plain(value: Any) -> Any:
    """Recursively strip :class:`_Substituted` markers, leaving ordinary types."""
    if isinstance(value, _Substituted):
        return str(value)
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _kind_name(kind: type) -> str:
    return {bool: "a boolean", int: "an integer", float: "a number"}.get(
        kind, "a string"
    )


# --------------------------------------------------------------------------
# The document
# --------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class ConfigDocument:
    """A parsed configuration: engine settings plus validated models.

    Frozen, because it is a deserialisation result and not a place to put
    override semantics. To vary a model per environment, either use ``${VAR}``
    in the YAML or take ``document.models`` and build a new ``Model`` in Python
    -- both of which keep ``Model`` the only thing the engine ever runs.
    """

    catalog: str
    """DuckLake catalog DSN, e.g. ``ducklake:catalog.ducklake``."""

    data_path: str | None = None
    """Where parquet data files go. Only used when creating a new catalog."""

    settings: dict[str, Any] = field(default_factory=dict)
    """``SET`` values applied to every connection, e.g. ``memory_limit``."""

    models: list[Model] = field(default_factory=list)
    """Every model in the document, already validated."""

    path: str | None = field(default=None, compare=False)
    """Where the document was read from, for messages. Not part of equality."""

    @property
    def names(self) -> list[str]:
        """Declared model names, in document order."""
        return [m.name for m in self.models]

    def model(self, name: str) -> Model:
        """The model called ``name``. Supports ``run --model NAME``."""
        for candidate in self.models:
            if candidate.name == name:
                return candidate
        known = ", ".join(repr(n) for n in self.names) or "none"
        raise ConfigError(
            f"no model named {name!r} in this configuration. Declared models: "
            f"{known}.",
            path=self.path,
        )

    def to_config(self) -> dict[str, Any]:
        """The document as a plain dict, the inverse of :func:`parse_config`."""
        config: dict[str, Any] = {"catalog": self.catalog}
        if self.data_path is not None:
            config["data_path"] = self.data_path
        if self.settings:
            config["settings"] = dict(self.settings)
        config["models"] = [m.to_config() for m in self.models]
        return config


# --------------------------------------------------------------------------
# Public entry points
# --------------------------------------------------------------------------


def load_config(
    path: str | os.PathLike[str], *, env: Mapping[str, str] | None = None
) -> ConfigDocument:
    """Read a YAML file and deserialise it into a :class:`ConfigDocument`.

    Args:
        path: The YAML document.
        env: Environment used for ``${VAR}`` substitution. Defaults to
            ``os.environ``; passing one is for tests and for embedding.

    Raises:
        ConfigError: for anything about the document -- unreadable file, invalid
            YAML, unknown key, unresolvable registry name, unset variable -- and
            for several bad models at once.
        ModelValidationError: when exactly one model is invalid. It is the same
            error the Python front door raises for the same declaration, with
            the file and line appended, because the config path must not give a
            weaker rejection than the Python path.
    """
    location = os.fspath(path)
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            f"could not read the configuration file: {type(exc).__name__}: {exc}",
            path=location,
        ) from exc
    return parse_yaml(text, source=location, env=env)


def parse_yaml(
    text: str,
    *,
    source: str | None = None,
    env: Mapping[str, str] | None = None,
) -> ConfigDocument:
    """Deserialise a YAML string. ``source`` names it in error messages."""
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(_yaml_message(exc), path=_yaml_location(exc, source)) from exc

    lines = _line_index(text, source)
    if raw is None:
        raise ConfigError(
            "the configuration is empty. A document needs at least 'catalog' and "
            "'models'.",
            path=source,
        )
    return parse_config(raw, source=source, lines=lines, env=env)


def parse_config(
    document: Mapping[str, Any],
    *,
    source: str | None = None,
    lines: Mapping[tuple[Any, ...], int] | None = None,
    env: Mapping[str, str] | None = None,
) -> ConfigDocument:
    """Deserialise an already-loaded mapping into a :class:`ConfigDocument`.

    The same code path :func:`load_config` uses, exposed so a caller that
    already has a dict -- a test, an embedding application, a different file
    format -- does not have to round-trip through YAML text to reach it.

    ``lines`` maps a path tuple such as ``("models", 0, "grain")`` to a 1-based
    line number, and is what :func:`load_config` supplies from the YAML node
    marks. Without it, errors simply omit the line.
    """
    return _Deserialiser(source=source, lines=lines, env=env).document(document)


def substitute_env(
    value: Any, *, env: Mapping[str, str] | None = None
) -> Any:
    """Apply ``${VAR}`` / ``${VAR:-default}`` substitution to string values.

    Exposed because it is a documented part of the config contract, and useful
    on its own. Substitution reaches string *values* at any depth, and never
    mapping keys: a key is part of the document's schema, and a schema that
    varies with the environment would defeat the unknown-key check.

    Results are always strings. Type coercion needs a target field to take the
    type from, which only the loader has -- see :class:`_Substituted`.
    """
    return _plain(_Deserialiser(env=env).substituted(value, ()))


# --------------------------------------------------------------------------
# Deserialisation
# --------------------------------------------------------------------------


class _Deserialiser:
    """One pass over one document. Holds the error-location machinery."""

    def __init__(
        self,
        *,
        source: str | None = None,
        lines: Mapping[tuple[Any, ...], int] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self.source = source
        self.lines: Mapping[tuple[Any, ...], int] = dict(lines or {})
        self.env: Mapping[str, str] = os.environ if env is None else env

    # -- error location ----------------------------------------------------

    def line(self, path: tuple[Any, ...]) -> int | None:
        """Line of ``path``, or of its nearest recorded ancestor."""
        probe = tuple(path)
        while True:
            if probe in self.lines:
                return self.lines[probe]
            if not probe:
                return None
            probe = probe[:-1]

    def at(self, path: tuple[Any, ...], model: str | None = None) -> str:
        """A human location: ``models.yaml:12, model 'hourly_counts'``."""
        parts: list[str] = []
        line = self.line(path)
        if self.source:
            parts.append(f"{self.source}:{line}" if line else str(self.source))
        elif line:
            parts.append(f"line {line}")
        if model:
            parts.append(f"model {model!r}")
        return ", ".join(parts)

    def fail(
        self, path: tuple[Any, ...], message: str, *, model: str | None = None
    ) -> None:
        raise ConfigError(message, path=self.at(path, model) or None)

    def relocate(self, exc: ConfigError, path: tuple[Any, ...], model: str | None):
        """Re-raise a registry or component error with this document's location."""
        return ConfigError(str(exc), path=self.at(path, model) or None)

    # -- type coercion of substituted values -------------------------------

    def coerce(
        self,
        value: Any,
        annotation: Any,
        *,
        path: tuple[Any, ...],
        field_name: str,
        model: str | None = None,
    ) -> Any:
        """Give a fully substituted value the declared type of its target.

        Only a value that came entirely from one ``${VAR}`` is touched, and the
        type comes from the target's annotation rather than the shape of the
        text -- so ``${TABLE_SUFFIX}`` resolving to ``"2024"`` stays a string
        when it lands in a ``str`` field.

        Everything else is returned unchanged, except that a plain
        :class:`_Substituted` is downgraded to ``str`` so nothing of that type
        reaches a ``Model``.
        """
        if not isinstance(value, _Substituted):
            return value
        kind = _target_kind(annotation)
        if kind is None or kind is str:
            return str(value)
        try:
            return _parse_scalar(str(value), kind)
        except ValueError:
            origin = (
                f"the default of ${{{value.variable}:-...}}"
                if value.used_default
                else f"environment variable {value.variable!r}"
            )
            self.fail(
                path,
                f"{origin} resolved to {str(value)!r}, but {field_name!r} expects "
                f"{_kind_name(kind)}.",
                model=model,
            )

    def plain(self, value: Any) -> Any:
        """Strip the substitution marker without coercing. For ``str`` targets."""
        return _plain(value)

    # -- ${VAR} substitution -----------------------------------------------

    def substituted(
        self, value: Any, path: tuple[Any, ...], model: str | None = None
    ) -> Any:
        if isinstance(value, str):
            return self.substitute(value, path, model)
        if isinstance(value, Mapping):
            # Keys deliberately untouched: see substitute_env's docstring.
            return {
                key: self.substituted(item, path + (key,), model)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [
                self.substituted(item, path + (index,), model)
                for index, item in enumerate(value)
            ]
        return value

    def substitute(
        self, text: str, path: tuple[Any, ...], model: str | None = None
    ) -> str:
        matches = list(_VARIABLE.finditer(text))
        spans = [(m.start(), m.end()) for m in matches]

        index = text.find("${")
        while index != -1:
            if not any(start <= index < end for start, end in spans):
                self.fail(
                    path,
                    f"{text!r} contains a malformed variable reference at "
                    f"character {index}. The forms are ${{NAME}} and "
                    f"${{NAME:-default}}, where NAME is letters, digits and "
                    f"underscores. A literal '$' not followed by '{{' needs no "
                    f"escaping.",
                    model=model,
                )
            index = text.find("${", index + 2)

        if not matches:
            return text

        only = matches[0]
        if len(matches) == 1 and only.start() == 0 and only.end() == len(text):
            # The whole value is one reference, so it is eligible for coercion
            # to the declared type of whatever it lands in. See _Substituted.
            return _Substituted(
                self._variable(only, text, path, model),
                variable=only.group(1),
                used_default=only.group(2) is not None,
            )

        out: list[str] = []
        position = 0
        for match in matches:
            out.append(text[position : match.start()])
            out.append(self._variable(match, text, path, model))
            position = match.end()
        out.append(text[position:])
        return "".join(out)

    def _variable(
        self,
        match: re.Match[str],
        text: str,
        path: tuple[Any, ...],
        model: str | None,
    ) -> str:
        name = match.group(1)
        default = match.group(2)
        present = name in self.env
        value = self.env.get(name, "")

        if default is not None:
            # POSIX ':-' semantics: unset *or* empty falls back to the default.
            return default if (not present or value == "") else value
        if not present:
            self.fail(
                path,
                f"environment variable {name!r} is referenced by {text!r} but is "
                f"not set, and no default was given. duckstream refuses to "
                f"substitute an empty string here: a pipeline that quietly read "
                f"the wrong directory, or wrote to the wrong table, is far worse "
                f"than one that will not start.",
                model=model,
            )
        return value

    # -- the document ------------------------------------------------------

    def document(self, raw: Any) -> ConfigDocument:
        if not isinstance(raw, Mapping):
            self.fail(
                (),
                f"a configuration must be a mapping with the keys "
                f"{', '.join(repr(k) for k in DOCUMENT_KEYS)}; got "
                f"{type(raw).__name__}",
            )

        document = self.substituted(raw, ())

        for key in document:
            if key not in DOCUMENT_KEYS:
                self.fail(
                    ("__key__", key),
                    unknown_key_message(str(key), DOCUMENT_KEYS, "the document"),
                )

        catalog = self._catalog(document)
        data_path = self._data_path(document)
        settings = self._settings(document)
        models = self._models(document)

        return ConfigDocument(
            catalog=catalog,
            data_path=data_path,
            settings=settings,
            models=models,
            path=self.source,
        )

    def _catalog(self, document: Mapping[str, Any]) -> str:
        if "catalog" not in document:
            self.fail(
                (),
                "no 'catalog' declared. duckstream writes to a DuckLake catalog "
                "from phase 1 -- it is the storage layer, not an option -- so "
                "give one, for example catalog: 'ducklake:catalog.ducklake'.",
            )
        catalog = document["catalog"]
        if not isinstance(catalog, str) or not catalog.strip():
            self.fail(
                ("catalog",),
                f"'catalog' must be a non-empty string such as "
                f"'ducklake:catalog.ducklake'; got {catalog!r}",
            )
        return str(catalog)

    def _data_path(self, document: Mapping[str, Any]) -> str | None:
        if "data_path" not in document or document["data_path"] is None:
            return None
        data_path = document["data_path"]
        if not isinstance(data_path, str) or not data_path.strip():
            self.fail(
                ("data_path",),
                f"'data_path' must be a non-empty string naming the directory "
                f"parquet data files are written to; got {data_path!r}",
            )
        return str(data_path)

    def _settings(self, document: Mapping[str, Any]) -> dict[str, Any]:
        if "settings" not in document or document["settings"] is None:
            return {}
        settings = document["settings"]
        if not isinstance(settings, Mapping):
            self.fail(
                ("settings",),
                f"'settings' must be a mapping of DuckDB setting names to "
                f"values, e.g. {{memory_limit: '2GB', threads: 2}}; got "
                f"{type(settings).__name__}",
            )
        resolved: dict[str, Any] = {}
        for key, value in settings.items():
            if not isinstance(key, str):
                self.fail(
                    ("settings",),
                    f"setting name {key!r} must be a string",
                )
            if isinstance(value, _Substituted):
                # Nothing declares a type for a DuckDB setting, so only an
                # unambiguous number or boolean is converted. `memory_limit:
                # "${MEM}"` resolving to "2GB" has to stay a string.
                resolved[key] = _free_form_scalar(str(value))
                continue
            if isinstance(value, _SETTING_TYPES):
                resolved[key] = value
                continue
            self.fail(
                ("settings", key),
                f"setting {key!r} must be a string, integer, float or boolean -- "
                f"it is interpolated into a SQL `SET` statement -- got "
                f"{type(value).__name__}: {value!r}",
            )
        return resolved

    def _models(self, document: Mapping[str, Any]) -> list[Model]:
        if "models" not in document:
            self.fail(
                (),
                "no 'models' declared. A configuration with nothing to run is "
                "almost certainly a mistake, so it is refused rather than "
                "silently doing nothing.",
            )
        blocks = document["models"]
        if isinstance(blocks, (str, bytes)) or not isinstance(blocks, Sequence):
            self.fail(
                ("models",),
                f"'models' must be a list of model blocks; got "
                f"{type(blocks).__name__}",
            )
        if not blocks:
            self.fail(("models",), "'models' is empty; declare at least one model.")

        models: list[Model] = []
        errors: list[Exception] = []
        for index, block in enumerate(blocks):
            try:
                models.append(self._model(block, index))
            except ModelValidationError as exc:
                errors.append(self._located_model_error(exc, index))
            except ConfigError as exc:
                errors.append(exc)

        errors.extend(self._duplicate_names(models))
        if errors:
            self._raise(errors)
        return models

    def _duplicate_names(self, models: Sequence[Model]) -> list[Exception]:
        seen: dict[str, int] = {}
        problems: list[Exception] = []
        for index, model in enumerate(models):
            first = seen.get(model.name)
            if first is None:
                seen[model.name] = index
                continue
            problems.append(
                ConfigError(
                    f"two models are called {model.name!r} (the first at "
                    f"{self.at(('models', first)) or 'index ' + str(first)}). A "
                    f"model's name keys its offsets and watermarks in the state "
                    f"store, so two models sharing one would silently overwrite "
                    f"each other's checkpoints.",
                    path=self.at(("models", index, "name")) or None,
                )
            )
        return problems

    def _raise(self, errors: Sequence[Exception]) -> None:
        if len(errors) == 1:
            raise errors[0]
        where = f" in {self.source}" if self.source else ""
        body = "\n".join(f"  - {exc}" for exc in errors)
        aggregate = ConfigError(f"{len(errors)} problems{where}:\n{body}")
        # Structured access for a CLI that wants to render them itself.
        aggregate.errors = list(errors)  # type: ignore[attr-defined]
        raise aggregate

    def _located_model_error(
        self, exc: ModelValidationError, index: int
    ) -> ModelValidationError:
        """Re-render a model error with the file and line it came from.

        Deliberately the *same* exception type the Python front door raises, so
        a caller catching ``ModelValidationError`` behaves identically whichever
        door the model came through. Only the location is added.
        """
        path: tuple[Any, ...] = ("models", index)
        if exc.field and self.line(path + (exc.field,)) is not None:
            path = path + (exc.field,)
        where = self.at(path)
        if not where:
            return exc
        return ModelValidationError(
            f"{exc.reason} (declared at {where})",
            model=exc.model,
            field=exc.field,
            remedy=exc.remedy,
        )

    # -- one model ---------------------------------------------------------

    def _model(self, block: Any, index: int) -> Model:
        path: tuple[Any, ...] = ("models", index)
        if not isinstance(block, Mapping):
            self.fail(
                path,
                f"each entry under 'models' must be a mapping declaring one "
                f"model; entry {index} is a {type(block).__name__}",
            )

        raw_name = block.get("name")
        name = raw_name if isinstance(raw_name, str) else None

        for key in block:
            if key not in MODEL_KEYS:
                self.fail(
                    path + (key,),
                    unknown_key_message(str(key), MODEL_KEYS, "the model block"),
                    model=name,
                )

        for required in REQUIRED_MODEL_KEYS:
            if required not in block:
                self.fail(
                    path,
                    f"the model block has no {required!r}. Every model needs "
                    f"{', '.join(repr(k) for k in REQUIRED_MODEL_KEYS)}.",
                    model=name,
                )

        kwargs: dict[str, Any] = {}
        for key in MODEL_KEYS:
            if key not in block:
                continue
            value = block[key]
            field_path = path + (key,)
            if key == "source":
                kwargs[key] = self._component(SOURCES, value, field_path, name)
            elif key == "sink":
                kwargs[key] = self._component(SINKS, value, field_path, name)
            elif key == "limits":
                kwargs[key] = self._limits(value, field_path, name)
            else:
                kwargs[key] = self._scalar(key, value, field_path, name)

        model = Model(**kwargs)
        # The same validation the Python front door runs. Nothing above this
        # line decided anything about meaning.
        model.validate()
        return model

    def _component(
        self,
        registry: Any,
        raw: Any,
        path: tuple[Any, ...],
        model: str | None,
    ) -> Any:
        label = registry.kind
        if not isinstance(raw, Mapping):
            self.fail(
                path,
                f"the {label} must be a mapping with a 'type' key, for example "
                f"{{type: {registry.names()[0] if registry.names() else 'file'}, "
                f"...}}; got {type(raw).__name__}: {raw!r}",
                model=model,
            )
        block = dict(raw)
        type_name = block.pop("type", None)
        if type_name is None:
            self.fail(
                path,
                f"the {label} block has no 'type'. Give a built-in name "
                f"({', '.join(repr(n) for n in registry.names())}) or a dotted "
                f"path to your own class.",
                model=model,
            )
        if not isinstance(type_name, str):
            self.fail(
                path + ("type",),
                f"the {label} 'type' must be a string, got "
                f"{type(type_name).__name__}: {type_name!r}",
                model=model,
            )
        type_name = str(type_name)

        try:
            accepted = registry.accepted_keys(type_name)
        except ConfigError as exc:
            raise self.relocate(exc, path + ("type",), model) from exc

        if accepted is not None:
            for key in block:
                if key not in accepted:
                    self.fail(
                        path + (key,),
                        unknown_key_message(
                            str(key),
                            set(accepted) | {"type"},
                            f"the {type_name!r} {label} block",
                        ),
                        model=model,
                    )

        # A substituted value becomes whatever the factory says it accepts:
        # `max_files_per_trigger: "${MAX_FILES}"` against `int | None` is an int.
        annotations = registry.parameter_annotations(type_name)
        block = {
            key: self.coerce(
                value,
                annotations.get(key, inspect.Parameter.empty),
                path=path + (key,),
                field_name=key,
                model=model,
            )
            for key, value in block.items()
        }

        # A relative path in a config file means "relative to the config file",
        # not to whatever directory cron started in. Keyed off the factory's own
        # signature rather than the name 'file', so a user component opts in
        # simply by declaring the parameter. An explicit value in YAML wins.
        if "base_dir" in annotations and "base_dir" not in block and self.source:
            block["base_dir"] = os.path.dirname(os.path.abspath(self.source)) or "."

        try:
            return registry.build(type_name, block)
        except ConfigError as exc:
            raise self.relocate(exc, path, model) from exc

    def _limits(
        self, raw: Any, path: tuple[Any, ...], model: str | None
    ) -> BatchLimits:
        if isinstance(raw, BatchLimits):
            return raw
        if not isinstance(raw, Mapping):
            self.fail(
                path,
                f"'limits' must be a mapping, for example "
                f"{{max_rows_per_trigger: 50000}}; got {type(raw).__name__}",
                model=model,
            )
        for key in raw:
            if key not in LIMIT_KEYS:
                self.fail(
                    path + (key,),
                    unknown_key_message(str(key), LIMIT_KEYS, "the 'limits' block"),
                    model=model,
                )
        # Values are not *checked* here -- Model._check_limits owns that, and
        # owning it in one place is what keeps the two front doors identical --
        # but a substituted one is given the field's declared type first.
        return BatchLimits(
            **{
                str(key): self.coerce(
                    value,
                    _LIMIT_ANNOTATIONS.get(str(key)),
                    path=path + (key,),
                    field_name=str(key),
                    model=model,
                )
                for key, value in raw.items()
            }
        )

    def _scalar(
        self, key: str, value: Any, path: tuple[Any, ...], model: str | None
    ) -> Any:
        """Shape-check a plain field. Meaning is ``Model.validate``'s business."""
        if key == "name":
            if not isinstance(value, str):
                self.fail(
                    path,
                    f"'name' must be a string, got {type(value).__name__}: "
                    f"{value!r}",
                    model=model,
                )
            return str(value)

        if key == "aggregates":
            if not isinstance(value, Mapping):
                self.fail(
                    path,
                    f"'aggregates' must be a mapping of output column to SQL "
                    f"expression, for example {{n: 'count(*)'}}; got "
                    f"{type(value).__name__}",
                    model=model,
                )
            resolved: dict[str, str] = {}
            for column, expression in value.items():
                if not isinstance(column, str) or not isinstance(expression, str):
                    self.fail(
                        path + (column,),
                        f"aggregate {column!r} must map a column name to a SQL "
                        f"expression string; got {expression!r}",
                        model=model,
                    )
                resolved[str(column)] = str(expression)
            return resolved

        if key in ("key", "udfs"):
            example = (
                "[window_ts, sensor_id]"
                if key == "key"
                else "['my_pkg.signal:arrow_fft']"
            )
            if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
                self.fail(
                    path,
                    f"{key!r} must be a list, for example {example}; got "
                    f"{type(value).__name__}: {value!r}",
                    model=model,
                )
            for position, item in enumerate(value):
                if not isinstance(item, str):
                    self.fail(
                        path + (position,),
                        f"{key}[{position}] must be a string, got "
                        f"{type(item).__name__}: {item!r}",
                        model=model,
                    )
            return [str(item) for item in value]

        if key in (
            "time_column",
            "grain",
            "strategy",
            "memory_profile",
            "on_failure",
        ):
            if value is not None and not isinstance(value, str):
                self.fail(
                    path,
                    f"{key!r} must be a string or absent, got "
                    f"{type(value).__name__}: {value!r}",
                    model=model,
                )
            return None if value is None else str(value)

        # A field added to Model that needs no structural conversion arrives
        # here. It still gets the coercion every other field gets, taken from
        # its dataclass annotation, so a future `int` field on Model works from
        # config without touching this method. If it needs more than that, the
        # round-trip test in tests/unit/test_config.py is what will say so.
        return self.coerce(
            value,
            _MODEL_ANNOTATIONS.get(key),
            path=path,
            field_name=key,
            model=model,
        )


# --------------------------------------------------------------------------
# YAML node marks
# --------------------------------------------------------------------------


def _yaml_message(exc: yaml.YAMLError) -> str:
    problem = getattr(exc, "problem", None)
    context = getattr(exc, "context", None)
    if problem:
        detail = f"{context}, {problem}" if context else problem
        return f"could not parse YAML: {detail}"
    return f"could not parse YAML: {exc}"


def _yaml_location(exc: yaml.YAMLError, source: str | None) -> str | None:
    mark = getattr(exc, "problem_mark", None) or getattr(exc, "context_mark", None)
    if mark is None:
        return source
    if source:
        return f"{source}:{mark.line + 1}"
    return f"line {mark.line + 1}"


def _line_index(text: str, source: str | None) -> dict[tuple[Any, ...], int]:
    """Map path tuples to 1-based line numbers, using the YAML node marks.

    ``yaml.compose`` builds the node tree without constructing any Python
    objects, so it is safe in the same way ``yaml.safe_load`` is, and it is the
    only place the line numbers exist. Duplicate mapping keys are detected on
    the way past: ``safe_load`` keeps the last silently, and two ``path:`` keys
    in one source block is a mistake worth failing on.
    """
    try:
        root = yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError:  # pragma: no cover - safe_load already reported it
        return {}
    if root is None:
        return {}

    index: dict[tuple[Any, ...], int] = {}
    duplicates: list[tuple[str, int, int]] = []
    _walk_node(root, (), index, duplicates)

    if duplicates:
        key, first, second = duplicates[0]
        where = f"{source}:{second}" if source else f"line {second}"
        raise ConfigError(
            f"the key {key!r} appears twice in the same block (first at line "
            f"{first}). YAML keeps the last one silently, which makes the "
            f"earlier declaration look effective when it is not.",
            path=where,
        )
    return index


def _walk_node(
    node: Any,
    path: tuple[Any, ...],
    index: dict[tuple[Any, ...], int],
    duplicates: list[tuple[str, int, int]],
) -> None:
    index.setdefault(path, node.start_mark.line + 1)
    if isinstance(node, yaml.MappingNode):
        seen: dict[Any, int] = {}
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.ScalarNode):
                continue
            key = key_node.value
            line = key_node.start_mark.line + 1
            if key in seen:
                duplicates.append((str(key), seen[key], line))
            else:
                seen[key] = line
            child = path + (key,)
            index.setdefault(child, line)
            if not path:
                # A document-level unknown key has no container path to hang
                # off, so it is reported against this synthetic one.
                index.setdefault(("__key__", key), line)
            _walk_node(value_node, child, index, duplicates)
    elif isinstance(node, yaml.SequenceNode):
        for position, item in enumerate(node.value):
            _walk_node(item, path + (position,), index, duplicates)
