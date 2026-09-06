"""Exception hierarchy for duckstream.

Everything duckstream raises deliberately derives from :class:`DuckstreamError`,
so a host application can catch the whole framework with one ``except`` clause.

The messages built here are not incidental. Refusing an incorrect model at load
time is the framework's reason to exist (see ``docs/duckstream/PLAN.md``), and a
rejection is only useful if the operator can act on it. Every constructor below
therefore pushes towards a message that says *what was declared*, *what was
inferred from the declaration*, and *what to do about it*.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "DuckstreamError",
    "ModelValidationError",
    "ConfigError",
]


class DuckstreamError(Exception):
    """Base class for every error duckstream raises."""


class BatchFailed(DuckstreamError):
    """Raised at the end of a run in which some batch would not process.

    Deliberately raised *after* every model has had its turn rather than at the
    moment of failure, so one broken model cannot stop the others from running.
    It carries the :class:`~duckstream.engine.RunReport`, so a caller can still
    see everything that did succeed:

    .. code-block:: python

        try:
            report = engine.run()
        except BatchFailed as failure:
            report = failure.report          # the successful batches are here

    A *quarantined* batch does not raise. Quarantine is the outcome the model
    asked for, and it is recorded permanently in ``duckstream.quarantine``, so
    the run has done what it was told; ``RunReport.quarantined`` is how a caller
    notices, and ``duckstream run`` exits non-zero when it is non-empty.
    """

    def __init__(self, message: str, *, report: object = None) -> None:
        super().__init__(message)
        self.report = report


class ModelValidationError(DuckstreamError):
    """A model declaration is invalid and must not be executed.

    Raised only from validation and classification, never from execution. The
    ``field`` and ``model`` attributes are kept structured so a CLI can render
    them however it likes; ``str(err)`` stays human-readable on its own.
    """

    def __init__(
        self,
        message: str,
        *,
        model: str | None = None,
        field: str | None = None,
        remedy: str | None = None,
    ) -> None:
        self.model = model
        self.field = field
        self.remedy = remedy
        self.reason = message
        super().__init__(self._render(message))

    def _render(self, message: str) -> str:
        head = ""
        if self.model and self.field:
            head = f"model {self.model!r}, field {self.field!r}: "
        elif self.model:
            head = f"model {self.model!r}: "
        elif self.field:
            head = f"field {self.field!r}: "
        if not self.remedy:
            return f"{head}{message}"
        separator = "" if message.rstrip().endswith((".", "!", "?")) else "."
        return f"{head}{message}{separator} {self.remedy}"

    @classmethod
    def strategy_conflict(
        cls,
        *,
        model: str | None,
        declared: str,
        tier: Any,
        column: str,
        expression: str,
        aggregate: str,
        allowed: str,
    ) -> "ModelValidationError":
        """The headline rejection: a strategy that is wrong for the computed tier.

        This is the bug class recorded in ``CONTEXT.md`` section 4 — a mart that
        folded averages as ``(target.avg + source.avg) / 2`` and held 3.0 where
        the truth was 2.0. It is refused here, at load time.
        """
        tier_name = getattr(tier, "value", tier)
        return cls(
            f"strategy {declared!r} was declared, but column {column!r} computes "
            f"{expression!r}, which classifies as tier {tier_name!r} because of "
            f"{aggregate}. Folding a {tier_name!r} aggregate as if it were "
            f"additive produces silently wrong numbers, not an error at runtime.",
            model=model,
            field="strategy",
            remedy=f"Declare strategy={allowed!r}, or omit strategy and let "
            f"duckstream infer it from the tier.",
        )

    @classmethod
    def bad_expression(
        cls,
        *,
        model: str | None,
        column: str,
        expression: str,
        detail: str,
        remedy: str | None = None,
    ) -> "ModelValidationError":
        return cls(
            f"aggregate expression for column {column!r} is not usable: "
            f"{expression!r}: {detail}",
            model=model,
            field="aggregates",
            remedy=remedy,
        )


class ConfigError(DuckstreamError):
    """A configuration document could not be turned into models.

    Reserved for deserialisation problems — unreadable YAML, an unknown registry
    name, a missing required key. Once a :class:`~duckstream.model.Model` exists,
    problems with it are :class:`ModelValidationError` instead: the loader is a
    deserialiser only and has no validation of its own.
    """

    def __init__(self, message: str, *, path: str | None = None) -> None:
        self.path = path
        super().__init__(f"{path}: {message}" if path else message)
