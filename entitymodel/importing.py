"""
Resolve a dotted path to the object it names.

Wherever this system is driven by configuration rather than code -- handlers
listed in a settings file, a Celery app named on a command line -- something
has to turn "package.module:attr" into the object. That is all this does.

    from entitymodel.importing import import_attr

    handler = import_attr("myapp.handlers:create_sample_result")
    handler = import_attr("myapp.handlers.create_sample_result")   # also fine

Both spellings work because both are in common use: the colon form is
unambiguous about where the module ends, the dotted form is what people
usually type. The colon form is worth preferring in configuration for exactly
that reason -- "a.b.c" cannot say whether c is an attribute of a.b or a
submodule.

Raises ValueError for a malformed path, ImportError if the module will not
import, and AttributeError if the module has no such name -- ordinary
exceptions, not SystemExit, because this is called from library code as well
as from command lines. Callers that are command lines convert.
"""

from __future__ import annotations

import importlib
from typing import Any

__all__ = ["import_attr"]


def import_attr(dotted: str) -> Any:
    """Import and return the object named by "module:attr" or "module.attr"."""
    if not isinstance(dotted, str) or not dotted.strip():
        raise ValueError(f"expected a dotted path, got {dotted!r}")

    module_name, _, attr = dotted.partition(":")
    if not attr:
        module_name, _, attr = dotted.rpartition(".")
    if not module_name or not attr:
        raise ValueError(f"expected module:attr or module.attr, got {dotted!r}")

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(f"cannot import {module_name!r}: {exc}") from exc

    # Deliberately outside the try above. An AttributeError raised by the
    # module's own code while importing must not be reported as "no such
    # attribute" -- that sends whoever reads the message to the wrong file.
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise AttributeError(f"{module_name!r} has no attribute {attr!r}") from exc
