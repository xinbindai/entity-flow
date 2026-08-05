"""
Tests for entitymodel.importing -- resolving a dotted path to the object it
names.

Small, but it is the seam every configuration-driven entry point goes
through: handlers listed in a settings file, the Celery app named on a command
line, the worker config dict. A wrong error message here sends whoever reads
it to the wrong file.

    python test/test_importing.py
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entitymodel.importing import import_attr  # noqa: E402


def expect(exc_type, fn, fragment: str, what: str):
    try:
        fn()
    except exc_type as exc:
        assert fragment in str(exc), f"{what}: expected {fragment!r} in {exc}"
    except Exception as exc:  # noqa: BLE001 - the type itself is the assertion
        raise AssertionError(f"{what}: expected {exc_type.__name__}, got {type(exc).__name__}: {exc}")
    else:
        raise AssertionError(f"{what}: expected {exc_type.__name__}")


def test_resolves_the_colon_form() -> None:
    import handlers_fixture

    assert import_attr("handlers_fixture:record") is handlers_fixture.record


def test_resolves_the_dotted_form() -> None:
    import handlers_fixture

    assert import_attr("handlers_fixture.record") is handlers_fixture.record


def test_resolves_a_nested_module() -> None:
    from entitymodel.outbox import fire_event

    assert import_attr("entitymodel.outbox:fire_event") is fire_event
    assert import_attr("entitymodel.outbox.fire_event") is fire_event


def test_resolves_non_callables_too() -> None:
    """Resolution and the callable check are separate concerns -- the Celery
    app and the worker config dict are not callables."""
    assert import_attr("handlers_fixture:NOT_A_FUNCTION") == "just a string"


def test_a_missing_module_raises_import_error() -> None:
    expect(ImportError, lambda: import_attr("no.such.module:thing"),
           "cannot import", "missing module")


def test_a_missing_attribute_raises_attribute_error() -> None:
    expect(AttributeError, lambda: import_attr("handlers_fixture:nope"),
           "has no attribute", "missing attribute")


def test_a_malformed_path_raises_value_error() -> None:
    for spec in ("nocolonnodot", ":attr", "module:", ""):
        expect(ValueError, lambda s=spec: import_attr(s),
               "expected", f"malformed path {spec!r}")


def test_a_non_string_raises_value_error() -> None:
    expect(ValueError, lambda: import_attr(None), "expected a dotted path", "None")
    expect(ValueError, lambda: import_attr(123), "expected a dotted path", "int")


def test_an_attribute_error_raised_during_import_is_not_disguised(tmp_dir: Path) -> None:
    """
    The bug that came from having two copies of this: wrapping getattr in the
    same try as import_module reported a module's own AttributeError as "no
    such attribute", pointing the reader at the wrong file entirely.
    """
    module = tmp_dir / "explodes_on_import.py"
    module.write_text("None.this_attribute_does_not_exist\n")
    sys.path.insert(0, str(tmp_dir))
    try:
        try:
            import_attr("explodes_on_import:anything")
        except ImportError as exc:
            raise AssertionError(f"the module's own error was disguised as an import failure: {exc}")
        except AttributeError as exc:
            assert "has no attribute 'anything'" not in str(exc), (
                "the module's own AttributeError was reported as a missing attribute; "
                f"got: {exc}"
            )
            assert "this_attribute_does_not_exist" in str(exc), exc
    finally:
        sys.path.remove(str(tmp_dir))
        sys.modules.pop("explodes_on_import", None)


# --------------------------------------------------------------------------
TESTS = [
    test_resolves_the_colon_form,
    test_resolves_the_dotted_form,
    test_resolves_a_nested_module,
    test_resolves_non_callables_too,
    test_a_missing_module_raises_import_error,
    test_a_missing_attribute_raises_attribute_error,
    test_a_malformed_path_raises_value_error,
    test_a_non_string_raises_value_error,
    test_an_attribute_error_raised_during_import_is_not_disguised,
]


def main() -> int:
    import inspect
    import tempfile

    passed, failed = 0, []
    for test in TESTS:
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                if inspect.signature(test).parameters:
                    test(Path(tmpdir))
                else:
                    test()
                print(f"PASS  {test.__name__}")
                passed += 1
            except Exception:
                print(f"FAIL  {test.__name__}")
                traceback.print_exc()
                failed.append(test.__name__)

    print(f"\n{passed} passed, {len(failed)} failed")
    for name in failed:
        print(f"  failed: {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
