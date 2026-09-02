"""
Tests for entitymodel.migrate -- the packaged migrations and their entry point.

These exist because of a specific defect: for seven releases the wheel shipped
the models without the migrations that build the tables they need, so a
deployment that installed the package could not create its schema. The failure
was invisible from a checkout, where the migrations are always on disk, which
is exactly why it survived so long. Most of what follows checks that the
packaged copy is complete and reachable without a repository around it.

    python test/test_migrate.py
"""

from __future__ import annotations

import subprocess
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entitymodel.migrate import main, make_config, migrations_path  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_the_migrations_live_inside_the_package() -> None:
    """Inside entitymodel/, or they do not reach the wheel."""
    path = migrations_path()

    assert path.is_dir(), path
    assert path.parent.name == "entitymodel", \
        f"migrations must sit inside the package to be installed, found {path}"


def test_alembic_needs_all_of_these_files() -> None:
    """env.py drives the run and script.py.mako is what `revision` renders."""
    path = migrations_path()

    for required in ("env.py", "script.py.mako"):
        assert (path / required).exists(), f"{required} missing from {path}"
    assert (path / "versions").is_dir(), "versions/ missing"


def test_no_revision_is_left_outside_the_package() -> None:
    """A revision added at the old repo-root path would not ship."""
    stray = REPO_ROOT / "migrations"

    assert not stray.exists(), \
        f"{stray} is back; revisions there are invisible to an installed copy"


def test_every_revision_is_present() -> None:
    versions = migrations_path() / "versions"
    revisions = sorted(p.name for p in versions.glob("*.py"))

    assert len(revisions) >= 4, revisions
    for expected in ("0a9fc379d832", "5a072b1fb587", "27f88ce75b7b", "9c4e1a7b2d05"):
        assert any(expected in name for name in revisions), \
            f"{expected} missing from the packaged migrations: {revisions}"


def test_the_chain_has_exactly_one_head() -> None:
    """Two heads means `upgrade head` is ambiguous and fails at deploy time."""
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(make_config())
    heads = script.get_heads()

    assert len(heads) == 1, f"expected a single head, got {heads}"


def test_the_chain_is_unbroken_back_to_base() -> None:
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(make_config())
    walked = [rev.revision for rev in script.walk_revisions()]

    assert len(walked) == len(set(walked)), f"duplicate revision ids: {walked}"
    base = script.get_base()
    assert base in walked, f"base {base} not reachable from head"


def test_the_config_points_at_the_packaged_copy() -> None:
    config = make_config()

    assert config.get_main_option("script_location") == str(migrations_path())


def test_the_url_is_passed_as_an_attribute_not_a_main_option() -> None:
    """
    Main options go through ConfigParser interpolation, so a password
    containing '%' would raise or be silently mangled.
    """
    url = "postgresql+psycopg2://user:pa%%ss@host/db"
    config = make_config(url)

    assert config.attributes["db_url"] == url
    assert config.get_main_option("sqlalchemy.url", None) in (None, "")


def expect_exit(argv: list[str], fragment: str, what: str) -> None:
    try:
        main(argv)
    except SystemExit as exc:
        assert fragment in str(exc), f"{what}: expected {fragment!r} in {exc}"
    else:
        raise AssertionError(f"{what}: expected SystemExit")


def test_upgrade_without_a_revision_is_refused() -> None:
    expect_exit(["upgrade"], "needs a revision", "bare upgrade")


def test_downgrade_without_a_revision_is_refused() -> None:
    """Guessing a target here would destroy schema nobody asked it to."""
    expect_exit(["downgrade"], "needs a revision", "bare downgrade")


def test_the_module_is_runnable() -> None:
    """python -m entitymodel.migrate, which is what the docs tell people to run."""
    result = subprocess.run(
        [sys.executable, "-m", "entitymodel.migrate", "history"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert "9c4e1a7b2d05" in result.stdout, result.stdout


def test_a_console_script_is_declared() -> None:
    """entity-flow-migrate is what a deploy job invokes."""
    text = (REPO_ROOT / "pyproject.toml").read_text()

    assert "[project.scripts]" in text, "no console script declared"
    assert "entity-flow-migrate" in text, text


def test_alembic_is_an_extra_not_a_hard_dependency() -> None:
    text = (REPO_ROOT / "pyproject.toml").read_text()
    before_optional, _, after_optional = text.partition("[project.optional-dependencies]")

    assert "alembic" in after_optional, "alembic should be declared as an extra"
    assert "alembic" not in before_optional.split("dependencies = [")[1].split("]")[0], \
        "alembic must not be a hard runtime dependency"


TESTS = [
    test_the_migrations_live_inside_the_package,
    test_alembic_needs_all_of_these_files,
    test_no_revision_is_left_outside_the_package,
    test_every_revision_is_present,
    test_the_chain_has_exactly_one_head,
    test_the_chain_is_unbroken_back_to_base,
    test_the_config_points_at_the_packaged_copy,
    test_the_url_is_passed_as_an_attribute_not_a_main_option,
    test_upgrade_without_a_revision_is_refused,
    test_downgrade_without_a_revision_is_refused,
    test_the_module_is_runnable,
    test_a_console_script_is_declared,
    test_alembic_is_an_extra_not_a_hard_dependency,
]


def main_runner() -> int:
    passed, failed = 0, []
    for test in TESTS:
        try:
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
    sys.exit(main_runner())
