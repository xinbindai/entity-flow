"""
Tests for entitymodel.log_search -- pulling one handler's run out of a log
file.

The lines outbox writes name handler_name, event_id and event_type, so
matching them is bookkeeping. The interesting half is the lines the handler
itself wrote: they carry none of the three, and the only thing that ties them
to a run is sitting between its brackets. These tests are mostly about that
attribution being right, and about it saying so when it cannot be.

    python test/test_log_search.py
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entitymodel.log_search import search_logs  # noqa: E402

E1 = "52c97fc4-bbc6-4ff8-bb15-9173426b756a"
E2 = "d31c8496-521a-46b6-a11b-9ee6cfefffa2"


def _at(t: str, ms: str, who: str | None) -> str:
    """A line prefix, with the [pid] or [pid:tid] field when one is asked for."""
    return f"2026-08-23 {t},{ms}" + (f" [{who}]" if who else "")


def enter(handler: str, event: str, t: str = "20:00:00", who: str | None = None) -> str:
    return (f"{_at(t, '000', who)} DEBUG entitymodel.outbox: {handler}: dispatching "
            f"event {event} (AnalysisTaskSucceeded) -- entering handler")


def leave(handler: str, event: str, t: str = "20:00:01", who: str | None = None) -> str:
    return (f"{_at(t, '000', who)} DEBUG entitymodel.outbox: {handler}: left handler "
            f"for event {event} (AnalysisTaskSucceeded) after 12.3 ms")


def done(handler: str, event: str, t: str = "20:00:02", who: str | None = None) -> str:
    return (f"{_at(t, '000', who)} DEBUG entitymodel.outbox: {handler}: event {event} "
            f"(AnalysisTaskSucceeded) handled and checkpointed")


def app(message: str, t: str = "20:00:00", who: str | None = None) -> str:
    return f"{_at(t, '500', who)} INFO  myapp.handlers: {message}"


def write(tmpdir: Path, name: str, *lines: str) -> Path:
    path = tmpdir / name
    path.write_text("\n".join(lines) + "\n")
    return path


def texts(lines) -> list[str]:
    return [line.text for line in lines]


def test_it_returns_the_outbox_lines_for_one_handler(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1), leave("create-result", E1),
                done("create-result", E1))

    found = search_logs(log, handler_name="create-result")

    assert len(found) == 3, texts(found)
    assert all(line.handler_name == "create-result" for line in found)
    assert all(line.event_id == E1 for line in found)


def test_it_includes_the_lines_the_handler_wrote(tmpdir: Path) -> None:
    """The point of the module: those lines name neither handler nor event."""
    log = write(tmpdir, "a.log",
                enter("create-result", E1),
                app("looked up sample SEQ-001"),
                app("wrote result row"),
                leave("create-result", E1))

    found = texts(search_logs(log, handler_name="create-result"))

    assert any("looked up sample SEQ-001" in t for t in found), found
    assert any("wrote result row" in t for t in found), found


def test_lines_outside_a_run_are_left_out(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                "2026-08-23 19:59:00,000 INFO  myapp: starting up",
                enter("create-result", E1),
                app("inside"),
                leave("create-result", E1),
                "2026-08-23 20:00:09,000 INFO  myapp: shutting down")

    found = texts(search_logs(log, handler_name="create-result"))

    assert any("inside" in t for t in found), found
    assert not any("starting up" in t for t in found), "before any run"
    assert not any("shutting down" in t for t in found), "after every run"


def test_two_handlers_on_one_event_stay_apart(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00"),
                app("created the result", "20:00:00"),
                leave("create-result", E1, "20:00:01"),
                enter("archive", E1, "20:00:02"),
                app("started archiving", "20:00:02"),
                leave("archive", E1, "20:00:03"))

    created = texts(search_logs(log, handler_name="create-result"))
    archived = texts(search_logs(log, handler_name="archive"))

    assert any("created the result" in t for t in created), created
    assert not any("started archiving" in t for t in created), \
        "the second handler's own line must not leak into the first's run"
    assert any("started archiving" in t for t in archived), archived
    assert not any("created the result" in t for t in archived), archived


def test_filtering_by_event_spans_every_handler_that_saw_it(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00"),
                leave("create-result", E1, "20:00:01"),
                enter("archive", E1, "20:00:02"),
                leave("archive", E1, "20:00:03"),
                enter("create-result", E2, "20:00:04"),
                leave("create-result", E2, "20:00:05"))

    found = search_logs(log, event_id=E1)

    assert {line.handler_name for line in found} == {"create-result", "archive"}
    assert all(line.event_id == E1 for line in found), texts(found)


def test_both_filters_narrow_to_one_run(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00"),
                leave("create-result", E1, "20:00:01"),
                enter("archive", E1, "20:00:02"),
                leave("archive", E1, "20:00:03"))

    found = search_logs(log, handler_name="archive", event_id=E1)

    assert len(found) == 2, texts(found)
    assert all(line.handler_name == "archive" for line in found)


def test_interleaved_runs_are_marked_ambiguous(tmpdir: Path) -> None:
    """Two workers on one file: position stops being proof, and it says so."""
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00"),
                enter("archive", E2, "20:00:01"),
                app("whose line is this?", "20:00:02"),
                leave("archive", E2, "20:00:03"),
                leave("create-result", E1, "20:00:04"))

    found = search_logs(log, handler_name="create-result")
    unknown = [line for line in found if "whose line is this?" in line.text]

    assert unknown, texts(found)
    assert unknown[0].ambiguous, "must not claim an interleaved line as certain"


def test_an_unambiguous_line_is_not_marked(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1), app("certain"), leave("create-result", E1))

    found = search_logs(log, handler_name="create-result")

    assert not any(line.ambiguous for line in found), texts(found)


def test_a_traceback_stays_with_the_line_that_raised(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("archive", E1),
                "2026-08-23 20:00:00,500 ERROR myapp.handlers: it broke",
                "Traceback (most recent call last):",
                '  File "handlers.py", line 12, in archive',
                "RuntimeError: cold storage unreachable",
                leave("archive", E1))

    found = search_logs(log, handler_name="archive")
    broke = [line for line in found if "it broke" in line.text]

    assert broke, texts(found)
    assert "RuntimeError: cold storage unreachable" in broke[0].text, \
        "the frames must ride with the record, not be stranded outside the run"


def test_no_handler_lines_returns_only_what_outbox_wrote(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1), app("chatty"), leave("create-result", E1))

    found = search_logs(log, handler_name="create-result", include_handler_lines=False)

    assert not any("chatty" in t for t in texts(found)), texts(found)
    assert len(found) == 2, texts(found)


def test_it_merges_files_in_time_order(tmpdir: Path) -> None:
    a = write(tmpdir, "a.log",
              enter("create-result", E1, "20:00:00"), leave("create-result", E1, "20:00:03"))
    b = write(tmpdir, "b.log",
              enter("archive", E2, "20:00:01"), leave("archive", E2, "20:00:02"))

    found = search_logs(a, b)
    stamps = [line.timestamp for line in found]

    assert stamps == sorted(stamps), texts(found)
    assert "archive" in found[1].text, "the second file's run interleaves by time"


def test_a_run_does_not_span_two_files(tmpdir: Path) -> None:
    """An unterminated run at the end of one file must not swallow the next."""
    a = write(tmpdir, "a.log", enter("create-result", E1, "20:00:00"))
    b = write(tmpdir, "b.log", app("unrelated", "20:00:01"))

    found = texts(search_logs(a, b, handler_name="create-result"))

    assert not any("unrelated" in t for t in found), found


def test_the_event_id_is_matched_case_insensitively(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log", enter("create-result", E1), leave("create-result", E1))

    assert search_logs(log, event_id=E1.upper()), "a pasted uppercase uuid must still match"


def test_a_missing_file_is_reported(tmpdir: Path) -> None:
    try:
        search_logs(tmpdir / "nope.log")
    except FileNotFoundError as exc:
        assert "nope.log" in str(exc), exc
    else:
        raise AssertionError("a missing log file must say so, not return nothing")


def test_no_paths_is_an_error() -> None:
    try:
        search_logs()
    except ValueError as exc:
        assert "no log files" in str(exc), exc
    else:
        raise AssertionError("expected ValueError")


# --------------------------------------------------------------------------
# %(process)d in the format. Two workers on one file stop competing for the
# same brackets, because each line says which process wrote it.
# --------------------------------------------------------------------------
def test_a_process_id_resolves_interleaved_runs(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00", who="101"),
                enter("archive", E2, "20:00:01", who="202"),
                app("belongs to create-result", "20:00:02", who="101"),
                app("belongs to archive", "20:00:03", who="202"),
                leave("archive", E2, "20:00:04", who="202"),
                leave("create-result", E1, "20:00:05", who="101"))

    created = texts(search_logs(log, handler_name="create-result"))
    archived = texts(search_logs(log, handler_name="archive"))

    assert any("belongs to create-result" in t for t in created), created
    assert not any("belongs to archive" in t for t in created), \
        "a line written by another process must not be claimed"
    assert any("belongs to archive" in t for t in archived), archived
    assert not any("belongs to create-result" in t for t in archived), archived


def test_a_process_id_removes_the_ambiguity(tmpdir: Path) -> None:
    """The same interleaving that is a guess without the pid is certain with it."""
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00", who="101"),
                enter("archive", E2, "20:00:01", who="202"),
                app("mine", "20:00:02", who="101"),
                leave("archive", E2, "20:00:03", who="202"),
                leave("create-result", E1, "20:00:04", who="101"))

    found = search_logs(log, handler_name="create-result")

    assert not any(line.ambiguous for line in found), \
        "with a process id, two open runs are no longer competing"


def test_the_process_and_thread_are_recorded(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00", who="4242:7"),
                leave("create-result", E1, "20:00:01", who="4242:7"))

    line = search_logs(log)[0]

    assert line.process == 4242, line
    assert line.thread == 7, line


def test_threads_of_one_process_are_kept_apart(tmpdir: Path) -> None:
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00", who="99:1"),
                enter("archive", E2, "20:00:01", who="99:2"),
                app("thread one's line", "20:00:02", who="99:1"),
                leave("archive", E2, "20:00:03", who="99:2"),
                leave("create-result", E1, "20:00:04", who="99:1"))

    archived = texts(search_logs(log, handler_name="archive"))

    assert not any("thread one's line" in t for t in archived), archived


def test_a_bracketed_number_in_a_message_is_not_a_process_id(tmpdir: Path) -> None:
    """Only the field straight after the timestamp counts, or messages lie."""
    log = write(tmpdir, "a.log",
                enter("create-result", E1, "20:00:00"),
                "2026-08-23 20:00:01,500 INFO  myapp.handlers: retrying batch [12345]",
                leave("create-result", E1, "20:00:02"))

    found = search_logs(log, handler_name="create-result")
    batch = [line for line in found if "retrying batch" in line.text]

    assert batch, texts(found)
    assert batch[0].process is None, "a number inside a message is not the writer"


def test_a_format_without_a_process_id_still_works(tmpdir: Path) -> None:
    """The field is optional -- omitting it costs certainty, not function."""
    log = write(tmpdir, "a.log",
                enter("create-result", E1), app("still found"), leave("create-result", E1))

    found = search_logs(log, handler_name="create-result")

    assert any("still found" in t for t in texts(found)), texts(found)
    assert all(line.process is None for line in found)



TESTS = [
    test_it_returns_the_outbox_lines_for_one_handler,
    test_it_includes_the_lines_the_handler_wrote,
    test_lines_outside_a_run_are_left_out,
    test_two_handlers_on_one_event_stay_apart,
    test_filtering_by_event_spans_every_handler_that_saw_it,
    test_both_filters_narrow_to_one_run,
    test_interleaved_runs_are_marked_ambiguous,
    test_an_unambiguous_line_is_not_marked,
    test_a_traceback_stays_with_the_line_that_raised,
    test_no_handler_lines_returns_only_what_outbox_wrote,
    test_it_merges_files_in_time_order,
    test_a_run_does_not_span_two_files,
    test_the_event_id_is_matched_case_insensitively,
    test_a_missing_file_is_reported,
    test_no_paths_is_an_error,
    test_a_process_id_resolves_interleaved_runs,
    test_a_process_id_removes_the_ambiguity,
    test_the_process_and_thread_are_recorded,
    test_threads_of_one_process_are_kept_apart,
    test_a_bracketed_number_in_a_message_is_not_a_process_id,
    test_a_format_without_a_process_id_still_works,
]


def main() -> int:
    import inspect
    import tempfile

    passed, failed = 0, []
    for test in TESTS:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                if inspect.signature(test).parameters:
                    test(Path(tmp))
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
