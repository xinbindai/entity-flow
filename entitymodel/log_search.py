"""
Pull one handler's run out of a log file, the lines the handler itself wrote
included.

    from entitymodel.log_search import search_logs

    for line in search_logs("app.log", handler_name="create-result"):
        print(line.text)

Every line entitymodel.outbox writes names handler_name, event_id and
event_type, so filtering those is a matter of matching them. The lines the
handler itself writes are the problem this module exists for: they go to the
application's own logger, and carry none of the three. Nothing in the text
ties them to the run that produced them.

What ties them is position. The handler call is bracketed --

    create-result: dispatching event 46a6a6e6-... (AnalysisTaskSucceeded) -- entering handler
    myapp.handlers: looked up the sample                     <- no handler name, no event id
    myapp.handlers: wrote the result row                     <- likewise
    create-result: left handler for event 46a6a6e6-... (AnalysisTaskSucceeded) after 20.1 ms

-- so anything between the two ends came out of that handler, and this reads
the brackets to claim it. That is the whole trick, and it is also the limit:
position is only proof of authorship when one run is open at a time.

Several workers writing to one file interleave, and then the lines between one
run's brackets include another run's output. Put the process id in the format
and they stop competing:

    logging.basicConfig(
        format="%(asctime)s [%(process)d] %(levelname)-5s %(name)s: %(message)s",
    )

Each line then says who wrote it, brackets are matched per process, and a line
is attributed to the run that was open *in its own process*. Use
[%(process)d:%(thread)d] if one process dispatches in several threads.

Without it every line looks alike and attribution falls back to "whatever was
open", which is right whenever one run is open at a time and a guess otherwise.
It does not guess silently: a line picked up while two runs were open comes
back with ambiguous=True, and the CLI marks it `?`.

    python -m entitymodel.log_search app.log --handler create-result
    python -m entitymodel.log_search *.log --event 46a6a6e6-945c-473b-a106-d1613df1b086
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

__all__ = ["Line", "search_logs"]


_UUID = r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}"

# Leading timestamp, in the shapes logging produces by default: asctime's
# "2026-08-23 19:05:01,123" and the ISO spelling with T and a dot. Anything
# else parses as no timestamp, which costs ordering across files but nothing
# else -- see _sort_key.
_TS = r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?"
_TIMESTAMP = re.compile(rf"^(?P<ts>{_TS})")

# The writer of a line, when the format says. With
#
#     "%(asctime)s [%(process)d] %(levelname)-5s %(name)s: %(message)s"
#
# runs in different processes stop competing for the same brackets, and a line
# between them is attributed to the process that wrote it rather than to
# whichever run happened to be open. [pid:tid] narrows it further, for a
# handler that runs several dispatches in threads of one process.
#
# Read only from directly after the timestamp, so a bracketed number anywhere
# else in a message cannot be mistaken for one.
_ORIGIN = re.compile(rf"^{_TS}\s+\[(?P<process>\d+)(?::(?P<thread>\d+))?\]")

# The three shapes an outbox line takes. Anchored on the message rather than
# on the whole line, because the prefix in front of it is the application's
# format string and this module has no business assuming one.
_ENTER = re.compile(
    rf"(?P<handler>[\w.\-]+): dispatching event (?P<event>{_UUID}) "
    rf"\((?P<type>[^)]*)\) -- entering handler"
)
_LEAVE = re.compile(
    rf"(?P<handler>[\w.\-]+): left handler for event (?P<event>{_UUID}) "
    rf"\((?P<type>[^)]*)\) after"
)
# Everything else outbox says about a specific event: handled and
# checkpointed, raised, cancelled, skipping.
_OTHER = re.compile(
    rf"(?P<handler>[\w.\-]+): (?:event|skipping event) (?P<event>{_UUID})"
)


@dataclass(frozen=True)
class Line:
    """One line of a log file, with whatever this module could work out about it."""

    path: str
    lineno: int          # 1-based, as an editor counts
    text: str
    timestamp: datetime | None
    process: int | None  # from %(process)d, when the format carries it
    thread: int | None   # from %(thread)d, likewise
    handler_name: str | None    # None on a line the handler itself wrote
    event_id: str | None
    event_type: str | None
    role: str            # "enter" | "leave" | "outbox" | "handler"
    ambiguous: bool      # attributed by position while >1 run was open here

    def __str__(self) -> str:
        return self.text


def _parse_timestamp(text: str) -> datetime | None:
    m = _TIMESTAMP.match(text)
    if not m:
        return None
    raw = m.group("ts").replace(",", ".").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_origin(text: str) -> tuple[int | None, int | None]:
    m = _ORIGIN.match(text)
    if not m:
        return None, None
    thread = m.group("thread")
    return int(m.group("process")), int(thread) if thread else None


def _classify(text: str) -> tuple[str, str | None, str | None, str | None]:
    """(role, handler_name, event_id, event_type) for one line."""
    for role, pattern in (("enter", _ENTER), ("leave", _LEAVE), ("outbox", _OTHER)):
        m = pattern.search(text)
        if m:
            groups = m.groupdict()
            return role, groups["handler"], groups["event"], groups.get("type")
    return "handler", None, None, None


def _read(path: Path) -> list[tuple[int, str, datetime | None, tuple[int | None, int | None]]]:
    """
    Lines of one file as (lineno, text, timestamp, origin), with continuation
    lines folded into the record above them.

    A traceback is one logging call and many lines; splitting it would strand
    the frames outside the run that raised them, which is exactly the case
    someone is reading the log to understand.
    """
    records: list[tuple[int, str, datetime | None, tuple[int | None, int | None]]] = []
    with path.open(encoding="utf-8", errors="replace") as fh:
        for lineno, raw in enumerate(fh, start=1):
            text = raw.rstrip("\n")
            ts = _parse_timestamp(text)
            if ts is None and records and records[-1][2] is not None:
                prev_no, prev_text, prev_ts, prev_origin = records[-1]
                records[-1] = (prev_no, prev_text + "\n" + text, prev_ts, prev_origin)
                continue
            records.append((lineno, text, ts, _parse_origin(text)))
    return records


def search_logs(
    *paths: str | Path,
    handler_name: str | None = None,
    event_id: str | None = None,
    include_handler_lines: bool = True,
) -> list[Line]:
    """
    Lines from `paths` for one handler, one event, or both, in time order.

    handler_name and event_id each narrow the result and both are optional;
    passing neither returns every line that belongs to some run, which is the
    way to see what a file contains before filtering it.

    include_handler_lines=False restricts the result to the lines outbox
    itself wrote -- the shape of the run without its contents, which is what
    you want when the handler is chatty and the question is only how long it
    took or how it ended.

    Ordering is by timestamp where the format has one. A line without its own
    timestamp -- a continuation, or a format that omits asctime -- sorts with
    the last line that had one, so a traceback stays under the line it belongs
    to instead of migrating to the top of the output.
    """
    if not paths:
        raise ValueError("no log files given")

    wanted_event = event_id.lower() if event_id else None

    parsed: list[Line] = []
    # (handler, event) -> True for the runs open at this point in the file.
    # Reset per file: a run cannot span two files, and an unterminated one at
    # the end of a file must not swallow the start of the next.
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"no such log file: {path}")

        # Keyed by origin, so runs in different processes do not compete for
        # the same brackets. Without %(process)d in the format every line has
        # origin (None, None) and they all share one bucket, which is the
        # older, guessier behaviour -- correct, just less able to be sure.
        open_runs: dict[tuple[int | None, int | None], dict[tuple[str, str], None]] = {}
        last_ts: datetime | None = None
        pending: list[tuple[Line, list[tuple[str, str]]]] = []

        for lineno, text, ts, origin in _read(path):
            if ts is not None:
                last_ts = ts
            role, handler, event, event_type = _classify(text)
            process, thread = origin

            if role == "enter":
                open_runs.setdefault(origin, {})[(handler, event.lower())] = None
            elif role == "leave":
                open_runs.get(origin, {}).pop((handler, event.lower()), None)

            if role == "handler":
                # Nothing in the text identifies it, so it belongs to whatever
                # was open in the same process. One open run is proof; more
                # than one is a guess, and says so.
                owners = list(open_runs.get(origin, {}))
                ambiguous = len(owners) > 1
                line = Line(
                    path=str(path), lineno=lineno, text=text,
                    timestamp=ts or last_ts, process=process, thread=thread,
                    handler_name=None, event_id=None, event_type=None,
                    role=role, ambiguous=ambiguous,
                )
                pending.append((line, owners))
            else:
                line = Line(
                    path=str(path), lineno=lineno, text=text,
                    timestamp=ts or last_ts, process=process, thread=thread,
                    handler_name=handler, event_id=event, event_type=event_type,
                    role=role, ambiguous=False,
                )
                pending.append((line, [(handler, event.lower())]))

        for line, owners in pending:
            if not owners:
                continue  # outside every run: startup, polling, shutdown
            if handler_name is not None and not any(h == handler_name for h, _ in owners):
                continue
            if wanted_event is not None and not any(e == wanted_event for _, e in owners):
                continue
            if not include_handler_lines and line.role == "handler":
                continue
            parsed.append(line)

    parsed.sort(key=_sort_key)
    return parsed


def _sort_key(line: Line) -> tuple:
    # Lines with no timestamp anywhere in the file fall back to file order,
    # which is the only ordering there is for them.
    return (line.timestamp or datetime.min, line.path, line.lineno)


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m entitymodel.log_search",
        description="Filter a log down to one handler's run, its own lines included.",
    )
    parser.add_argument("paths", nargs="+", help="log files to read")
    parser.add_argument("--handler", default=None, help="handler_name to filter on")
    parser.add_argument("--event", default=None, help="event_id to filter on")
    parser.add_argument(
        "--no-handler-lines", action="store_true",
        help="only the lines entitymodel.outbox wrote",
    )
    parser.add_argument(
        "--show-source", action="store_true", help="prefix each line with file:lineno",
    )
    args = parser.parse_args(argv)

    try:
        lines = search_logs(
            *args.paths,
            handler_name=args.handler,
            event_id=args.event,
            include_handler_lines=not args.no_handler_lines,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    ambiguous = 0
    for line in lines:
        prefix = "? " if line.ambiguous else "  "
        ambiguous += line.ambiguous
        where = f"{line.path}:{line.lineno}: " if args.show_source else ""
        print(f"{prefix}{where}{line.text}")

    if ambiguous:
        print(
            f"\n{ambiguous} line(s) marked ? were attributed by position while more "
            f"than one run was open, so they may belong to another handler. Add "
            f"[%(process)d] to the log format -- runs are then matched per process "
            f"and the doubt goes away.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
