"""A module for the registry tests to resolve handlers out of by dotted path."""

SEEN: list = []
NOT_A_FUNCTION = "just a string"


def record(session, ev) -> None:
    SEEN.append(ev.payload["seq"])
