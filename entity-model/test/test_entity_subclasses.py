"""
Tests for the polymorphic Entity subclasses and the taxonomy that backs them.

The trap these guard against: a subcategory listed in entity_type but with no
rows in entity_status can never be inserted, because the validate_entity_status
trigger rejects every status for it. That is a data gap, not a schema error, so
nothing catches it until someone tries to create the entity.

    python test/test_entity_subclasses.py
"""

from __future__ import annotations

import sys
import traceback

from sqlalchemy import select
from sqlalchemy.orm import Session

from testdata import (  # noqa: E402
    drop_schema,
    fresh_session,
    make_engine,
)

from models import (  # noqa: E402
    Client,
    Entity,
    EntityStatus,
    EntityType,
    Patient,
)


def test_every_subcategory_has_at_least_one_status(session: Session) -> None:
    """The whole-taxonomy version of the trap above."""
    types = {(t.category, t.subcategory) for t in session.scalars(select(EntityType))}
    with_status = {(s.category, s.subcategory) for s in session.scalars(select(EntityStatus))}

    orphans = sorted(types - with_status)
    assert not orphans, f"subcategories with no valid status, so uninsertable: {orphans}"


def test_client_round_trips_as_client(session: Session) -> None:
    session.add(
        Client(
            subcategory="ordering_institution",
            name="Northside Oncology Associates",
            status="Active",
            attributes={
                "account_number": "ACCT-4471",
                "billing_contact_email": "ap@northside-onc.example",
            },
        )
    )
    session.commit()

    loaded = session.scalars(select(Entity).where(Entity.name == "Northside Oncology Associates")).one()
    assert isinstance(loaded, Client), f"loaded as {type(loaded).__name__}, not Client"
    assert loaded.category == "Client", loaded.category
    assert loaded.account_number == "ACCT-4471"
    assert loaded.billing_contact_email == "ap@northside-onc.example"


def test_client_accessors_return_none_when_absent(session: Session) -> None:
    session.add(
        Client(subcategory="ordering_institution", name="Bare Client",
               status="Onboarding", attributes={})
    )
    session.commit()

    client = session.scalars(select(Client)).one()
    assert client.account_number is None
    assert client.billing_contact_email is None


def test_selecting_client_excludes_other_categories(session: Session) -> None:
    """polymorphic_identity should filter the discriminator automatically."""
    session.add_all(
        [
            Client(subcategory="ordering_institution", name="Client A", status="Active", attributes={}),
            Patient(subcategory="patient", name="P-001", status="Active", attributes={"mrn": "MRN-1"}),
        ]
    )
    session.commit()

    clients = session.scalars(select(Client)).all()
    assert [c.name for c in clients] == ["Client A"], [c.name for c in clients]
    assert session.scalars(select(Entity)).all().__len__() == 2


def test_client_rejects_a_status_outside_its_state_machine(session: Session) -> None:
    """A Client can't borrow another subcategory's status."""
    session.add(
        Client(subcategory="ordering_institution", name="Wrong Status",
               status="Sequencing", attributes={})
    )
    try:
        session.commit()
    except Exception as exc:
        session.rollback()
        assert "invalid status" in str(exc), exc
    else:
        raise AssertionError("expected the status trigger to reject 'Sequencing' for a Client")


def test_patient_is_insertable(session: Session) -> None:
    """Regression: Patient was in entity_type with no entity_status rows, so
    every Patient insert failed the trigger."""
    session.add(
        Patient(subcategory="patient", name="P-002", status="Active", attributes={"mrn": "MRN-2"})
    )
    session.commit()

    patient = session.scalars(select(Patient)).one()
    assert isinstance(patient, Patient)
    assert patient.mrn == "MRN-2"


def test_terminal_statuses_are_flagged(session: Session) -> None:
    terminal = {
        (s.subcategory, s.status)
        for s in session.scalars(select(EntityStatus).where(EntityStatus.category == "Client"))
        if s.is_terminal
    }
    assert terminal == {("ordering_institution", "Offboarded")}, terminal


# --------------------------------------------------------------------------
TESTS = [
    test_every_subcategory_has_at_least_one_status,
    test_client_round_trips_as_client,
    test_client_accessors_return_none_when_absent,
    test_selecting_client_excludes_other_categories,
    test_client_rejects_a_status_outside_its_state_machine,
    test_patient_is_insertable,
    test_terminal_statuses_are_flagged,
]


def main(keep: bool = False) -> int:
    engine = make_engine()
    passed, failed = 0, []

    for test in TESTS:
        session = fresh_session(engine)
        try:
            test(session)
            print(f"PASS  {test.__name__}")
            passed += 1
        except Exception:
            print(f"FAIL  {test.__name__}")
            traceback.print_exc()
            failed.append(test.__name__)
        finally:
            session.close()

    if not keep:
        drop_schema(engine)
    engine.dispose()

    print(f"\n{passed} passed, {len(failed)} failed")
    for name in failed:
        print(f"  failed: {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(keep="--keep" in sys.argv))
