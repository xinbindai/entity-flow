"""
The reusable half of the entity-event-task platform: the schema (models) and
the transactional-outbox machinery built on it (outbox).

Deliberately domain-free -- nothing in here knows about sequencing labs. This
lab's taxonomy lives in taxonomy.py at the repo root, and its handlers in
demo.py, so another deployment can take this package unchanged.
"""
