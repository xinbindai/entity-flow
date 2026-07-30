-- Unified entity schema (PostgreSQL) -- one polymorphic `entity` table for
-- every entity type (Patient, Order, Sample, Batch, Result, Task), a
-- `entity_type` taxonomy table for category/subcategory, and a single
-- `entity_relationship` graph table replacing every individual foreign key
-- from the per-entity-table design that was considered and rejected (see
-- §2.4 of entity-event-task-architecture.md).
--
-- This is the project's one authoritative entity schema. The trade-off it
-- accepts, recorded here because it is a real cost and not a strict win:
--   + adding a new subcategory (a new Task type, a new Result type, a new
--     Sample stage) is a data change -- an INSERT into entity_type -- not
--     a schema migration.
--   + one table, one index set, for "all entities"; lineage traversal
--     (entity_relationship) covers the whole graph uniformly, including
--     what used to be plain FKs like sample.order_id, and a Task's target
--     entity / produced result (Task no longer needs its own
--     target_entity_type / target_entity_id columns -- those are just
--     relationship edges now).
--   - the database can no longer enforce "a Sample's fields look like X"
--     via column types the way dedicated tables did -- attribute shape
--     validation moves to the application layer (or a JSON Schema check
--     per subcategory). Status is the one thing still enforced in-database,
--     via the trigger below, since the state machine per subcategory
--     matters enough to protect.
--   - a first-class FK inside `attributes` JSONB (e.g. a Task's
--     triggering_event_id) is no longer enforced by the database; only
--     entity_relationship edges get real referential integrity.

-- 1. Taxonomy: valid (category, subcategory) pairs. Add a row to introduce
--    a new subcategory -- no ALTER TABLE needed.
CREATE TABLE entity_type (
    category     TEXT NOT NULL,   -- 'Patient' | 'Order' | 'Sample' | 'Batch' | 'Result' | 'Task'
    subcategory  TEXT NOT NULL,   -- e.g. category='Sample': 'raw_specimen' | 'library_sample'
    description  TEXT,
    PRIMARY KEY (category, subcategory)
);

-- The rows themselves are not listed here. The taxonomy is data, not schema,
-- and it lives in one place only: entity_types.csv and entity_statuses.csv at
-- the repo root. Load or reconcile them with
--
--     python taxonomy.py <url> --sync
--
-- which inserts what is new and updates what changed. Duplicating the rows
-- into this file would give a second copy to keep in step by hand.

-- 2. Valid statuses per subcategory -- documents, and (via the trigger
--    below) enforces, the state machine each subcategory owns, replacing
--    the per-table CHECK constraints from the earlier design.
CREATE TABLE entity_status (
    category     TEXT NOT NULL,
    subcategory  TEXT NOT NULL,
    status       TEXT NOT NULL,
    is_terminal  BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (category, subcategory, status),
    FOREIGN KEY (category, subcategory) REFERENCES entity_type (category, subcategory)
);

-- Rows come from entity_statuses.csv, as above. Every subcategory in
-- entity_type needs at least one row here: the trigger below rejects any
-- status without a matching (category, subcategory, status) row, so a
-- subcategory with no statuses can never be inserted at all. The CSV loader
-- refuses that combination rather than letting it reach the database.

-- Example `attributes` shapes -- documented convention, not enforced by
-- the database (validate at the application layer or with a JSON Schema
-- check per subcategory if you want it enforced):
--   Patient/patient:       { "mrn", "first_name", "last_name", "date_of_birth" }
--   Order/lab_order:       { "ordering_provider", "test_panel" }
--   Sample/raw_specimen:   { "specimen_type", "collection_date" }
--   Sample/library_sample: { "library_prep_protocol", "barcode", "concentration_ng_ul" }
--   Batch/illumina_run:    { "run_id", "instrument", "flow_cell_id", "qc_metrics" }
--   Result/sample_result:  { "pipeline_version", "reference_genome", "output_location" }
--   Task/*:                { "input_ref", "output_ref", "pipeline_version", "triggering_event_id",
--                             "retry_count", "started_at", "ended_at" }

-- 3. The unified entity table. Common attributes are real columns
--    (indexed, queryable without touching JSON); everything specific to
--    one category/subcategory goes in `attributes`.
CREATE TABLE entity (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category        TEXT NOT NULL,
    subcategory     TEXT NOT NULL,
    name            TEXT NOT NULL,      -- human-readable identifier, e.g. an accession number or run name;
                                          -- unique within its subcategory, not globally (see unique index below)
    status          TEXT NOT NULL,
    correlation_id  UUID,              -- present for 1:1-lineage entities (Order, raw_specimen, library_sample);
                                         -- null for fan-in entities (illumina_run) and joint results/tasks --
                                         -- their lineage lives in entity_relationship instead
    attributes      JSONB NOT NULL DEFAULT '{}',
    -- clock_timestamp(), not now(): now() is transaction start time, so a
    -- batch of entities created in one transaction would share a created_at,
    -- and a row updated twice in one transaction would show no change.
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (category, subcategory) REFERENCES entity_type (category, subcategory)
);

-- name only needs to be unique within one subcategory (e.g. two different
-- library_sample rows can't share a name, but a library_sample and an
-- illumina_run can coincidentally have the same name string with no
-- conflict). Scoped by (category, subcategory) rather than subcategory
-- alone, matching the FK to entity_type above.
CREATE UNIQUE INDEX idx_entity_name_per_subcategory ON entity (category, subcategory, name);

-- A CHECK constraint can't reference another table, so validating status
-- against entity_status (the polymorphic equivalent of the per-table CHECK
-- constraints used before) needs a trigger. Also keeps updated_at honest.
CREATE OR REPLACE FUNCTION validate_entity_status() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM entity_status
        WHERE category = NEW.category AND subcategory = NEW.subcategory AND status = NEW.status
    ) THEN
        RAISE EXCEPTION 'invalid status % for %/%', NEW.status, NEW.category, NEW.subcategory;
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_entity_status
BEFORE INSERT OR UPDATE ON entity
FOR EACH ROW EXECUTE FUNCTION validate_entity_status();

-- 4. The relationship graph -- in the rejected per-entity-table design this
--    would have been a mix of ordinary FKs (sample.order_id,
--    sequencing_sample.sample_id, ...) plus a fan-in-only link table. Here
--    it is one table covering both cases. This is the single
--    mechanism for all lineage, in both directions: linear (Patient has
--    Order has Sample), fan-in (many library_samples pooled_into one
--    illumina_run), and Task edges (a Task analyzed_by a library_sample or
--    illumina_run; a Task produced a sample_result).
CREATE TABLE entity_relationship (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_entity_id   UUID NOT NULL REFERENCES entity(id),
    child_entity_id    UUID NOT NULL REFERENCES entity(id),
    relationship_type  TEXT NOT NULL,   -- 'has_order' | 'has_sample' | 'prepped_into' | 'pooled_into' |
                                          -- 'analyzed_by' | 'produced' | 'archived_from' | ...
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (parent_entity_id, child_entity_id, relationship_type)
);

-- Indexes
CREATE INDEX idx_entity_category            ON entity (category, subcategory);
CREATE INDEX idx_entity_status              ON entity (status);
CREATE INDEX idx_entity_correlation_id      ON entity (correlation_id);
CREATE INDEX idx_entity_attributes_gin      ON entity USING GIN (attributes);  -- e.g. attributes->>'mrn'
CREATE INDEX idx_entity_relationship_parent ON entity_relationship (parent_entity_id);
CREATE INDEX idx_entity_relationship_child  ON entity_relationship (child_entity_id);
CREATE INDEX idx_entity_relationship_type   ON entity_relationship (relationship_type);
