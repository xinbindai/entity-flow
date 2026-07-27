-- Recommended event schema (PostgreSQL), synthesizing the design discussed:
-- entity/event/task model, outbox pattern, correlation/causation tracing,
-- envelope-vs-payload separation, and per-handler idempotency.
--
-- Design notes:
-- 1. One table serves double duty as both the outbox (short-lived relay
--    buffer) and the durable event log (long-term audit/replay source) --
--    just never delete rows, only flag them published. Split into two
--    tables later only if write volume on the hot outbox path demands it.
-- 2. Envelope fields (who/what/when/why) are columns; only the
--    event-type-specific business data lives in the JSONB payload.
-- 3. causation_id can point at either a prior event or a task (a task
--    creating an event, or an event triggering a task creation record),
--    so it's modeled as a polymorphic (type, id) pair rather than a
--    strict FK to this same table.

CREATE TABLE events (
    -- identity
    event_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type          TEXT NOT NULL,              -- 'SequencingReady', 'AnalysisTaskSucceeded', ...
    schema_version       INT NOT NULL DEFAULT 1,      -- for safe payload evolution

    -- subject: what entity is this event about
    entity_type          TEXT NOT NULL,               -- 'Sample' | 'SequencingSample' | 'SampleBatch' | 'SampleResult' | 'Task' | ...
    entity_id            UUID NOT NULL,

    -- lineage / tracing (envelope, not payload -- see "trace source" discussion)
    correlation_id        UUID NOT NULL,               -- constant across one whole patient->result journey
    causation_type        TEXT,                        -- 'event' | 'task' | null (root event, e.g. manual order creation)
    causation_id           UUID,                        -- id of the event or task that directly caused this one
    trace_id               TEXT,                        -- optional OpenTelemetry trace id, for cross-service debugging

    -- provenance: who/what produced this event
    source                 TEXT NOT NULL,               -- producing service, e.g. 'pipeline-orchestrator', 'sample-service'
    producer_version       TEXT,                        -- deploy/build version of the producing service
    actor_type             TEXT NOT NULL,               -- 'system' | 'user' | 'worker'
    actor_id               TEXT,                        -- user id, worker id, or null for pure system actions

    -- payload: event-type-specific business data only
    payload                 JSONB NOT NULL,

    -- timing
    -- business time: when the fact actually happened. No default -- only the
    -- producer knows it, and now() is transaction *start* time, so it would
    -- both lag the real event and collapse every event in one transaction.
    occurred_at             TIMESTAMPTZ NOT NULL,
    -- write time. clock_timestamp(), not now(), so events inserted in one
    -- transaction get distinct values. Still insert time, not commit time.
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- outbox relay bookkeeping (irrelevant once published; kept for audit)
    published_at            TIMESTAMPTZ,                          -- null until relayed to the bus
    publish_attempts        INT NOT NULL DEFAULT 0
);

-- Common query patterns from the discussion above:
-- "everything in one patient's journey" -> correlation_id
-- "history of one entity"               -> entity_type + entity_id
-- "what happened next after X"          -> causation_type + causation_id
-- "outbox poller's hot path"            -> unpublished rows only
CREATE INDEX idx_events_correlation_id  ON events (correlation_id);
CREATE INDEX idx_events_entity          ON events (entity_type, entity_id);
CREATE INDEX idx_events_causation       ON events (causation_type, causation_id);
CREATE INDEX idx_events_type_occurred   ON events (event_type, occurred_at);
CREATE INDEX idx_events_unpublished     ON events (published_at) WHERE published_at IS NULL;

-- Optional at scale: partition by month on occurred_at for retention
-- management, e.g. PARTITION BY RANGE (occurred_at).

-- Per-handler idempotency ledger (only needed for handlers whose side
-- effect isn't naturally idempotent -- see "processed-events ledger"
-- discussion). A naturally idempotent handler, e.g. an UPDATE ... WHERE
-- status != 'Complete', doesn't need a row here at all.
CREATE TABLE event_handler_checkpoints (
    handler_name     TEXT NOT NULL,               -- e.g. 'create-pipeline-task-on-sequencing-ready'
    event_id         UUID NOT NULL REFERENCES events(event_id),
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_name, event_id)
);
