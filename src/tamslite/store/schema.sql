CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS sources (
    id          uuid PRIMARY KEY,
    format      text NOT NULL,
    doc         jsonb NOT NULL,
    created     timestamptz NOT NULL DEFAULT now(),
    updated     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS flows (
    id                uuid PRIMARY KEY,
    source_id         uuid NOT NULL REFERENCES sources(id),
    format            text NOT NULL,
    codec             text,
    container         text,
    doc               jsonb NOT NULL,
    created           timestamptz NOT NULL DEFAULT now(),
    metadata_updated  timestamptz NOT NULL DEFAULT now(),
    segments_updated  timestamptz
);

CREATE TABLE IF NOT EXISTS objects (
    object_id        text PRIMARY KEY,
    flow_id_created  uuid NOT NULL,
    storage_key      text NOT NULL,
    object_timerange text,
    key_frame_count  integer,
    uploaded         boolean NOT NULL DEFAULT false,
    created          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS segments (
    flow_id    uuid NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
    object_id  text NOT NULL REFERENCES objects(object_id),
    ts_range   int8range NOT NULL,  -- half-open [start_ns, end_ns) on the TAI timeline
    doc        jsonb NOT NULL,      -- the segment as posted (validated), minus get_urls
    created    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (flow_id, ts_range),
    CONSTRAINT segments_no_overlap EXCLUDE USING gist (flow_id WITH =, ts_range WITH &&)
);

CREATE INDEX IF NOT EXISTS segments_by_object ON segments (object_id);
