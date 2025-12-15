CREATE TABLE IF NOT EXISTS monitoring.detector_state (
    id              smallint PRIMARY KEY DEFAULT 1,
    last_window_end timestamptz NULL,
    bad_runs_streak int NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (id = 1)
);