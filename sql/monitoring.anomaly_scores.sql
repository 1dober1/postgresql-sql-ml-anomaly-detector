CREATE TABLE IF NOT EXISTS monitoring.anomaly_scores (
    window_start   timestamptz NOT NULL,
    window_end     timestamptz NOT NULL,
    dbid           oid         NOT NULL,
    userid         oid         NOT NULL,
    queryid        bigint      NOT NULL,

    model_version  text        NOT NULL,
    anomaly_score  double precision NOT NULL,

    features       jsonb       NOT NULL,
    scored_at      timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (model_version, window_end, dbid, userid, queryid)
);