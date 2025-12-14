CREATE TABLE IF NOT EXISTS monitoring.pgss_snapshots_raw (
    snapshot_id       bigserial PRIMARY KEY,
    snapshot_ts       timestamptz NOT NULL,
    dbid              oid         NOT NULL,
    userid            oid         NOT NULL,
    queryid           bigint      NOT NULL,
    calls             bigint      NOT NULL,
    total_exec_time   double precision NOT NULL,
    rows              bigint      NOT NULL,
    shared_blks_hit   bigint      NOT NULL,
    shared_blks_read  bigint      NOT NULL,
    temp_blks_read    bigint      NOT NULL,
    temp_blks_written bigint      NOT NULL,
    wal_bytes         bigint      NOT NULL,
    query_text        text        NULL
);

CREATE INDEX IF NOT EXISTS idx_pgss_snapshots_raw_ts_qid
    ON monitoring.pgss_snapshots_raw (snapshot_ts, dbid, userid, queryid);