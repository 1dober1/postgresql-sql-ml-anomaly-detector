CREATE TABLE IF NOT EXISTS monitoring.pgss_deltas (
    window_start              timestamptz NOT NULL,
    window_end                timestamptz NOT NULL,
    dbid                      oid         NOT NULL,
    userid                    oid         NOT NULL,
    queryid                   bigint      NOT NULL,

    calls_delta               bigint      NOT NULL,
    total_exec_time_delta     double precision NOT NULL,
    rows_delta                bigint      NOT NULL,
    shared_blks_hit_delta     bigint      NOT NULL,
    shared_blks_read_delta    bigint      NOT NULL,
    temp_blks_read_delta      bigint      NOT NULL,
    temp_blks_written_delta   bigint      NOT NULL,
    wal_bytes_delta           bigint      NOT NULL,

    PRIMARY KEY (window_start, dbid, userid, queryid)
);

CREATE INDEX IF NOT EXISTS idx_pgss_deltas_qid_ts
    ON monitoring.pgss_deltas (dbid, userid, queryid, window_start);