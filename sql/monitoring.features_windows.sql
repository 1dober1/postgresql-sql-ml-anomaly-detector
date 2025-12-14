CREATE TABLE IF NOT EXISTS monitoring.features_windows (
    window_start           timestamptz NOT NULL,
    window_end             timestamptz NOT NULL,
    dbid                   oid         NOT NULL,
    userid                 oid         NOT NULL,
    queryid                bigint      NOT NULL,

    window_len_sec         double precision NOT NULL,

    calls_in_window        bigint      NOT NULL,
    calls_per_sec          double precision,

    exec_time_per_call_ms  double precision,
    exec_ms_per_sec        double precision,

    rows_per_call          double precision,
    rows_per_sec           double precision,

    shared_read_per_call   double precision,
    shared_read_per_sec    double precision,

    temp_read_per_call     double precision,
    temp_read_per_sec      double precision,

    wal_bytes_per_call     double precision,
    wal_bytes_per_sec      double precision,

    temp_share             double precision,
    cache_miss_ratio       double precision,

    ms_per_row             double precision,
    read_blks_per_row      double precision,

    PRIMARY KEY (window_start, dbid, userid, queryid)
);

CREATE INDEX IF NOT EXISTS idx_features_qid_ts
    ON monitoring.features_windows (dbid, userid, queryid, window_start);