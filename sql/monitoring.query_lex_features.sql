CREATE TABLE IF NOT EXISTS monitoring.query_lex_features (
    dbid oid NOT NULL,
    userid oid NOT NULL,
    queryid bigint NOT NULL,

    query_text text NOT NULL,
    query_md5 text NOT NULL,

    query_len_chars int NOT NULL,
    query_len_norm_chars int NOT NULL,
    num_tokens int NOT NULL,

    num_joins int NOT NULL,
    num_where int NOT NULL,
    num_group_by int NOT NULL,
    num_order_by int NOT NULL,
    num_having int NOT NULL,
    num_union int NOT NULL,
    num_subqueries int NOT NULL,
    num_cte int NOT NULL,

    has_write boolean NOT NULL,
    has_ddl boolean NOT NULL,
    has_tx boolean NOT NULL,

    num_case int NOT NULL,
    num_functions int NOT NULL,

    first_seen_ts timestamptz NOT NULL DEFAULT now(),
    last_seen_ts  timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (dbid, userid, queryid)
);

CREATE INDEX IF NOT EXISTS idx_query_lex_features_last_seen
    ON monitoring.query_lex_features (last_seen_ts DESC);