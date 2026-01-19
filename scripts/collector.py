"""Collect pg_stat_statements snapshots into the raw table."""

from datetime import datetime, timezone

import psycopg

try:
    from db_config import DB_CONFIG
except Exception:
    from scripts.db_config import DB_CONFIG

SELECT_PGSS = """
SELECT
    dbid,
    userid,
    queryid,
    calls,
    total_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    temp_blks_read,
    temp_blks_written,
    wal_bytes,
    query AS query_text
FROM pg_stat_statements;
"""

INSERT_SNAPSHOT = """
INSERT INTO monitoring.pgss_snapshots_raw (
    snapshot_ts,
    dbid,
    userid,
    queryid,
    calls,
    total_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    temp_blks_read,
    temp_blks_written,
    wal_bytes,
    query_text
)
VALUES (
    %(snapshot_ts)s,
    %(dbid)s,
    %(userid)s,
    %(queryid)s,
    %(calls)s,
    %(total_exec_time)s,
    %(rows)s,
    %(shared_blks_hit)s,
    %(shared_blks_read)s,
    %(temp_blks_read)s,
    %(temp_blks_written)s,
    %(wal_bytes)s,
    %(query_text)s
);
"""


def collect_snapshot():
    """Collect one snapshot and insert rows into pgss_snapshots_raw."""
    with psycopg.connect(**DB_CONFIG, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(SELECT_PGSS)
                records = cur.fetchall()

                if not records:
                    print("No records found in pg_stat_statements.")
                    return

                snapshot_ts = datetime.now(timezone.utc)

                for r in records:
                    r["snapshot_ts"] = snapshot_ts

                cur.executemany(INSERT_SNAPSHOT, records)
                conn.commit()

                print(
                    f"{datetime.now()}: inserted {len(records)} rows into monitoring.pgss_snapshots_raw"
                )

            except Exception as e:
                conn.rollback()
                print(f"Error: {e}")


if __name__ == "__main__":
    collect_snapshot()
