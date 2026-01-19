"""Build aggregated features from pgss deltas."""

from datetime import datetime

import psycopg
from psycopg.rows import dict_row

try:
    from db_config import DB_CONFIG
except Exception:
    from scripts.db_config import DB_CONFIG


def load_unprocessed_deltas(cur):
    """Fetch deltas missing rows in monitoring.features_windows."""
    cur.execute(
        """
        SELECT
            d.window_start,
            d.window_end,
            d.dbid,
            d.userid,
            d.queryid,
            d.calls_delta,
            d.total_exec_time_delta,
            d.rows_delta,
            d.shared_blks_hit_delta,
            d.shared_blks_read_delta,
            d.temp_blks_read_delta,
            d.temp_blks_written_delta,
            d.wal_bytes_delta
        FROM monitoring.pgss_deltas d
        LEFT JOIN monitoring.features_windows f
          ON f.window_start = d.window_start
         AND f.dbid         = d.dbid
         AND f.userid       = d.userid
         AND f.queryid      = d.queryid
        WHERE f.window_start IS NULL;
        """
    )
    return cur.fetchall()


def compute_features(r):
    """Build a feature dict from one delta row."""
    window_start = r["window_start"]
    window_end = r["window_end"]

    calls = r["calls_delta"]
    total_exec_ms = r["total_exec_time_delta"]
    rows = r["rows_delta"]
    shared_hit = r["shared_blks_hit_delta"]
    shared_read = r["shared_blks_read_delta"]
    temp_read = r["temp_blks_read_delta"]
    wal_bytes = r["wal_bytes_delta"]

    window_len_sec = (window_end - window_start).total_seconds()
    if window_len_sec <= 0:
        window_len_sec = 1.0

    if calls <= 0:
        return None

    exec_time_per_call_ms = total_exec_ms / calls
    rows_per_call = rows / calls
    shared_read_per_call = shared_read / calls
    temp_read_per_call = temp_read / calls
    wal_bytes_per_call = wal_bytes / calls

    calls_per_sec = calls / window_len_sec
    exec_ms_per_sec = total_exec_ms / window_len_sec
    rows_per_sec = rows / window_len_sec
    shared_read_per_sec = shared_read / window_len_sec
    temp_read_per_sec = temp_read / window_len_sec
    wal_bytes_per_sec = wal_bytes / window_len_sec

    denom_reads = shared_read + temp_read
    temp_share = (temp_read / denom_reads) if denom_reads > 0 else None

    denom_cache = shared_read + shared_hit
    cache_miss_ratio = (shared_read / denom_cache) if denom_cache > 0 else None

    ms_per_row = total_exec_ms / max(rows, 1)
    read_blks_per_row = shared_read / max(rows, 1)

    return {
        "window_start": window_start,
        "window_end": window_end,
        "dbid": r["dbid"],
        "userid": r["userid"],
        "queryid": r["queryid"],
        "window_len_sec": float(window_len_sec),
        "calls_in_window": int(calls),
        "calls_per_sec": float(calls_per_sec),
        "exec_time_per_call_ms": float(exec_time_per_call_ms),
        "exec_ms_per_sec": float(exec_ms_per_sec),
        "rows_per_call": float(rows_per_call),
        "rows_per_sec": float(rows_per_sec),
        "shared_read_per_call": float(shared_read_per_call),
        "shared_read_per_sec": float(shared_read_per_sec),
        "temp_read_per_call": float(temp_read_per_call),
        "temp_read_per_sec": float(temp_read_per_sec),
        "wal_bytes_per_call": float(wal_bytes_per_call),
        "wal_bytes_per_sec": float(wal_bytes_per_sec),
        "temp_share": float(temp_share) if temp_share is not None else None,
        "cache_miss_ratio": (
            float(cache_miss_ratio) if cache_miss_ratio is not None else None
        ),
        "ms_per_row": float(ms_per_row),
        "read_blks_per_row": float(read_blks_per_row),
    }


def save_features(cur, features):
    """Insert feature rows into monitoring.features_windows."""
    if not features:
        return 0

    query = """
        INSERT INTO monitoring.features_windows (
            window_start,
            window_end,
            dbid,
            userid,
            queryid,
            window_len_sec,

            calls_in_window,
            calls_per_sec,

            exec_time_per_call_ms,
            exec_ms_per_sec,

            rows_per_call,
            rows_per_sec,

            shared_read_per_call,
            shared_read_per_sec,

            temp_read_per_call,
            temp_read_per_sec,

            wal_bytes_per_call,
            wal_bytes_per_sec,

            temp_share,
            cache_miss_ratio,

            ms_per_row,
            read_blks_per_row
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s,

            %s, %s,

            %s, %s,

            %s, %s,

            %s, %s,

            %s, %s,

            %s, %s,

            %s, %s,

            %s, %s
        )
        ON CONFLICT DO NOTHING;
    """

    params = [
        (
            f["window_start"],
            f["window_end"],
            f["dbid"],
            f["userid"],
            f["queryid"],
            f["window_len_sec"],
            f["calls_in_window"],
            f["calls_per_sec"],
            f["exec_time_per_call_ms"],
            f["exec_ms_per_sec"],
            f["rows_per_call"],
            f["rows_per_sec"],
            f["shared_read_per_call"],
            f["shared_read_per_sec"],
            f["temp_read_per_call"],
            f["temp_read_per_sec"],
            f["wal_bytes_per_call"],
            f["wal_bytes_per_sec"],
            f["temp_share"],
            f["cache_miss_ratio"],
            f["ms_per_row"],
            f["read_blks_per_row"],
        )
        for f in features
    ]

    cur.executemany(query, params)
    return len(features)


def build_features():
    """Load deltas, compute features, and persist them."""
    with psycopg.connect(**DB_CONFIG, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            deltas = load_unprocessed_deltas(cur)

            if not deltas:
                print("No new deltas to process.")
                return

            features = []
            for row in deltas:
                f = compute_features(row)
                if f is not None:
                    features.append(f)

            if not features:
                print("No valid features computed.")
                return

            inserted = save_features(cur, features)
            conn.commit()

            print(
                f"{datetime.now()}: inserted {inserted} rows into monitoring.features_windows"
            )


if __name__ == "__main__":
    build_features()
