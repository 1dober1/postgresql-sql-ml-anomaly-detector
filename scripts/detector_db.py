import psycopg
from psycopg.rows import dict_row

try:
    from scripts.db_config import DB_CONFIG
except Exception:
    from db_config import DB_CONFIG


STATE_SELECT = """
SELECT last_window_end, bad_runs_streak
FROM monitoring.detector_state
WHERE id = 1;
"""

STATE_UPSERT_INIT = """
INSERT INTO monitoring.detector_state (id, last_window_end, bad_runs_streak)
VALUES (1, NULL, 0)
ON CONFLICT (id) DO NOTHING;
"""

STATE_UPDATE = """
UPDATE monitoring.detector_state
SET last_window_end = %s,
    bad_runs_streak = %s,
    updated_at = now()
WHERE id = 1;
"""

FETCH_WINDOWS = """
SELECT *
FROM monitoring.features_with_lex
WHERE window_end > COALESCE(%s::timestamptz, '-infinity'::timestamptz)
ORDER BY window_end ASC
LIMIT %s;
"""

INSERT_ANOMALIES = """
INSERT INTO monitoring.anomaly_scores (
  window_start, window_end, dbid, userid, queryid,
  model_version, anomaly_score, features, scored_at
) VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s::jsonb, %s
)
ON CONFLICT (model_version, window_end, dbid, userid, queryid) DO NOTHING;
"""


def connect():
    return psycopg.connect(**DB_CONFIG, row_factory=dict_row)


def ensure_state_row(conn):
    with conn.cursor() as cur:
        cur.execute(STATE_UPSERT_INIT)
    conn.commit()


def load_state(conn):
    ensure_state_row(conn)
    with conn.cursor() as cur:
        cur.execute(STATE_SELECT)
        row = cur.fetchone()
        if not row:
            return {"last_window_end": None, "bad_runs_streak": 0}
        return {
            "last_window_end": row["last_window_end"],
            "bad_runs_streak": row["bad_runs_streak"],
        }


def save_state(conn, last_window_end, bad_runs_streak):
    with conn.cursor() as cur:
        cur.execute(STATE_UPDATE, (last_window_end, bad_runs_streak))
    conn.commit()


def fetch_new_windows(conn, last_window_end, limit: int):
    with conn.cursor() as cur:
        cur.execute(FETCH_WINDOWS, (last_window_end, limit))
        rows = cur.fetchall()
        return rows


def insert_anomaly_rows(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(INSERT_ANOMALIES, rows)
    conn.commit()
