from datetime import datetime

import psycopg
from psycopg.rows import dict_row

try:
    from db_config import DB_CONFIG
except Exception:
    from scripts.db_config import DB_CONFIG


def get_last_processed_window_end(cur):
    """
    Возвращает последний window_end, который уже есть в pgss_deltas.
    Если таблица пустая -> None.
    """
    cur.execute("SELECT max(window_end) AS last_end FROM monitoring.pgss_deltas;")
    row = cur.fetchone()
    return row["last_end"] if row and row["last_end"] is not None else None


def get_snapshot_timestamps(cur, since_ts=None):
    """
    Возвращает список DISTINCT snapshot_ts по возрастанию.
    Если since_ts задан, берём снапшоты >= since_ts (чтобы можно было построить пары).
    """
    if since_ts is None:
        cur.execute(
            """
            SELECT DISTINCT snapshot_ts
            FROM monitoring.pgss_snapshots_raw
            ORDER BY snapshot_ts ASC;
            """
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT snapshot_ts
            FROM monitoring.pgss_snapshots_raw
            WHERE snapshot_ts >= %s
            ORDER BY snapshot_ts ASC;
            """,
            (since_ts,),
        )
    rows = cur.fetchall()
    return [r["snapshot_ts"] for r in rows]


def window_already_processed(cur, window_start):
    """
    Проверяем, есть ли уже хоть одна строка для окна с данным window_start.
    """
    cur.execute(
        """
        SELECT 1
        FROM monitoring.pgss_deltas
        WHERE window_start = %s
        LIMIT 1;
        """,
        (window_start,),
    )
    return cur.fetchone() is not None


def load_snapshot(cur, snapshot_ts):
    """
    Загружаем все строки для конкретного snapshot_ts в словарь:
    key = (dbid, userid, queryid)
    value = dict с метриками.
    """
    cur.execute(
        """
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
            wal_bytes
        FROM monitoring.pgss_snapshots_raw
        WHERE snapshot_ts = %s;
        """,
        (snapshot_ts,),
    )
    rows = cur.fetchall()

    snapshot = {}
    for r in rows:
        key = (r["dbid"], r["userid"], r["queryid"])
        snapshot[key] = r
    return snapshot


def safe_delta(curr_val, prev_val):
    """
    Возвращает (curr - prev) или None, если одно из значений None.
    """
    if curr_val is None or prev_val is None:
        return None
    return curr_val - prev_val


def deltas_for_window(prev_snapshot, curr_snapshot, window_start, window_end):
    """
    Считаем дельты для одного окна.
      - если ключ появился впервые (нет prev), считаем prev = 0 (это нормально для новых queryid).
      - любые отрицательные дельты считаем некорректными (reset/перезапуск) и пропускаем.
    """
    deltas = []

    for key, curr in curr_snapshot.items():
        prev = prev_snapshot.get(key)

        if prev is None:
            prev = {
                "calls": 0,
                "total_exec_time": 0.0,
                "rows": 0,
                "shared_blks_hit": 0,
                "shared_blks_read": 0,
                "temp_blks_read": 0,
                "temp_blks_written": 0,
                "wal_bytes": 0,
            }

        calls_delta = safe_delta(curr["calls"], prev["calls"])
        total_exec_time_delta = safe_delta(
            curr["total_exec_time"], prev["total_exec_time"]
        )
        rows_delta = safe_delta(curr["rows"], prev["rows"])
        shared_hit_delta = safe_delta(curr["shared_blks_hit"], prev["shared_blks_hit"])
        shared_read_delta = safe_delta(
            curr["shared_blks_read"], prev["shared_blks_read"]
        )
        temp_read_delta = safe_delta(curr["temp_blks_read"], prev["temp_blks_read"])
        temp_written_delta = safe_delta(
            curr["temp_blks_written"], prev["temp_blks_written"]
        )
        wal_bytes_delta = safe_delta(curr["wal_bytes"], prev["wal_bytes"])

        if calls_delta is None or total_exec_time_delta is None:
            continue

        if calls_delta <= 0:
            continue

        candidates = [
            total_exec_time_delta,
            rows_delta,
            shared_hit_delta,
            shared_read_delta,
            temp_read_delta,
            temp_written_delta,
            wal_bytes_delta,
        ]

        if any(v is not None and v < 0 for v in candidates):
            continue

        deltas.append(
            {
                "window_start": window_start,
                "window_end": window_end,
                "dbid": key[0],
                "userid": key[1],
                "queryid": key[2],
                "calls_delta": int(calls_delta),
                "total_exec_time_delta": float(total_exec_time_delta),
                "rows_delta": int(rows_delta) if rows_delta is not None else 0,
                "shared_blks_hit_delta": (
                    int(shared_hit_delta) if shared_hit_delta is not None else 0
                ),
                "shared_blks_read_delta": (
                    int(shared_read_delta) if shared_read_delta is not None else 0
                ),
                "temp_blks_read_delta": (
                    int(temp_read_delta) if temp_read_delta is not None else 0
                ),
                "temp_blks_written_delta": (
                    int(temp_written_delta) if temp_written_delta is not None else 0
                ),
                "wal_bytes_delta": (
                    int(wal_bytes_delta) if wal_bytes_delta is not None else 0
                ),
            }
        )

    return deltas


def save_deltas(cur, deltas):
    if not deltas:
        return 0

    query = """
        INSERT INTO monitoring.pgss_deltas (
            window_start,
            window_end,
            dbid,
            userid,
            queryid,
            calls_delta,
            total_exec_time_delta,
            rows_delta,
            shared_blks_hit_delta,
            shared_blks_read_delta,
            temp_blks_read_delta,
            temp_blks_written_delta,
            wal_bytes_delta
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT DO NOTHING;
    """

    params = [
        (
            d["window_start"],
            d["window_end"],
            d["dbid"],
            d["userid"],
            d["queryid"],
            d["calls_delta"],
            d["total_exec_time_delta"],
            d["rows_delta"],
            d["shared_blks_hit_delta"],
            d["shared_blks_read_delta"],
            d["temp_blks_read_delta"],
            d["temp_blks_written_delta"],
            d["wal_bytes_delta"],
        )
        for d in deltas
    ]

    cur.executemany(query, params)
    return len(deltas)


def build_deltas_backfill():
    """
    Главная функция: догоняет все окна, которые ещё не посчитаны.
    """
    with psycopg.connect(**DB_CONFIG, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            last_end = get_last_processed_window_end(cur)

            snapshot_ts = get_snapshot_timestamps(cur, since_ts=last_end)

            if len(snapshot_ts) < 2:
                print("Not enough snapshots to compute deltas.")
                return

            total_inserted = 0

            for i in range(1, len(snapshot_ts)):
                window_start = snapshot_ts[i - 1]
                window_end = snapshot_ts[i]

                if window_already_processed(cur, window_start):
                    continue

                prev_snapshot = load_snapshot(cur, window_start)
                curr_snapshot = load_snapshot(cur, window_end)

                deltas = deltas_for_window(
                    prev_snapshot, curr_snapshot, window_start, window_end
                )
                inserted = save_deltas(cur, deltas)
                total_inserted += inserted
            conn.commit()
            print(
                f"{datetime.now()}: inserted {total_inserted} rows into monitoring.pgss_deltas"
            )


if __name__ == "__main__":
    build_deltas_backfill()
