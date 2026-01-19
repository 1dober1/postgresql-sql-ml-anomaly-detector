"""Bootstrap and run the detector pipeline and database."""

import time
import os
import sys
import subprocess
import psycopg

try:
    from detector_alerts import send_telegram
except Exception:

    def send_telegram(_: str) -> None:
        return


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
sys.path.append(PROJECT_ROOT)

try:
    from scripts.db_config import DB_CONFIG
except Exception:
    print("❌ Не найден db_config.py / DB_CONFIG")
    sys.exit(1)

S_COLLECT = os.path.join(BASE_DIR, "collector.py")
S_DELTAS = os.path.join(BASE_DIR, "build_deltas.py")
S_FEATURES = os.path.join(BASE_DIR, "build_features.py")
S_LEX = os.path.join(BASE_DIR, "build_lex_features.py")
S_TRAIN = os.path.join(BASE_DIR, "train_model.py")
S_DETECT = os.path.join(BASE_DIR, "detect_anomalies.py")

COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", "15"))
RETRAIN_INTERVAL = int(os.getenv("RETRAIN_INTERVAL", str(24 * 60 * 60)))

TRAIN_COLLECT_ITERATIONS = int(os.getenv("TRAIN_COLLECT_ITERATIONS", "40"))
TRAIN_COLLECT_SLEEP = int(os.getenv("TRAIN_COLLECT_SLEEP", "10"))
TRAIN_RETRY_LIMIT = int(os.getenv("TRAIN_RETRY_LIMIT", "10"))

MODEL_FILE = os.getenv("MODEL_FILE", "model_baseline_v1.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "baseline_v1")

DDL_INIT = r"""
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

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

CREATE TABLE IF NOT EXISTS monitoring.detector_state (
    id              smallint PRIMARY KEY DEFAULT 1,
    last_window_end timestamptz NULL,
    bad_runs_streak int NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (id = 1)
);

INSERT INTO monitoring.detector_state (id, last_window_end, bad_runs_streak)
VALUES (1, NULL, 0)
ON CONFLICT (id) DO NOTHING;

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

CREATE INDEX IF NOT EXISTS idx_anomaly_scores_ts
    ON monitoring.anomaly_scores (window_end DESC);

DROP VIEW IF EXISTS monitoring.features_with_lex;

CREATE VIEW monitoring.features_with_lex AS
SELECT
    w.*,
    l.query_text,
    l.query_md5,
    l.query_len_chars,
    l.query_len_norm_chars,
    l.num_tokens,
    l.num_joins,
    l.num_where,
    l.num_group_by,
    l.num_order_by,
    l.num_having,
    l.num_union,
    l.num_subqueries,
    l.num_cte,
    l.has_write,
    l.has_ddl,
    l.has_tx,
    l.num_case,
    l.num_functions
FROM monitoring.features_windows w
LEFT JOIN monitoring.query_lex_features l
  ON l.dbid = w.dbid AND l.userid = w.userid AND l.queryid = w.queryid;
"""


def wait_for_db(max_wait_sec: int = 120):
    """Wait for PostgreSQL availability until the timeout."""
    print("⏳ Ожидание доступности PostgreSQL...")
    started = time.time()
    while True:
        try:
            with psycopg.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            print("✅ PostgreSQL доступен.")
            return
        except Exception as e:
            if time.time() - started > max_wait_sec:
                print(f"❌ PostgreSQL не доступен > {max_wait_sec} сек: {e}")
                raise
            time.sleep(2)


def init_db_structure():
    """Create the monitoring schema and tables if missing."""
    print("🧱 Инициализация структуры monitoring...")
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_INIT)
        conn.commit()
    print("✅ Структура готова.")


def _run(script_path: str, check: bool = True):
    """Run a Python script as a separate process."""
    subprocess.run([sys.executable, script_path], check=check)


def run_pipeline_once():
    """Run one pipeline pass.

    Runs collection, deltas, features, lex, and detection.
    """
    _run(S_COLLECT, check=True)
    _run(S_DELTAS, check=True)
    _run(S_FEATURES, check=True)
    _run(S_LEX, check=True)
    _run(S_DETECT, check=True)


def run_training_cycle():
    """Run bootstrap collection and model training with retries."""
    attempts = 0
    while True:
        attempts += 1
        print("🧪 Первичная подготовка данных для обучения (bootstrap)...")
        send_telegram("🧪 Bootstrap: собираю baseline для обучения модели…")

        for i in range(TRAIN_COLLECT_ITERATIONS):
            try:
                _run(S_COLLECT, check=True)
                _run(S_DELTAS, check=True)
                _run(S_FEATURES, check=True)
                _run(S_LEX, check=True)
                sys.stdout.write(f"\r   progress {i + 1}/{TRAIN_COLLECT_ITERATIONS}\n")
                sys.stdout.flush()
            except Exception as e:
                print(f"\n❌ Ошибка bootstrap-итерации: {e}")
            time.sleep(TRAIN_COLLECT_SLEEP)

        print("\n🎓 Обучение модели...")
        send_telegram("🎓 Обучение модели…")
        try:
            _run(S_TRAIN, check=True)
        except Exception as e:
            send_telegram(
                f"⚠️ Обучение не удалось (attempt {attempts}/{TRAIN_RETRY_LIMIT}): {e}"
            )
            if attempts >= TRAIN_RETRY_LIMIT:
                raise
            continue

        if not os.path.exists(MODEL_FILE):
            send_telegram(
                f"⚠️ Файл модели {MODEL_FILE} не создан после обучения. Повторяю bootstrap…"
            )
            if attempts >= TRAIN_RETRY_LIMIT:
                raise RuntimeError(f"Model file {MODEL_FILE} was not created.")
            continue

        print("✅ Модель обучена.")
        send_telegram(
            f"✅ Модель обучена: {MODEL_FILE} ({MODEL_VERSION}). Запускаю детекцию…"
        )
        return


def main_loop():
    """Run the detection loop and scheduled retraining."""
    print("🔁 Запуск основного цикла детекции...")
    send_telegram(f"🚀 Детектор запущен. Модель: {MODEL_FILE} ({MODEL_VERSION}).")
    last_retrain = time.time()

    while True:
        try:
            run_pipeline_once()
        except Exception as e:
            print(f"❌ Ошибка пайплайна: {e}")

        if time.time() - last_retrain >= RETRAIN_INTERVAL:
            try:
                print("🕒 Плановое переобучение модели...")
                send_telegram("🕒 Плановое переобучение модели…")
                _run(S_TRAIN, check=True)
                last_retrain = time.time()
                print("✅ Плановое переобучение завершено.")
                send_telegram("✅ Плановое переобучение завершено.")
            except Exception as e:
                print(f"❌ Ошибка планового переобучения: {e}")
                send_telegram(f"❌ Ошибка планового переобучения: {e}")
        time.sleep(COLLECT_INTERVAL)


if __name__ == "__main__":
    wait_for_db()
    init_db_structure()

    model_in_root = os.path.exists(os.path.join(PROJECT_ROOT, MODEL_FILE))
    model_in_cwd = os.path.exists(MODEL_FILE)

    if not os.path.exists(MODEL_FILE):
        run_training_cycle()
    else:
        print(f"✅ Модель найдена: {MODEL_FILE}")

    main_loop()
