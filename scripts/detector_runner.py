import os
import pickle
from datetime import datetime, timezone

import pandas as pd

from detector_features import (
    coerce_features_df,
    prepare_model_features_df,
    build_features_json,
    dumps_json,
)
from detector_db import (
    connect,
    load_state,
    save_state,
    fetch_new_windows,
    insert_anomaly_rows,
)
from detector_alerts import (
    is_system_query,
    send_telegram,
    fetch_usernames_batch,
    build_alert_message,
)
from detector_drift import update_streak

MODEL_FILENAME = os.getenv("MODEL_FILE", "model_baseline_v1.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "baseline_v1")

BATCH_LIMIT = int(os.getenv("DETECT_BATCH_LIMIT", "2000"))
CONSECUTIVE_RUNS_LIMIT = int(os.getenv("DRIFT_CONSECUTIVE_LIMIT", "5"))
ALERT_TOP_K = int(os.getenv("ALERT_TOP_K", "5"))

ALERT_METRICS = [
    "exec_time_per_call_ms",
    "rows_per_call",
    "shared_read_per_call",
    "wal_bytes_per_call",
]

SIGNIF_EXEC_MS = float(os.getenv("DRIFT_SIGNIF_EXEC_MS", "20"))
SIGNIF_ROWS = float(os.getenv("DRIFT_SIGNIF_ROWS", "50000"))
SIGNIF_SHARED_READ = float(os.getenv("DRIFT_SIGNIF_SHARED_READ", "200"))
SIGNIF_WAL_BYTES = float(os.getenv("DRIFT_SIGNIF_WAL_BYTES", "200000"))


def _score_threshold_from_env_or_model(model_threshold: float | None) -> float:
    v = os.getenv("ALERT_SCORE_THRESHOLD")
    if v is None:
        return float(model_threshold or 0.0)
    v = v.strip().lower()
    if v in ("", "auto", "none"):
        return float(model_threshold or 0.0)
    try:
        return float(v)
    except Exception:
        return float(model_threshold or 0.0)


def _threshold_override_from_env() -> float | None:
    v = os.getenv("ALERT_SCORE_THRESHOLD")
    if v is None:
        return None
    v = v.strip().lower()
    if v in ("", "auto", "none"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def load_model_or_train():
    if os.path.exists(MODEL_FILENAME):
        with open(MODEL_FILENAME, "rb") as f:
            return pickle.load(f)

    from train_model import train as train_emergency

    train_emergency()
    if not os.path.exists(MODEL_FILENAME):
        raise FileNotFoundError(
            f"Model training finished, but file '{MODEL_FILENAME}' was not created. "
            "Check train_model.py MODEL_FILENAME/env handling."
        )
    with open(MODEL_FILENAME, "rb") as f:
        return pickle.load(f)


def run_once():
    model_obj = load_model_or_train()
    model_threshold = None
    thresholds_by_query = None
    if isinstance(model_obj, dict) and "pipeline" in model_obj:
        model_threshold = model_obj.get("threshold")
        thresholds_by_query = model_obj.get("thresholds_by_query")
        model = model_obj["pipeline"]
    else:
        model = model_obj
    override_threshold = _threshold_override_from_env()
    global_threshold = float(
        override_threshold
        if override_threshold is not None
        else (model_threshold if model_threshold is not None else 0.0)
    )
    use_per_query_thresholds = (
        override_threshold is None
        and isinstance(thresholds_by_query, dict)
        and len(thresholds_by_query) > 0
    )

    with connect() as conn:
        state = load_state(conn)
        last_window_end = state["last_window_end"]
        bad_runs_streak = int(state["bad_runs_streak"] or 0)

        rows = fetch_new_windows(conn, last_window_end, BATCH_LIMIT)
        if not rows:
            return

        df_all = pd.DataFrame(rows)
        df_all = coerce_features_df(df_all)
        new_last_window_end = df_all["window_end"].max()

        qt = df_all.get("query_text")
        if qt is None:
            df = df_all.iloc[0:0].copy()
        else:
            mask = qt.notna() & ~qt.apply(is_system_query)
            df = df_all[mask].copy()

        if df.empty:
            save_state(conn, new_last_window_end, bad_runs_streak)
            return

        X = prepare_model_features_df(df)
        scores = model.decision_function(X)

        df["anomaly_score"] = scores

        if use_per_query_thresholds:
            df["score_threshold"] = [
                float(
                    thresholds_by_query.get(
                        (int(dbid), int(userid), int(queryid)), global_threshold
                    )
                )
                for dbid, userid, queryid in zip(
                    df["dbid"], df["userid"], df["queryid"]
                )
            ]
            df["is_anomaly"] = df["anomaly_score"] <= df["score_threshold"]
        else:
            df["score_threshold"] = global_threshold
            df["is_anomaly"] = df["anomaly_score"] <= global_threshold
        df_anom = df[df["is_anomaly"]].copy()
        now_ts = datetime.now(timezone.utc)

        insert_rows = []
        for _, r in df_anom.iterrows():
            features = build_features_json(r)
            insert_rows.append(
                (
                    r["window_start"],
                    r["window_end"],
                    r["dbid"],
                    r["userid"],
                    r["queryid"],
                    MODEL_VERSION,
                    float(r["anomaly_score"]),
                    dumps_json(features),
                    now_ts,
                )
            )
        insert_anomaly_rows(conn, insert_rows)

        significant_alerts_total = 0
        if not df_anom.empty:
            significant_alerts_total = int(
                (
                    (df_anom["exec_time_per_call_ms"] >= SIGNIF_EXEC_MS)
                    | (df_anom["rows_per_call"] >= SIGNIF_ROWS)
                    | (df_anom["shared_read_per_call"] >= SIGNIF_SHARED_READ)
                    | (df_anom["wal_bytes_per_call"] >= SIGNIF_WAL_BYTES)
                ).sum()
            )

        if ALERT_TOP_K > 0 and len(df_anom) > ALERT_TOP_K:
            df_alert = df_anom.nsmallest(ALERT_TOP_K, "anomaly_score")
        else:
            df_alert = df_anom

        real_alerts_sent = 0
        if not df_alert.empty:
            user_map = fetch_usernames_batch(conn, set(df_alert["userid"].tolist()))

            for _, r in df_alert.iterrows():
                score = float(r["anomaly_score"])

                qtext = r.get("query_text")
                if is_system_query(qtext):
                    continue

                username = user_map.get(r["userid"], f"Unknown({r['userid']})")
                metrics = {m: float(r.get(m, 0) or 0) for m in ALERT_METRICS}

                msg = build_alert_message(username, score, qtext, metrics)
                send_telegram(msg)
                real_alerts_sent += 1

        bad_runs_streak, drift = update_streak(
            bad_runs_streak=bad_runs_streak,
            real_alerts_sent=significant_alerts_total,
            consecutive_limit=CONSECUTIVE_RUNS_LIMIT,
        )

        if drift:
            send_telegram(
                f"🛑 <b>DRIFT DETECTED</b>\n"
                f"{bad_runs_streak} runs подряд с существенными алертами.\n"
                "🔄 Retraining..."
            )
            from train_model import train as retrain

            retrain()
            bad_runs_streak = 0
            send_telegram("✅ Retrained.")

        save_state(conn, new_last_window_end, bad_runs_streak)
