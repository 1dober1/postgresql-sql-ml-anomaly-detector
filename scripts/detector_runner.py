import os
import pickle
from datetime import datetime, timezone

import pandas as pd

from detector_features import (
    ALL_FEATURES,
    coerce_features_df,
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
SCORE_THRESHOLD = float(os.getenv("ALERT_SCORE_THRESHOLD", "0.0"))

ALERT_METRICS = [
    "exec_time_per_call_ms",
    "rows_per_call",
    "shared_read_per_call",
    "wal_bytes_per_call",
]


def load_model_or_train():
    if os.path.exists(MODEL_FILENAME):
        with open(MODEL_FILENAME, "rb") as f:
            return pickle.load(f)
    from train_model import train as train_emergency

    train_emergency()
    with open(MODEL_FILENAME, "rb") as f:
        return pickle.load(f)


def run_once():
    model = load_model_or_train()

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

        mask = df_all["query_text"].notna() & ~df_all["query_text"].apply(is_system_query)
        df = df_all[mask].copy()

        if df.empty:
            save_state(conn, new_last_window_end, bad_runs_streak)
            return

        X = df[ALL_FEATURES].copy()
        preds = model.predict(X)
        scores = model.decision_function(X)

        df["is_anomaly"] = (preds == -1)
        df["anomaly_score"] = scores

        df_anom = df[df["is_anomaly"]].copy()
        now_ts = datetime.now(timezone.utc)

        insert_rows = []
        for _, r in df_anom.iterrows():
            features = build_features_json(r)
            insert_rows.append((
                r["window_start"],
                r["window_end"],
                r["dbid"],
                r["userid"],
                r["queryid"],
                MODEL_VERSION,
                float(r["anomaly_score"]),
                dumps_json(features),
                now_ts
            ))
        insert_anomaly_rows(conn, insert_rows)

        real_alerts_sent = 0
        if not df_anom.empty:
            user_map = fetch_usernames_batch(conn, set(df_anom["userid"].tolist()))

            for _, r in df_anom.iterrows():
                score = float(r["anomaly_score"])
                if score > SCORE_THRESHOLD:
                    continue

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
            real_alerts_sent=real_alerts_sent,
            consecutive_limit=CONSECUTIVE_RUNS_LIMIT,
        )

        if drift:
            send_telegram(
                f"🛑 <b>DRIFT DETECTED</b>\n{bad_runs_streak} runs подряд с алертами.\n🔄 Retraining..."
            )
            from train_model import train as retrain

            retrain()
            bad_runs_streak = 0
            send_telegram("✅ Retrained.")

        save_state(conn, new_last_window_end, bad_runs_streak)
