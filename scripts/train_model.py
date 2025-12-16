import os
import pickle
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

load_dotenv()

try:
    from scripts.db_config import DB_CONFIG
except Exception:
    from db_config import DB_CONFIG

try:
    from detector_alerts import is_system_query
except Exception:
    from scripts.detector_alerts import is_system_query

try:
    from detector_features import (
        ALL_FEATURES,
        coerce_features_df,
        prepare_model_features_df,
    )
except Exception:
    from scripts.detector_features import (
        ALL_FEATURES,
        coerce_features_df,
        prepare_model_features_df,
    )

MODEL_FILENAME = os.getenv("MODEL_FILE", "model_baseline_v1.pkl")
_cont = (os.getenv("MODEL_CONTAMINATION", "0.01") or "0.01").strip().lower()
MODEL_CONTAMINATION = "auto" if _cont == "auto" else float(_cont)
MODEL_N_ESTIMATORS = int(os.getenv("MODEL_N_ESTIMATORS", "200"))

MODEL_MIN_ROWS = int(os.getenv("MODEL_MIN_ROWS", "500"))
MODEL_MIN_QUERYIDS = int(os.getenv("MODEL_MIN_QUERYIDS", "10"))
MODEL_MAX_SAMPLES_PER_QUERYID = int(os.getenv("MODEL_MAX_SAMPLES_PER_QUERYID", "50"))

MODEL_ALERT_QUANTILE = float(os.getenv("MODEL_ALERT_QUANTILE", "0.002"))


def load_data():
    conn_str = (
        f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    query = """
    SELECT f.*
    FROM monitoring.features_with_lex f
    WHERE f.query_text IS NOT NULL;
    """

    try:
        df = pd.read_sql(query, conn_str)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    qt = df.get("query_text")
    if qt is None:
        return pd.DataFrame()

    mask = qt.notna() & ~qt.apply(is_system_query)
    return df[mask].copy()


def train():
    df = load_data()
    if df.empty:
        raise RuntimeError(
            "No training data: monitoring.features_with_lex is empty after filtering."
        )

    df = coerce_features_df(df)
    df = df.dropna(subset=ALL_FEATURES)

    df = df.sample(frac=1.0, random_state=42)
    df = df.groupby(["dbid", "userid", "queryid"], group_keys=False).head(
        MODEL_MAX_SAMPLES_PER_QUERYID
    )

    n_queryids = int(df[["dbid", "userid", "queryid"]].drop_duplicates().shape[0])
    if len(df) < MODEL_MIN_ROWS or n_queryids < MODEL_MIN_QUERYIDS:
        raise RuntimeError(
            f"Not enough training data: rows={len(df)} (min {MODEL_MIN_ROWS}), "
            f"unique_queryids={n_queryids} (min {MODEL_MIN_QUERYIDS})."
        )

    X = prepare_model_features_df(df)

    pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
            ("scaler", StandardScaler()),
            (
                "iso_forest",
                IsolationForest(
                    n_estimators=MODEL_N_ESTIMATORS,
                    contamination=MODEL_CONTAMINATION,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    pipeline.fit(X)

    train_scores = pipeline.decision_function(X)
    auto_threshold = float(np.quantile(train_scores, MODEL_ALERT_QUANTILE))

    score_df = df[["dbid", "userid", "queryid"]].copy()
    score_df["score"] = train_scores
    thresholds_by_query = {}
    for (dbid, userid, queryid), g in score_df.groupby(["dbid", "userid", "queryid"]):
        thresholds_by_query[(int(dbid), int(userid), int(queryid))] = float(
            np.quantile(g["score"].to_numpy(), MODEL_ALERT_QUANTILE)
        )

    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump(
            {
                "pipeline": pipeline,
                "threshold": auto_threshold,
                "thresholds_by_query": thresholds_by_query,
                "trained_at": datetime.now(timezone.utc).isoformat(),
                "n_rows": int(len(df)),
                "n_queryids": n_queryids,
                "alert_quantile": MODEL_ALERT_QUANTILE,
                "features": list(ALL_FEATURES),
            },
            f,
        )


if __name__ == "__main__":
    train()
