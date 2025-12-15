import os
import sys
import pickle
import pandas as pd

from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from dotenv import load_dotenv

from scripts.detector_features import ALL_FEATURES

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(BASE_DIR, "..")))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

try:
    from scripts.db_config import DB_CONFIG
except ImportError:
    from db_config import DB_CONFIG

MODEL_FILENAME = "model_baseline_v1.pkl"


def load_data():
    conn_str = (
        f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    query = """
    SELECT f.*
    FROM monitoring.features_with_lex f
    WHERE
          f.query_text IS NULL
       OR (
          f.query_text NOT ILIKE '%%pg_catalog%%'
      AND f.query_text NOT ILIKE '%%information_schema%%'
      AND f.query_text NOT ILIKE '%%pg_toast%%'
      AND f.query_text NOT ILIKE '%%pg_stat_statements%%'
      AND f.query_text NOT ILIKE '%%monitoring.%%'
       );
    """
    try:
        return pd.read_sql(query, conn_str)
    except Exception:
        return pd.DataFrame()


def train():
    df = load_data()
    if df.empty or len(df) < 10:
        return

    for c in ALL_FEATURES:
        if c not in df.columns:
            df[c] = 0
    X = df[ALL_FEATURES].fillna(0)

    pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
            ("scaler", StandardScaler()),
            (
                "iso_forest",
                IsolationForest(
                    n_estimators=200, contamination=0.01, random_state=42, n_jobs=-1
                ),
            ),
        ]
    )

    pipeline.fit(X)

    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump(pipeline, f)


if __name__ == "__main__":
    train()
