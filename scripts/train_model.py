import os
import sys
import pickle
import pandas as pd

from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from dotenv import load_dotenv

from detector_features import ALL_FEATURES

try:
    from scripts.db_config import DB_CONFIG
except ImportError:
    from db_config import DB_CONFIG

try:
    from detector_features import ALL_FEATURES
except Exception:
    from scripts.detector_features import ALL_FEATURES
    
try:
    from detector_features import ALL_FEATURES, coerce_features_df
except ImportError:
    from scripts.detector_features import ALL_FEATURES, coerce_features_df

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
sys.path.append(PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

MODEL_FILENAME = os.getenv("MODEL_FILE", "model_baseline_v1.pkl")
MODEL_CONTAMINATION = float(os.getenv("MODEL_CONTAMINATION", "0.01"))
MODEL_N_ESTIMATORS = int(os.getenv("MODEL_N_ESTIMATORS", "200"))


def load_data():
    conn_str = (
        f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    query = """
    SELECT f.*
    FROM monitoring.features_with_lex f
    WHERE f.query_text IS NOT NULL
      -- системные схемы/наши таблицы
      AND f.query_text NOT ILIKE '%%pg_catalog%%'
      AND f.query_text NOT ILIKE '%%information_schema%%'
      AND f.query_text NOT ILIKE '%%pg_toast%%'
      AND f.query_text NOT ILIKE '%%pg_stat_statements%%'
      AND f.query_text NOT ILIKE '%%monitoring.%%'
      -- короткие tx-команды
      AND lower(trim(both ';' from f.query_text)) NOT IN ('begin','commit','end','rollback')
      -- служебные команды
      AND f.query_text NOT ILIKE 'set %%'
      AND f.query_text NOT ILIKE 'show %%'
      AND f.query_text NOT ILIKE 'reset %%'
      -- типичная интроспекция клиентов
      AND f.query_text NOT ILIKE 'select current_schema%%'
      AND f.query_text NOT ILIKE 'select current_database%%'
      AND f.query_text NOT ILIKE 'select current_user%%'
      AND f.query_text NOT ILIKE 'select session_user%%'
      AND f.query_text NOT ILIKE 'select version%%'
      AND f.query_text NOT ILIKE 'select pg_backend_pid%%'
    ;
    """
    
    try:
        return pd.read_sql(query, conn_str)
    except Exception:
        return pd.DataFrame()


def train():
    df = load_data()
    # На стенде может быть мало окон в начале — обучаемся даже на небольшой выборке
    if df.empty:
        return

    df = coerce_features_df(df)  
    df = df.dropna(subset=ALL_FEATURES)
    X = df[ALL_FEATURES]

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

    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump(pipeline, f)


if __name__ == "__main__":
    train()
