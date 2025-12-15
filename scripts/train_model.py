import pandas as pd
import numpy as np
import pickle
import os
import sys
import psycopg
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from dotenv import load_dotenv

# --- КОНФИГ ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(BASE_DIR, '..')))
load_dotenv(os.path.join(BASE_DIR, '..', '.env'))

try:
    from scripts.db_config import DB_CONFIG
except ImportError:
    from db_config import DB_CONFIG

MODEL_FILENAME = "model_baseline_v1.pkl"

# Признаки
LOG_FEATURES = ['shared_read_per_call', 'temp_read_per_call', 'ms_per_row']
OTHER_NUM_FEATURES = ['calls_per_sec', 'cache_miss_ratio', 'temp_share', 'read_blks_per_row', 'exec_time_per_call_ms', 'rows_per_call', 'wal_bytes_per_call']
LEX_FEATURES = ['query_len_norm_chars', 'num_tokens', 'num_joins', 'num_where', 'num_group_by', 'num_order_by', 'has_write', 'has_ddl']
ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES

def load_data():
    print("--- [1] Загрузка данных для обучения ---")
    conn_str = f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    
    # ФИЛЬТРАЦИЯ: Используем двойные %% для экранирования в Python
    query = """
    SELECT f.* FROM monitoring.features_with_lex f
    JOIN monitoring.query_lex_features l ON f.queryid = l.queryid
    WHERE 
          l.query_text NOT ILIKE '%%pg_catalog%%' 
      AND l.query_text NOT ILIKE '%%information_schema%%'
      AND l.query_text NOT ILIKE '%%pg_toast%%'
      AND l.query_text NOT ILIKE '%%pg_stat_statements%%'
      AND l.query_text NOT ILIKE '%%monitoring.%%'
    """
    
    try:
        df = pd.read_sql(query, conn_str)
        print(f"✅ Загружено строк (Clean Business Data): {len(df)}")
        return df
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        return pd.DataFrame()

def preprocess_data(df):
    print("--- [2] Препроцессинг ---")
    if df.empty:
        return pd.DataFrame()
    
    df_clean = df.copy()
    # Заполняем пропуски нулями
    df_clean[ALL_FEATURES] = df_clean[ALL_FEATURES].fillna(0)
    return df_clean[ALL_FEATURES]

def train():
    df = load_data()
    if df.empty or len(df) < 10:
        print("⚠️ Слишком мало данных для обучения (<10 строк). Пропускаем.")
        return

    X = preprocess_data(df)
    
    print("--- [3] Обучение модели (Isolation Forest) ---")
    # contamination=0.01 значит, что мы готовы считать 1% самых странных данных выбросами
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
        ('scaler', StandardScaler()), 
        ('iso_forest', IsolationForest(
            n_estimators=200, 
            contamination=0.01, 
            random_state=42, 
            n_jobs=-1
        ))
    ])
    
    pipeline.fit(X)
    
    print("--- [4] Сохранение модели ---")
    path = os.path.abspath(MODEL_FILENAME)
    with open(path, 'wb') as f:
        pickle.dump(pipeline, f)
    print(f"✅ Модель сохранена: {path}")

if __name__ == "__main__":
    train()