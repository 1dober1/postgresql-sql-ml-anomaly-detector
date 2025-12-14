try:
    from db_config import DB_CONFIG
except Exception:
    from scripts.db_config import DB_CONFIG
import numpy as np
import pickle
import os
from sklearn.ensemble import IsolationForest
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline
import pandas as pd

MODEL_FILENAME = "model_baseline_v1.pkl"
MODEL_VERSION = "baseline_v1"

# --- [ВАЖНО] КОНФИГУРАЦИЯ ПРИЗНАКОВ ---
# LOG_FEATURES: Признаки, которые имеют огромный разброс, но НЕ являются главными маркерами твоей атаки.
# Их мы логарифмируем, чтобы сгладить выбросы.
LOG_FEATURES = [
    'shared_read_per_call', 
    'temp_read_per_call', 
    'ms_per_row'
]

# OTHER_NUM_FEATURES: Признаки, которые мы оставляем "КАК ЕСТЬ" (Passthrough).
# Сюда перенесены главные индикаторы тяжести (rows, wal_bytes, time), 
# чтобы модель видела реальный масштаб катастрофы.
OTHER_NUM_FEATURES = [
    'calls_per_sec', 
    'cache_miss_ratio', 
    'temp_share', 
    'read_blks_per_row',
    'exec_time_per_call_ms', # <--- ВАЖНО: Без логарифма
    'rows_per_call',         # <--- ВАЖНО: Без логарифма
    'wal_bytes_per_call'     # <--- ВАЖНО: Без логарифма
]

LEX_FEATURES = [
    'query_len_norm_chars', 'num_tokens', 'num_joins',
    'num_where', 'num_group_by', 'num_order_by',
    'has_write', 'has_ddl'
]

ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES

def get_training_data():
    conn_str = f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    # Берем данные только за последние 24 часа, чтобы не цеплять старый мусор
    query = """
    SELECT * FROM monitoring.features_with_lex
    WHERE window_start >= now() - interval '1 day';
    """
    try:
        df = pd.read_sql(query, conn_str)
        print(f"Загружено строк для обучения: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame()

def train():
    print("--- [1] Загрузка данных ---")
    df = get_training_data()

    if df.empty:
        print("❌ Нет данных для обучения.")
        return

    print("--- [2] Препроцессинг ---")
    df[LEX_FEATURES] = df[LEX_FEATURES].fillna(0)
    df[LOG_FEATURES + OTHER_NUM_FEATURES] = df[LOG_FEATURES + OTHER_NUM_FEATURES].fillna(0)

    preprocessor = ColumnTransformer(
        transformers=[
            ('log', FunctionTransformer(np.log1p), LOG_FEATURES),
            ('pass', 'passthrough', OTHER_NUM_FEATURES + LEX_FEATURES)
        ]
    )

    # Увеличиваем n_estimators для надежности
    # Contamination ставим авто или очень низкий, так как мы обучаемся на ЧИСТЫХ данных
    model_pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('iso_forest', IsolationForest(
            n_estimators=200,    # Больше деревьев = стабильнее скор
            contamination=0.05, # Мы предполагаем, что в обучающей выборке почти нет грязи
            random_state=42,
            n_jobs=-1
        ))
    ])

    print("--- [3] Обучение модели (Isolation Forest) ---")
    X = df[ALL_FEATURES]
    model_pipeline.fit(X)

    print("--- [4] Сохранение модели ---")
    with open(MODEL_FILENAME, 'wb') as f:
        pickle.dump(model_pipeline, f)

    print(f"✅ Модель сохранена: {os.path.abspath(MODEL_FILENAME)}")

if __name__ == "__main__":
    train()