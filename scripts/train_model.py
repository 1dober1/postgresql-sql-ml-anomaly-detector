try:
    from db_config import DB_CONFIG
except Exception:
    # Allow running as `python -m scripts.train_model` or `python scripts/train_model.py`
    from scripts.db_config import DB_CONFIG
import numpy as np
import pickle
import os
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline
import pandas as pd

MODEL_FILENAME = "model_baseline_v1.pkl"
MODEL_VERSION = "baseline_v1"


LOG_FEATURES = [
    'shared_read_per_call', 'temp_read_per_call', 'ms_per_row'
]

# Сюда переносим главные индикаторы тяжести, чтобы они шли "как есть" (RAW)
OTHER_NUM_FEATURES = [
    'calls_per_sec', 'cache_miss_ratio',
    'temp_share', 'read_blks_per_row',
    'exec_time_per_call_ms', # <--- Перенесли сюда
    'rows_per_call',         # <--- Перенесли сюда
    'wal_bytes_per_call'     # <--- Перенесли сюда
]

LEX_FEATURES = [
    'query_len_norm_chars', 'num_tokens', 'num_joins',
    'num_where', 'num_group_by', 'num_order_by',
    'has_write', 'has_ddl'
]

ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES


def get_training_data():
    """Загружаем данные за последние 7 дней"""
    conn_str = f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

    query = """
    SELECT * FROM monitoring.features_with_lex
    WHERE window_start >= now() - interval '7 days';
    """

    try:
        # pandas сам откроет и закроет соединение
        df = pd.read_sql(query, conn_str)
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame()


def train():
    df = get_training_data()

    if df.empty:
        print("Нет данных для обучения. Завершение.")
        return

    print("--- [2] Препроцессинг ---")
    # 1. Заполняем пропуски
    # Если нет лексики -> считаем 0 (простой запрос)
    df[LEX_FEATURES] = df[LEX_FEATURES].fillna(0)
    # Если метрики NULL -> 0
    df[LOG_FEATURES + OTHER_NUM_FEATURES] = df[LOG_FEATURES +
                                               OTHER_NUM_FEATURES].fillna(0)

    # 2. Собираем Pipeline
    # ColumnTransformer применит log1p только к нужным колонкам, остальные пропустит
    preprocessor = ColumnTransformer(
        transformers=[
            ('log', FunctionTransformer(np.log1p), LOG_FEATURES),
            ('pass', 'passthrough', OTHER_NUM_FEATURES + LEX_FEATURES)
        ]
    )

    # 3. Модель
    # n_estimators=100: количество деревьев (стандарт)
    # contamination=0.01: ожидаем ~1% аномалий. Можно настроить 'auto', если не уверены.
    # n_jobs=-1: используем все ядра процессора
    model_pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('iso_forest', IsolationForest(
            n_estimators=100,
            contamination=0.01,
            random_state=42,
            n_jobs=-1
        ))
    ])

    print("--- [3] Обучение модели (Isolation Forest) ---")
    X = df[ALL_FEATURES]
    model_pipeline.fit(X)

    print("--- [4] Сохранение модели ---")
    # Сохраняем объект pipeline целиком (вместе с препроцессингом!)
    # Это важно, чтобы при детекции новые данные проходили те же трансформации.
    with open(MODEL_FILENAME, 'wb') as f:
        pickle.dump(model_pipeline, f)

    print(f"Модель сохранена в файл: {os.path.abspath(MODEL_FILENAME)}")
    print(f"Версия модели: {MODEL_VERSION}")


if __name__ == "__main__":
    train()
