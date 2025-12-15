import pandas as pd
import numpy as np
import pickle
import os
import sys
import json
import psycopg
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

# --- НАСТРОЙКИ ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(BASE_DIR, '..')))
load_dotenv(os.path.join(BASE_DIR, '..', '.env'))

# Файл, чтобы помнить счетчик между запусками
STATE_FILE = os.path.join(BASE_DIR, "drift_state.json")
CONSECUTIVE_RUNS_LIMIT = 5  # Сколько "плохих" запусков подряд нужно для переобучения

try:
    from scripts.db_config import DB_CONFIG
    from scripts.train_model import train as train_model_emergency
except ImportError:
    # Фолбек для прямого запуска
    from db_config import DB_CONFIG
    from train_model import train as train_model_emergency

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MODEL_FILENAME = "model_baseline_v1.pkl"
MODEL_VERSION = "baseline_v1"

# Признаки (должны совпадать с train_model.py)
LOG_FEATURES = ['shared_read_per_call', 'temp_read_per_call', 'ms_per_row']
OTHER_NUM_FEATURES = ['calls_per_sec', 'cache_miss_ratio', 'temp_share', 'read_blks_per_row', 'exec_time_per_call_ms', 'rows_per_call', 'wal_bytes_per_call']
LEX_FEATURES = ['query_len_norm_chars', 'num_tokens', 'num_joins', 'num_where', 'num_group_by', 'num_order_by', 'has_write', 'has_ddl']
ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES

# --- РАБОТА С СОСТОЯНИЕМ (СЧЕТЧИК) ---
def load_state():
    if not os.path.exists(STATE_FILE):
        return {"bad_runs_streak": 0}
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"bad_runs_streak": 0}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

# --- УТИЛИТЫ ---
def send_telegram_msg(text):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ Telegram config missing") 
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        if len(text) > 4000: text = text[:4000] + "..."
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"❌ Failed to send Telegram: {e}")

def load_model():
    path = os.path.abspath(MODEL_FILENAME)
    if not os.path.exists(path):
        print("⚠️ Модель не найдена! Обучаем новую...")
        train_model_emergency()
    with open(path, 'rb') as f:
        return pickle.load(f)

def get_unscored_data():
    conn_str = f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    # Берем ВСЕ новые данные, которые еще не оценены
    query = f"""
    SELECT v.*
    FROM monitoring.features_with_lex v
    LEFT JOIN monitoring.anomaly_scores s
      ON s.model_version = '{MODEL_VERSION}'
     AND s.window_end = v.window_end
     AND s.dbid = v.dbid AND s.userid = v.userid AND s.queryid = v.queryid
    WHERE s.window_end IS NULL
    ORDER BY v.window_end ASC
    LIMIT 2000; 
    """
    try:
        return pd.read_sql(query, conn_str)
    except Exception as e:
        print(f"Ошибка БД: {e}")
        return pd.DataFrame()

def save_scores(df_results):
    query = """
    INSERT INTO monitoring.anomaly_scores (
        window_start, window_end, dbid, userid, queryid,
        model_version, anomaly_score, is_anomaly, reason, scored_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (model_version, window_end, dbid, userid, queryid) DO NOTHING;
    """
    records = []
    now_ts = datetime.now(timezone.utc)
    for _, row in df_results.iterrows():
        records.append((
            row['window_start'], row['window_end'], row['dbid'], row['userid'], row['queryid'],
            MODEL_VERSION, float(row['anomaly_score']), bool(row['is_anomaly']),
            json.dumps(row.get('reason_json', {})), now_ts
        ))
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, records)
            conn.commit()

def process_alerts(anomalies_df):
    SCORE_THRESHOLD = 0.0 
    alerts_sent = 0
    
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for _, row in anomalies_df.iterrows():
                if row['anomaly_score'] > SCORE_THRESHOLD: continue

                qid = row['queryid']
                try:
                    cur.execute("SELECT query_text FROM monitoring.query_lex_features WHERE queryid = %s LIMIT 1", (qid,))
                    res = cur.fetchone()
                    query_text = res[0] if res else "TEXT NOT FOUND"
                except: query_text = "ERR"

                if "monitoring." in str(query_text).lower(): continue

                msg = (
                    f"🚨 <b>ANOMALY DETECTED</b>\n"
                    f"<b>Score:</b> {row['anomaly_score']:.3f}\n"
                    f"SQL: <code>{str(query_text)[:200].replace('<','&lt;')}</code>" 
                )
                send_telegram_msg(msg)
                alerts_sent += 1
                
    return alerts_sent

# --- ГЛАВНАЯ ФУНКЦИЯ ---
def detect():
    print(f"--- ЗАПУСК ДЕТЕКТОРА ({datetime.now().strftime('%H:%M:%S')}) ---")
    
    # 1. Загружаем данные
    new_df = get_unscored_data()
    if new_df.empty:
        print("💤 Нет новых данных для анализа.")
        return

    print(f"📊 Обработка {len(new_df)} новых окон...")
    
    # 2. Предикт
    model = load_model()
    df_clean = new_df.copy()
    df_clean[ALL_FEATURES] = df_clean[ALL_FEATURES].fillna(0)
    
    X = df_clean[ALL_FEATURES]
    new_df['is_anomaly'] = (model.predict(X) == -1)
    new_df['anomaly_score'] = model.decision_function(X)
    
    # 3. Сохраняем оценки в базу
    save_scores(new_df)
    
    # 4. Анализ результатов для счетчика
    anomalies = new_df[new_df['is_anomaly'] == True]
    
    state = load_state()
    current_streak = state.get("bad_runs_streak", 0)
    
    if not anomalies.empty:
        # В этом запуске ЕСТЬ аномалии
        alerts_count = process_alerts(anomalies)
        
        if alerts_count > 0:
            current_streak += 1
            print(f"⚠️  В этом запуске найдены аномалии. Streak: {current_streak}/{CONSECUTIVE_RUNS_LIMIT}")
        else:
            # Аномалии были системные (monitoring.), счетчик не трогаем или сбрасываем? 
            # Допустим, если пользовательских алертов нет, то и паники нет.
            print("ℹ️ Аномалии только системные, счетчик не увеличиваем.")
            current_streak = 0 # Сброс, так как атаки нет
    else:
        # В этом запуске ВСЁ ЧИСТО
        if current_streak > 0:
            print("✅ Нагрузка нормализовалась. Сброс счетчика.")
        current_streak = 0

    # 5. Проверка условия ДРЕЙФА
    if current_streak >= CONSECUTIVE_RUNS_LIMIT:
        print(f"🛑 ДРЕЙФ ПОДТВЕРЖДЕН! ({current_streak} запусков подряд с аномалиями)")
        send_telegram_msg(f"🛑 <b>SYSTEM DRIFT DETECTED</b>\n{current_streak} checks in a row failed.\n🔄 Starting Retraining...")
        
        # Переобучение
        train_model_emergency()
        
        # Сброс счетчика после переобучения
        current_streak = 0
        send_telegram_msg("✅ Model successfully retrained to new data.")

    # 6. Сохраняем состояние
    state["bad_runs_streak"] = current_streak
    save_state(state)

if __name__ == "__main__":
    detect()