import pandas as pd
import numpy as np
import pickle
import os
import sys
import json
import psycopg
import requests
from datetime import datetime, timezone
from scipy.stats import ks_2samp 
from dotenv import load_dotenv  

# --- [2] ЗАГРУЖАЕМ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(BASE_DIR, '..')))
load_dotenv(os.path.join(BASE_DIR, '..', '.env')) 

STATE_FILE = os.path.join(BASE_DIR, "drift_state.json")

try:
    from scripts.db_config import DB_CONFIG
    from scripts.train_model import train as train_model_emergency
except ImportError:
    from scripts.db_config import DB_CONFIG
    from scripts.train_model import train as train_model_emergency

# --- ТЕЛЕГРАМ КОНФИГ ---
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- ML КОНФИГ ---
MODEL_FILENAME = "model_baseline_v1.pkl"
MODEL_VERSION = "baseline_v1"
DRIFT_CONSECUTIVE_LIMIT = 5
KS_P_VALUE_THRESHOLD = 0.01
MIN_BATCH_SIZE_FOR_DRIFT = 50

DRIFT_MONITOR_FEATURES = ['exec_time_per_call_ms', 'rows_per_call', 'shared_read_per_call', 'wal_bytes_per_call', 'calls_per_sec']
LOG_FEATURES = ['exec_time_per_call_ms', 'rows_per_call', 'shared_read_per_call', 'temp_read_per_call', 'wal_bytes_per_call', 'ms_per_row']
OTHER_NUM_FEATURES = ['calls_per_sec', 'cache_miss_ratio', 'temp_share', 'read_blks_per_row']
LEX_FEATURES = ['query_len_norm_chars', 'num_tokens', 'num_joins', 'num_where', 'num_group_by', 'num_order_by', 'has_write', 'has_ddl']
ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES

def send_telegram_msg(text):
    if not TG_TOKEN or not TG_CHAT_ID:
        # Пишем в консоль, чтобы ты видел в логах, если токена нет
        print(f"⚠️ Telegram config missing (Token={bool(TG_TOKEN)}, ID={bool(TG_CHAT_ID)})") 
        return
    
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        if len(text) > 4000:
            text = text[:4000] + "... (truncated)"
        
        # Убираем parse_mode="HTML" если есть спецсимволы, которые ломают разметку
        # Но пока оставим, т.к. красиво.
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
        print("✅ Telegram alert sent!")
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

def process_alerts(anomalies_df):
    if anomalies_df.empty:
        print("DEBUG: DataFrame аномалий пуст.")
        return

    print(f"--- ОБРАБОТКА АЛЕРТОВ ({len(anomalies_df)} шт) ---")
    

    SCORE_THRESHOLD = 0.0 
    
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for _, row in anomalies_df.iterrows():
                qid = row['queryid']
                score = row['anomaly_score']
                
                print(f"DEBUG: Проверка QueryID {qid}, Score {score:.4f}...")

                # 1. Проверка порога
                if score > SCORE_THRESHOLD:
                    print(f"   -> ПРОПУСК: Score {score:.4f} выше порога {SCORE_THRESHOLD}")
                    continue
                
                # 2. Получение текста (упрощенно для теста)
                try:
                    cur.execute("SELECT query_text FROM monitoring.query_lex_features WHERE queryid = %s", (qid,))
                    res = cur.fetchone()
                    if not res:
                         cur.execute("SELECT query_text FROM monitoring.pgss_snapshots_raw WHERE queryid = %s ORDER BY snapshot_ts DESC LIMIT 1", (qid,))
                         res = cur.fetchone()
                    query_text = res[0] if res else "TEXT NOT FOUND"
                except:
                    query_text = "ERROR FETCHING TEXT"

                # 3. Фильтр мониторинга
                if "monitoring." in str(query_text).lower():
                    print("   -> ПРОПУСК: Системный запрос мониторинга")
                    continue

                # 4. ОТПРАВКА
                print(f"   -> ОТПРАВКА В TELEGRAM! (Score: {score:.4f})")
                
                safe_query = str(query_text).replace("<", "&lt;").replace(">", "&gt;")
                msg = (
                    f"🚨 <b>ANOMALY DETECTED</b> 🚨\n\n"
                    f"<b>Score:</b> {score:.4f}\n"
                    f"<b>QueryID:</b> <code>{qid}</code>\n"
                    f"<b>SQL:</b> <code>{safe_query[:200]}</code>" 
                )
                send_telegram_msg(msg)

def get_drift_counter():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                return data.get('count', 0)
        except:
            return 0
    return 0

def update_drift_counter(count):
    with open(STATE_FILE, 'w') as f:
        json.dump({'count': count, 'last_updated': str(datetime.now())}, f)

def load_model():
    path = os.path.abspath(MODEL_FILENAME)
    if not os.path.exists(path):
        train_model_emergency()
    with open(path, 'rb') as f:
        return pickle.load(f)

def get_unscored_data():
    conn_str = f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    query = f"""
    SELECT v.*
    FROM monitoring.features_with_lex v
    LEFT JOIN monitoring.anomaly_scores s
      ON s.model_version = '{MODEL_VERSION}'
     AND s.window_end = v.window_end
     AND s.dbid = v.dbid AND s.userid = v.userid AND s.queryid = v.queryid
    WHERE s.window_end IS NULL
    ORDER BY v.window_end ASC
    LIMIT 5000; 
    """
    try:
        return pd.read_sql(query, conn_str)
    except Exception as e:
        print(f"Ошибка БД (unscored): {e}")
        return pd.DataFrame()

def get_reference_data(limit=2000):
    conn_str = f"postgresql+psycopg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    query = f"""
    SELECT v.*
    FROM monitoring.features_with_lex v
    JOIN monitoring.anomaly_scores s
      ON s.model_version = '{MODEL_VERSION}'
     AND s.window_end = v.window_end
     AND s.dbid = v.dbid AND s.userid = v.userid AND s.queryid = v.queryid
    -- WHERE s.is_anomaly = false
    ORDER BY s.scored_at DESC
    LIMIT {limit};
    """
    try:
        return pd.read_sql(query, conn_str)
    except Exception as e:
        print(f"Ошибка БД (reference): {e}")
        return pd.DataFrame()

def check_distribution_drift(new_df, ref_df):
    if ref_df.empty or new_df.empty:
        return False, []
    drifted_features = []
    n_df = new_df.fillna(0)
    r_df = ref_df.fillna(0)
    for feature in DRIFT_MONITOR_FEATURES:
        data_new = n_df[feature]
        data_ref = r_df[feature]
        statistic, p_value = ks_2samp(data_ref, data_new)
        if p_value < KS_P_VALUE_THRESHOLD:
            drifted_features.append(feature)
    is_drift = len(drifted_features) >= 2
    return is_drift, drifted_features

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
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, records)
                conn.commit()
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

def predict_batch(model, df):
    df_clean = df.copy()
    df_clean[LEX_FEATURES] = df_clean[LEX_FEATURES].fillna(0)
    df_clean[LOG_FEATURES + OTHER_NUM_FEATURES] = df_clean[LOG_FEATURES + OTHER_NUM_FEATURES].fillna(0)
    X = df_clean[ALL_FEATURES]
    return model.predict(X), model.decision_function(X)

def detect():
    model_pipeline = load_model()
    
    new_df = get_unscored_data()
    if new_df.empty:
        print("Нет новых данных.")
        return

    ref_df = get_reference_data()
    current_counter = get_drift_counter()
    drift_detected = False
    drift_details = []
    print(f"DEBUG: New rows={len(new_df)}, Ref rows={len(ref_df)}") 
    if len(new_df) >= MIN_BATCH_SIZE_FOR_DRIFT and not ref_df.empty:
        drift_detected, drift_details = check_distribution_drift(new_df, ref_df)
        if drift_detected:
            print(f"⚠️ ОБНАРУЖЕН ДРЕЙФ! {drift_details}")
            current_counter += 1
            update_drift_counter(current_counter)
            send_telegram_msg(f"⚠️ <b>DRIFT DETECTED</b> ({current_counter}/5)\nFeatures: {', '.join(drift_details)}")
        else:
            if current_counter > 0:
                update_drift_counter(0)
    else:
        print(f"DEBUG: Skip Drift Check. Need {MIN_BATCH_SIZE_FOR_DRIFT} rows, ref not empty.")
    
    if drift_detected and current_counter >= DRIFT_CONSECUTIVE_LIMIT:
        print("🛑 ЗАПУСК ПЕРЕОБУЧЕНИЯ...")
        send_telegram_msg("🛑 <b>RETRAINING MODEL</b> due to stable drift.")
        try:
            train_model_emergency()
            model_pipeline = load_model()
            update_drift_counter(0)
        except Exception as e:
            send_telegram_msg(f"❌ Retraining failed: {e}")

    print("--- Оценка аномалий ---")
    preds, scores = predict_batch(model_pipeline, new_df)
    
    new_df['anomaly_score'] = scores
    new_df['is_anomaly'] = (preds == -1)
    
    save_scores(new_df)
    print(f"Обработано {len(new_df)} окон.")
    
    # --- [4] ОТПРАВКА ---
    anomalies_found = new_df[new_df['is_anomaly'] == True]
    if not anomalies_found.empty:
        process_alerts(anomalies_found)

if __name__ == "__main__":
    detect()