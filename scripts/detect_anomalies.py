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

STATE_FILE = os.path.join(BASE_DIR, "drift_state.json")
CONSECUTIVE_RUNS_LIMIT = 5 

try:
    from scripts.db_config import DB_CONFIG
    from scripts.train_model import train as train_model_emergency
except ImportError:
    from db_config import DB_CONFIG
    from train_model import train as train_model_emergency

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MODEL_FILENAME = "model_baseline_v1.pkl"
MODEL_VERSION = "baseline_v1"

# Признаки
LOG_FEATURES = ['shared_read_per_call', 'temp_read_per_call', 'ms_per_row']
OTHER_NUM_FEATURES = ['calls_per_sec', 'cache_miss_ratio', 'temp_share', 'read_blks_per_row', 'exec_time_per_call_ms', 'rows_per_call', 'wal_bytes_per_call']
LEX_FEATURES = ['query_len_norm_chars', 'num_tokens', 'num_joins', 'num_where', 'num_group_by', 'num_order_by', 'has_write', 'has_ddl']
ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES

FEATURES_TO_EXTRACT = ['exec_time_per_call_ms', 'rows_per_call', 'shared_read_per_call', 'wal_bytes_per_call']

SYSTEM_KEYWORDS = [
    'pg_catalog', 'information_schema', 'pg_toast', 'pg_stat_statements',
    'monitoring.', 'set application_name', 'show transaction isolation level',
    'begin', 'commit', 'rollback'
]

def is_system_query(text):
    if not isinstance(text, str): return False
    text_lower = text.lower()
    for kw in SYSTEM_KEYWORDS:
        if kw in text_lower: return True
    if len(text_lower.strip()) < 10 and ('set' in text_lower or 'show' in text_lower):
        return True
    return False

# --- ФУНКЦИИ ---
def get_username(conn, userid):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT rolname FROM pg_roles WHERE oid = %s", (userid,))
            res = cur.fetchone()
            return res[0] if res else f"Unknown({userid})"
    except:
        return f"Unknown({userid})"

def load_state():
    if not os.path.exists(STATE_FILE): return {"bad_runs_streak": 0}
    try:
        with open(STATE_FILE, 'r') as f: return json.load(f)
    except: return {"bad_runs_streak": 0}

def save_state(state):
    with open(STATE_FILE, 'w') as f: json.dump(state, f)

def send_telegram_msg(text):
    if not TG_TOKEN or not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        if len(text) > 4000: text = text[:4000] + "..."
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e: print(f"❌ TG Error: {e}")

def load_model():
    path = os.path.abspath(MODEL_FILENAME)
    if not os.path.exists(path):
        train_model_emergency()
    with open(path, 'rb') as f: return pickle.load(f)

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
    LIMIT 2000; 
    """
    try: return pd.read_sql(query, conn_str)
    except Exception: return pd.DataFrame()

def save_scores(df_results):
    # Вставка данных (предполагаем, что таблица уже правильная благодаря boot.py)
    query = """
    INSERT INTO monitoring.anomaly_scores (
        window_start, window_end, dbid, userid, queryid,
        model_version, anomaly_score, is_anomaly, reason, scored_at,
        exec_time_ms, rows_cnt, disk_read, wal_bytes
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (model_version, window_end, dbid, userid, queryid) DO NOTHING;
    """
    records = []
    now_ts = datetime.now(timezone.utc)
    
    for _, row in df_results.iterrows():
        reason_dict = row.get('reason_json', {})
        reason_str = json.dumps(reason_dict)
        
        exec_t = float(reason_dict.get('exec_time_per_call_ms', 0))
        rows_c = float(reason_dict.get('rows_per_call', 0))
        disk_r = float(reason_dict.get('shared_read_per_call', 0))
        wal_b  = float(reason_dict.get('wal_bytes_per_call', 0))

        records.append((
            row['window_start'], row['window_end'], row['dbid'], row['userid'], row['queryid'],
            MODEL_VERSION, float(row['anomaly_score']), bool(row['is_anomaly']),
            reason_str, now_ts,
            exec_t, rows_c, disk_r, wal_b
        ))
        
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, records)
            conn.commit()

def process_alerts(anomalies_df):
    SCORE_THRESHOLD = 0.0 
    real_alerts_sent = 0
    
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

                if is_system_query(query_text): continue

                username = get_username(conn, row['userid'])
                r_json = row.get('reason_json', {})
                metrics_parts = []
                
                t = r_json.get('exec_time_per_call_ms', 0)
                metrics_parts.append(f"⏱ {t/1000:.2f}s" if t > 1000 else f"⏱ {t:.1f}ms")
                
                r = r_json.get('rows_per_call', 0)
                metrics_parts.append(f"🔢 {int(r)} rows")
                
                blk = r_json.get('shared_read_per_call', 0)
                if blk > 0: metrics_parts.append(f"💾 {int(blk)} blks")

                metrics_str = " | ".join(metrics_parts)

                msg = (
                    f"🚨 <b>ANOMALY DETECTED</b>\n"
                    f"👤 <b>User:</b> {username}\n" 
                    f"<b>Score:</b> {row['anomaly_score']:.3f}\n"
                    f"{metrics_str}\n"
                    f"--------------------------\n"
                    f"SQL: <code>{str(query_text)[:200].replace('<','&lt;')}</code>" 
                )
                print(f"🚀 Alert sent for User {username} (Score {row['anomaly_score']:.3f})")
                send_telegram_msg(msg)
                real_alerts_sent += 1
                
    return real_alerts_sent

def detect():
    # 1. Загрузка
    new_df = get_unscored_data()
    if new_df.empty: return

    print(f"📊 Обработка {len(new_df)} новых окон...")
    
    # 2. Предикт
    model = load_model()
    df_clean = new_df.copy()
    df_clean[ALL_FEATURES] = df_clean[ALL_FEATURES].fillna(0)
    
    X = df_clean[ALL_FEATURES]
    new_df['is_anomaly'] = (model.predict(X) == -1)
    new_df['anomaly_score'] = model.decision_function(X)
    
    # Подготовка JSON с причинами
    new_df['reason_json'] = new_df[FEATURES_TO_EXTRACT].apply(lambda x: x.to_dict(), axis=1)
    
    # 3. Сохранение
    save_scores(new_df)
    
    # 4. Алерты
    anomalies = new_df[new_df['is_anomaly'] == True]
    state = load_state()
    current_streak = state.get("bad_runs_streak", 0)
    
    if not anomalies.empty:
        real_alerts_count = process_alerts(anomalies)
        if real_alerts_count > 0:
            current_streak += 1
            print(f"⚠️ Аномалии! Streak: {current_streak}/{CONSECUTIVE_RUNS_LIMIT}")
        else:
            print("ℹ️ Игнор системных.")
            current_streak = 0
    else:
        if current_streak > 0: print("✅ Сброс счетчика.")
        current_streak = 0

    # 5. Дрейф
    if current_streak >= CONSECUTIVE_RUNS_LIMIT:
        print(f"🛑 ДРЕЙФ ПОДТВЕРЖДЕН!")
        send_telegram_msg(f"🛑 <b>DRIFT DETECTED</b>\n{current_streak} failures.\n🔄 Retraining...")
        train_model_emergency()
        current_streak = 0
        send_telegram_msg("✅ Retrained.")

    state["bad_runs_streak"] = current_streak
    save_state(state)

if __name__ == "__main__":
    detect()