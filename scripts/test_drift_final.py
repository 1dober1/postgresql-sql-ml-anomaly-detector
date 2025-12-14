import time
import os
import sys
import psycopg
import subprocess
import random
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, '..'))
load_dotenv(os.path.join(BASE_DIR, '..', '.env'))

try:
    from scripts.db_config import DB_CONFIG
except ImportError:
    print("Ошибка импорта конфига!")
    sys.exit(1)

PIPELINE_SCRIPT = os.path.join(BASE_DIR, '..', 'run_pipeline.sh')
DETECTOR_SCRIPT = os.path.join(BASE_DIR, 'detect_anomalies.py')

def run_step(cmd_list, name):
    # print(f"   [Exec] {name}...", end=" ", flush=True)
    try:
        subprocess.check_call(cmd_list, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # print("OK")
    except Exception as e:
        print(f"FAIL: {e}")

def generate_normal_load(batch_index):
    """Фаза 1: Генерируем обычные легкие SELECT-ы"""
    print(f"   [Normal Load] Генерируем 60 SELECT-запросов (Batch {batch_index})...")
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        for i in range(60):
            tag = f"norm_{batch_index}_{i}_{random.randint(100,999)}"
            # Быстрые запросы, мало чтений, нет записи
            conn.execute(f"SELECT count(*) FROM pg_class WHERE oid > {i} /* {tag} */")

def generate_drift_load(batch_index):
    """Фаза 2: Генерируем тяжелые INSERT/UPDATE"""
    print(f"   [DRIFT Load] Генерируем 60 INSERT-запросов (Batch {batch_index})...")
    
    init_sql = """
    CREATE TEMP TABLE IF NOT EXISTS drift_test_tbl (id serial, info text);
    TRUNCATE drift_test_tbl;
    """
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        conn.execute(init_sql)
        for i in range(60):
            tag = f"drift_{batch_index}_{i}_{random.randint(1000,9999)}"
            # Запись + генерация данных + WAL
            sql = f"""
            INSERT INTO drift_test_tbl (info) 
            SELECT md5(g::text) FROM generate_series(1, 50) as g
            /* {tag} */
            """
            conn.execute(sql)

def pipeline_cycle():
    # 1. Сбор
    run_step([PIPELINE_SCRIPT], "Collector")
    # 2. Анализ (через питон, чтобы точно отработал)
    run_step([sys.executable, DETECTOR_SCRIPT], "Detector")

def main():
    print("🚀 СТАРТ ФИНАЛЬНОГО ТЕСТА ДРЕЙФА")
    print("-----------------------------------")

    # 1. Очистка
    print("🧹 Сброс статистики...")
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        conn.execute("SELECT pg_stat_statements_reset();")
        # Опционально: можно очистить anomaly_scores, чтобы эталон был чист
        # conn.execute("TRUNCATE TABLE monitoring.anomaly_scores;") 

    # Ставим 50 итераций. Это займет время (50 * ~3-5 сек = ~4 минуты), но даст базу.
    TRAIN_ITERATIONS = 50 
    
    # print(f"\n📦 ФАЗА 1: Набиваем базу нормальными данными ({TRAIN_ITERATIONS} итераций)")
    # for i in range(1, TRAIN_ITERATIONS + 1):
    #     generate_normal_load(i)
        
    #     # Небольшая рандомизация паузы, чтобы "окна" были немного разными по длине
    #     time.sleep(random.uniform(0.5, 1.5)) 
        
    #     pipeline_cycle()
    #     print(f"   ✅ Норма {i}/{TRAIN_ITERATIONS} записана.")

    print("\n⚔️ ФАЗА 2: АТАКА ДРЕЙФА (Запускаем запись)")
    # Теперь у нас в базе ~180 нормальных записей. 
    # Новые пачки по 60 записей будут сильно отличаться от них.
    
    for i in range(1, 7):
        print(f"\n🔁 Дрейф-Итерация {i}/6")
        generate_drift_load(i)
        time.sleep(1)
        pipeline_cycle()
        print(f"   --> Проверяй Телеграм! (Ожидаем {i}/5)")
        time.sleep(5) # Чуть больше пауза, чтобы успеть прочитать

    print("\n🏁 Тест завершен.")

if __name__ == "__main__":
    main()