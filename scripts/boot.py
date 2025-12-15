import time
import os
import sys
import subprocess
import psycopg

# Настройка путей
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, '..'))

# Пытаемся импортировать конфиг
try:
    from scripts.db_config import DB_CONFIG
except ImportError:
    print("❌ Config not found. Check path.")
    sys.exit(1)

MODEL_FILE = "model_baseline_v1.pkl"
PIPELINE_SCRIPT = "./run_pipeline.sh"
TRAIN_SCRIPT = "scripts/train_model.py"
DETECT_SCRIPT = "scripts/detect_anomalies.py"

def wait_for_db():
    """Ждет пока Postgres поднимется"""
    print("⏳ Ожидание базы данных...")
    retries = 30
    while retries > 0:
        try:
            with psycopg.connect(**DB_CONFIG) as conn:
                conn.execute("SELECT 1")
            print("✅ База данных доступна!")
            return
        except Exception:
            time.sleep(2)
            retries -= 1
    print("❌ Не удалось подключиться к БД.")
    sys.exit(1)



def run_training_cycle():
    """Очистка -> pgbench -> Сбор -> Обучение"""
    print("\n🚀 МОДЕЛЬ НЕ НАЙДЕНА. НАЧИНАЕМ ОБУЧЕНИЕ С НУЛЯ.")
    
    # 1. Очистка
    print("🧹 Очистка старых данных...")
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_CONFIG['password']
    
    cmd_reset = [
        "psql", "-h", DB_CONFIG['host'], "-U", DB_CONFIG['user'], "-d", DB_CONFIG['dbname'],
        "-c", "TRUNCATE TABLE monitoring.features_windows, monitoring.pgss_deltas, monitoring.pgss_snapshots_raw RESTART IDENTITY CASCADE; SELECT pg_stat_statements_reset();"
    ]
    subprocess.run(cmd_reset, env=env)

    # 2. Инициализация pgbench
    print("📦 Инициализация pgbench (создание таблиц)...")
    subprocess.run(["pgbench", "-i", "-s", "5", "-h", DB_CONFIG['host'], "-U", DB_CONFIG['user'], DB_CONFIG['dbname']], env=env)

    # 3. Запуск нагрузки в фоне
    print("🔥 Запуск pgbench (Нагрузка на 180 сек)...")
    pgbench_proc = subprocess.Popen(
        ["pgbench", "-h", DB_CONFIG['host'], "-U", DB_CONFIG['user'], "-T", "180", "-c", "4", DB_CONFIG['dbname']],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    # 4. Сбор данных (Параллельно)
    print("📸 Сбор метрик для обучения (20 снимков)...")
    for i in range(20):
        try:
            subprocess.run([PIPELINE_SCRIPT], check=False, stdout=subprocess.DEVNULL)
            sys.stdout.write(f"\r   Progress: {i+1}/20")
            sys.stdout.flush()
        except Exception as e:
            print(f"Error in pipeline: {e}")
        time.sleep(10) # Делаем паузы, чтобы набралась статистика
    
    print("\n✅ Сбор данных завершен.")
    pgbench_proc.wait() # Ждем завершения бенчмарка

    # 5. Обучение
    print("🎓 Обучение модели...")
    subprocess.run([sys.executable, TRAIN_SCRIPT], check=True)
    print("🎉 Модель готова!")

def init_db_structure():
    """Создает расширения и обновляет структуру таблиц"""
    print("🛠 Настройка структуры БД (Extensions & Tables)...")
    try:
        with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
            
            # Базовая таблица
            conn.execute("""
            CREATE TABLE IF NOT EXISTS monitoring.anomaly_scores (
                window_start timestamptz,
                window_end timestamptz,
                dbid oid,
                userid oid,
                queryid bigint,
                model_version text,
                anomaly_score float,
                is_anomaly boolean,
                reason jsonb,
                scored_at timestamptz,
                -- Физические метрики
                exec_time_ms float,
                rows_cnt float,
                disk_read float,
                wal_bytes float,
                -- Лексические метрики (НОВОЕ)
                num_joins int,
                is_write boolean,
                is_ddl boolean,
                
                PRIMARY KEY (model_version, window_end, dbid, userid, queryid)
            );
            """)

            # МИГРАЦИЯ: Добавляем колонки, если их нет
            alter_cmds = [
                "ADD COLUMN IF NOT EXISTS exec_time_ms FLOAT",
                "ADD COLUMN IF NOT EXISTS rows_cnt FLOAT",
                "ADD COLUMN IF NOT EXISTS disk_read FLOAT",
                "ADD COLUMN IF NOT EXISTS wal_bytes FLOAT",
                # НОВЫЕ КОЛОНКИ:
                "ADD COLUMN IF NOT EXISTS num_joins INT",
                "ADD COLUMN IF NOT EXISTS is_write BOOLEAN",
                "ADD COLUMN IF NOT EXISTS is_ddl BOOLEAN"
            ]
            for cmd in alter_cmds:
                try:
                    conn.execute(f"ALTER TABLE monitoring.anomaly_scores {cmd}")
                except Exception:
                    pass 

            print("✅ Структура таблиц обновлена.")
            
    except Exception as e:
        print(f"⚠️ Ошибка инициализации БД: {e}")
    
def main_loop():
    """Бесконечный цикл работы"""
    print("\n🛡 СИСТЕМА ЗАПУЩЕНА В БОЕВОМ РЕЖИМЕ")
    print("   Сбор данных: каждые 15 сек")
    print("   Детекция: каждые 75 сек")
    print("   Переобучение: каждые 24 часа")
    
    # Настройки таймеров
    COLLECT_INTERVAL = 15
    DETECT_CYCLES = 5  # Запуск детектора каждые 5 сборов (5 * 15 = 75 сек)
    RETRAIN_INTERVAL = 24 * 60 * 60  # 24 часа в секундах
    
    step = 0
    last_retrain_time = time.time()

    while True:
        # 1. Сбор данных
        subprocess.run([PIPELINE_SCRIPT], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        step += 1
        
        # 2. Детекция (раз в 75 сек)
        if step % DETECT_CYCLES == 0:
            print(f"🕵️ Запуск детектора (Step {step})...")
            subprocess.run([sys.executable, DETECT_SCRIPT])
            
        # 3. Плановое переобучение (раз в сутки)
        if time.time() - last_retrain_time > RETRAIN_INTERVAL:
            print(f"⏰ ПЛАНОВОЕ ПЕРЕОБУЧЕНИЕ (Прошло 24 часа)...")
            try:
                subprocess.run([sys.executable, TRAIN_SCRIPT], check=True)
                last_retrain_time = time.time()
                print("✅ Модель обновлена по расписанию.")
                
                # Опционально: Сброс счетчика дрейфа, так как модель новая
                if os.path.exists("drift_state.json"):
                    os.remove("drift_state.json")
                    
            except Exception as e:
                print(f"❌ Ошибка планового переобучения: {e}")

        time.sleep(COLLECT_INTERVAL)

if __name__ == "__main__":
    wait_for_db()

    init_db_structure()
    
    if not os.path.exists(MODEL_FILE):
        run_training_cycle()
    else:
        print(f"✅ Модель найдена: {MODEL_FILE}")
        
    main_loop()