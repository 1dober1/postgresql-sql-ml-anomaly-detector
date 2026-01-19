# Детектор аномалий SQL для PostgreSQL

Пайплайн для детекции аномалий SQL-запросов по данным
`pg_stat_statements`: снимки -> дельты -> оконные фичи ->
лексика -> ML-скоринг -> алерты.

## Поток данных

pg_stat_statements
  -> monitoring.pgss_snapshots_raw
  -> monitoring.pgss_deltas
  -> monitoring.features_windows
  -> monitoring.query_lex_features
  -> monitoring.features_with_lex (представление)
  -> ML-скоринг
  -> monitoring.anomaly_scores + Telegram

## Схема БД (monitoring)

- pgss_snapshots_raw: сырые снапшоты, `snapshot_ts`, накопительные счётчики,
  `query_text`; PK `snapshot_id`.
- pgss_deltas: дельты между соседними снапшотами; PK
  `(window_start, dbid, userid, queryid)`.
- features_windows: оконные метрики и признаки; PK как у дельт.
- query_lex_features: лексика по `(dbid, userid, queryid)`, `query_md5`,
  `last_seen_ts`.
- features_with_lex: view, `LEFT JOIN` оконных и лексических признаков.
- detector_state: одиночная строка `id=1`, `last_window_end`,
  `bad_runs_streak`.
- anomaly_scores: только аномальные окна, `features` jsonb; PK
  `(model_version, window_end, dbid, userid, queryid)`.

DDL выполняется в `scripts/boot.py`. Эталонные SQL лежат в `sql/`.

## Скрипты

- `scripts/collector.py`: читаёт `pg_stat_statements`, пишет снапшоты.
- `scripts/build_deltas.py`: считает дельты окон, пропускает отрицательные
  значения и `calls_delta <= 0`.
- `scripts/build_features.py`: строит оконные признаки.
- `scripts/build_lex_features.py`: нормализует SQL, считает лексику, `UPSERT` по `query_md5`.
- `scripts/train_model.py`: обучает IsolationForest на `features_with_lex`.
- `scripts/detector_runner.py`: скоринг, запись аномалий, алерты.
- `scripts/detect_anomalies.py`: точка входа для `detector_runner.run_once`.
- `scripts/boot.py`: оркестрация, bootstrap, плановое переобучение.
- `scripts/detector_alerts.py`: фильтрация системных запросов, Telegram.
- `scripts/detector_db.py`: хелперы БД и хранение состояния/аномалий.
- `scripts/detector_features.py`: набор фич, log1p, JSON сериализация.
- `run_pipeline.sh`: ручной запуск шагов пайплайна (без детекта).

## Поведение и идемпотентность

- Дельты считаются только для `calls_delta > 0`, отрицательные дельты пропускаются (сброс статистики).
- Вставки в `pgss_deltas`, `features_windows`, `anomaly_scores` идемпотентны (`ON CONFLICT DO NOTHING`).
- Лексика обновляется только при смене `query_md5`.
- `window_len_sec <= 0` нормализуется в 1 секунду.

## Признаки и модель

### Оконные признаки (features_windows)

- `window_len_sec`, `calls_in_window`.
- per-call: `exec_time_per_call_ms`, `rows_per_call`,
  `shared_read_per_call`, `temp_read_per_call`, `wal_bytes_per_call`.
- per-sec: `calls_per_sec`, `exec_ms_per_sec`, `rows_per_sec`,
  `shared_read_per_sec`, `temp_read_per_sec`, `wal_bytes_per_sec`.
- ratios: `temp_share`, `cache_miss_ratio`.
- efficiency: `ms_per_row`, `read_blks_per_row`.

### Лексические признаки (query_lex_features)

- Длины и токены: `query_len_chars`, `query_len_norm_chars`, `num_tokens`.
- Структура: `num_joins`, `num_where`, `num_group_by`, `num_order_by`,
  `num_having`, `num_union`, `num_subqueries`, `num_cte`.
- Флаги: `has_write`, `has_ddl`, `has_tx`.
- Сложность: `num_case`, `num_functions`.
- Идентификация: `query_md5`.

Нормализация SQL: удаление комментариев, замена литералов/чисел,
`lower()`, схлопывание пробелов.

### Набор для модели

`ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES`

- LOG_FEATURES: `shared_read_per_call`, `temp_read_per_call`, `ms_per_row`.
- OTHER_NUM_FEATURES: `calls_per_sec`, `cache_miss_ratio`, `temp_share`,
  `read_blks_per_row`, `exec_time_per_call_ms`, `rows_per_call`,
  `wal_bytes_per_call`.
- LEX_FEATURES: `query_len_norm_chars`, `num_tokens`, `num_joins`,
  `num_where`, `num_group_by`, `num_order_by`, `has_write`, `has_ddl`.

Log1p применяется к `MODEL_LOG1P_FEATURES`:
`exec_time_per_call_ms`, `rows_per_call`, `wal_bytes_per_call`,
`shared_read_per_call`, `temp_read_per_call`, `ms_per_row`,
`read_blks_per_row`, `calls_per_sec`.

Отсутствующие значения, `NaN` и `inf` приводятся к `0`.

### Обучение

- Источник: `monitoring.features_with_lex`.
- Фильтр: `is_system_query` (системные/служебные запросы исключаются).
- Семплирование: перемешивание + `MODEL_MAX_SAMPLES_PER_QUERYID`.
- Минимум данных: `MODEL_MIN_ROWS`, `MODEL_MIN_QUERYIDS`.
- Pipeline: `SimpleImputer(constant=0) -> StandardScaler -> IsolationForest`.
- Порог: квантиль `decision_function` по `MODEL_ALERT_QUANTILE`.
- Артефакт: `MODEL_FILE` (pickle словаря с `pipeline`, `threshold`, метаданными).

## Детекция и дрейф

- Окна берутся с `window_end > last_window_end`, лимит `DETECT_BATCH_LIMIT`.
- Аномалия: `score <= threshold`.
  `ALERT_SCORE_THRESHOLD=auto/none/""` берёт порог из модели.
- Запись: `monitoring.anomaly_scores`, `features` сохраняются как jsonb.
- Telegram опционален; текст обрезается до 4000 символов, SQL до 200.
- Drift: "существенный" прогон, если метрики превышают `DRIFT_SIGNIF_*`. При `DRIFT_CONSECUTIVE_LIMIT` запускается переобучение и сброс streak.

## Оркестрация (boot.py)

- `wait_for_db` -> `init_db_structure` (схема + таблицы + представление).
- Если `MODEL_FILE` отсутствует:
  bootstrap `collector/deltas/features/lex` на
  `TRAIN_COLLECT_ITERATIONS` с паузой `TRAIN_COLLECT_SLEEP`, затем train;
  повтор до `TRAIN_RETRY_LIMIT`.
- Основной цикл: `collector -> deltas -> features -> lex -> detect`
  каждые `COLLECT_INTERVAL` секунд.
- Плановое переобучение: раз в `RETRAIN_INTERVAL` секунд.

## Конфигурация

Полный перечень в `.env.example`. Ключевые группы:

- БД: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`.
- Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.
- Модель: `MODEL_FILE`, `MODEL_VERSION`, `MODEL_CONTAMINATION`,
  `MODEL_N_ESTIMATORS`, `MODEL_MIN_ROWS`, `MODEL_MIN_QUERYIDS`,
  `MODEL_MAX_SAMPLES_PER_QUERYID`, `MODEL_ALERT_QUANTILE`.
- Детекция: `ALERT_SCORE_THRESHOLD`, `DETECT_BATCH_LIMIT`.
- Планировщик: `COLLECT_INTERVAL`, `RETRAIN_INTERVAL`.
- Bootstrap: `TRAIN_COLLECT_ITERATIONS`, `TRAIN_COLLECT_SLEEP`,
  `TRAIN_RETRY_LIMIT`.
- Drift: `DRIFT_CONSECUTIVE_LIMIT`, `DRIFT_SIGNIF_EXEC_MS`,
  `DRIFT_SIGNIF_ROWS`, `DRIFT_SIGNIF_SHARED_READ`,
  `DRIFT_SIGNIF_WAL_BYTES`.

`python-dotenv` подхватывает `.env` при импорте `db_config`.

## Запуск

### Docker

1) Скопируйте `.env.example` -> `.env` и настройте DB.
2) Включите `pg_stat_statements` (`shared_preload_libraries`),
   нужны права на `CREATE EXTENSION/SCHEMA/TABLE/VIEW`.
3) Создайте сеть: `docker network create anomaly-net`
   (или задайте `ANOMALY_NET`).
4) `docker compose up --build`.

Для сохранения модели удобно выставить `MODEL_FILE=artifacts/...` и
примонтировать `./artifacts` (volume уже описан в `docker-compose.yml`).

### Локально

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 scripts/boot.py
```

### Ручной прогон пайплайна

```bash
bash run_pipeline.sh
```

## Проверка

Пример проверки аномалий:

```sql
SELECT pg_sleep(5);
```

Далее дождитесь 1-2 циклов и проверьте:

```sql
SELECT *
FROM monitoring.anomaly_scores
ORDER BY scored_at DESC
LIMIT 20;
```

Сброс состояния детектора:

```sql
UPDATE monitoring.detector_state
SET last_window_end = NULL,
    bad_runs_streak = 0,
    updated_at = now()
WHERE id = 1;
```

## Логи

- `docker compose logs -f ml_service`
- `docker logs -f anomaly_detector`

## Требования

- PostgreSQL с включённым `pg_stat_statements`.
- Python 3.10+ (в Docker используется 3.10).
- Ключевые пакеты: `psycopg`, `pandas`, `scikit-learn`, `SQLAlchemy`,
  `requests`.
