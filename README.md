# PostgreSQL SQL Anomaly Detector (pg_stat_statements → ML → Alerts)

Этот репозиторий содержит пайплайн + ML-детектор аномалий SQL-запросов к PostgreSQL.  
Источник данных — расширение `pg_stat_statements`, которое хранит **накопительные** счётчики по запросам (`queryid`).

## TL;DR (что делает пайплайн)
Мы превращаем накопительные счётчики из `pg_stat_statements` в:
1) **снапшоты** (raw snapshots) — “фотографии” статистики на момент времени  
2) **дельты** (deltas) — “что изменилось за окно” между двумя снапшотами  
3) **оконные фичи** (features) — нормированные признаки для ML  
4) **лексические фичи** (lex features) — справочник признаков по тексту запроса  
5) **единый датасет** для ML — через view `monitoring.features_with_lex`  
6) **детекцию/алерты** — скоринг окон → `monitoring.anomaly_scores` (+ Telegram)

---

## Схема БД
Все служебные таблицы находятся в схеме `monitoring`:

- `monitoring.pgss_snapshots_raw` — сырые снапшоты `pg_stat_statements`
- `monitoring.pgss_deltas` — дельты счётчиков между двумя снапшотами (одно “окно”)
- `monitoring.features_windows` — признаки по окнам (для ML)
- `monitoring.query_lex_features` — лексические признаки (справочник по queryid)
- `monitoring.features_with_lex` (VIEW) — объединение оконных + лексических фич
- `monitoring.detector_state` — watermark детектора + streak дрифта
- `monitoring.anomaly_scores` — только аномальные окна (score + json-фичи)

---

## Скрипты

### 1) `scripts/collector.py` — сбор снапшотов из `pg_stat_statements`
**Задача:** периодически (cron/ручной запуск) сохранять текущее состояние счётчиков.

**Как работает:**
- один раз фиксирует `snapshot_ts` (timestamp запуска)
- читает статистику из `pg_stat_statements`
- сохраняет `query_text` (текст запроса), если доступен
- вставляет строки в `monitoring.pgss_snapshots_raw`

**Почему это нужно:**
`pg_stat_statements` хранит накопительные счётчики (calls/total_exec_time/blocks/wal).  
Чтобы узнать “нагрузку за интервал”, надо взять разницу между двумя снапшотами.

**Выход:** `monitoring.pgss_snapshots_raw`

---

### 2) `scripts/build_deltas.py` — расчёт дельт между двумя снапшотами
**Задача:** из соседних снапшотов получить “что произошло за окно”.

**Как работает:**
- работает инкрементально/бэкфиллом: берёт `max(window_end)` из `monitoring.pgss_deltas` и досчитывает новые окна
- строит окна по последовательным `snapshot_ts` из `monitoring.pgss_snapshots_raw` (`window_start` → `window_end`)
- соединяет строки по `(dbid, userid, queryid)`
- считает дельты:
  - `calls_delta`
  - `total_exec_time_delta`
  - `rows_delta`
  - `shared_blks_*_delta`
  - `temp_blks_*_delta`
  - `wal_bytes_delta`
- сохраняет результат в `monitoring.pgss_deltas` с полями:
  - `window_start` = время первого снапшота
  - `window_end` = время второго снапшота

**Важно:**
- в `pgss_deltas` обычно попадает **мало строк**, потому что мы сохраняем только те запросы, которые реально выполнялись в окне (`calls_delta > 0` или аналогичный фильтр).
- отрицательные дельты (reset/перезапуск статистики) пропускаются
- если снапшотов недостаточно — скрипт корректно пишет: `Not enough snapshots to compute deltas.`

**Выход:** `monitoring.pgss_deltas`

---

### 3) `scripts/build_features.py` — расчёт признаков окна (для ML)
**Задача:** превратить дельты в нормированные фичи, удобные для модели.

**Как работает:**
- берёт новые строки из `monitoring.pgss_deltas`
- рассчитывает `window_len_sec = window_end - window_start` в секундах
- строит признаки:
  - интенсивности: `calls_per_sec`, `rows_per_sec`, `exec_ms_per_sec`, `wal_bytes_per_sec` и т.п.
  - нормировки: `exec_time_per_call_ms`, `rows_per_call`, `wal_bytes_per_call` и т.п.
  - доли/отношения: `cache_miss_ratio`, `temp_share` и т.п. (если применимо)
- записывает в `monitoring.features_windows`

**Почему так:**
Абсолютные значения (например, `wal_bytes_delta`) сильно зависят от длины окна.  
Нормировки/интенсивности позволяют сравнивать окна разной длительности и лучше подходят для ML.

**Выход:** `monitoring.features_windows`

---

### 4) `scripts/build_lex_features.py` — лексические признаки по тексту запроса
**Задача:** построить “справочник” признаков по SQL-тексту (не по окну).

**Как работает:**
- берёт последние `query_text` из `monitoring.pgss_snapshots_raw` по каждому `(dbid, userid, queryid)`
- нормализует текст (удаление комментариев, приведение к lower, замена литералов/чисел и т.п.)
- считает лексические/структурные признаки, например:
  - длина запроса, количество токенов
  - `JOIN/WHERE/GROUP BY/ORDER BY/HAVING/UNION`
  - количество подзапросов, CTE
  - флаги: есть ли DML/DDL/TRANSACTION
  - количество функций, CASE
- пишет/обновляет `monitoring.query_lex_features` через UPSERT
- если текст не менялся — пишет: `Lex features are up-to-date (nothing to insert/update).`

**Важно:**
Лексика — **статическая** относительно queryid (обновляется редко), поэтому этот скрипт можно запускать реже, чем сбор окон.

**Выход:** `monitoring.query_lex_features`

---

## View для ML: `monitoring.features_with_lex`
**Что это:** объединение оконных и лексических фич.

Логика:
- `features_windows` — “что происходило за окно” (ресурсы/время/интенсивности)
- `query_lex_features` — “какой это запрос по структуре” (лексика/ключевые слова/сложность)

View делает `LEFT JOIN` по `(dbid, userid, queryid)` и даёт готовую таблицу признаков для обучения/скоринга.

---

## Рекомендуемый порядок запуска (pipeline)
Минимальный цикл (например, каждую минуту):
1) `scripts/collector.py`
2) `scripts/build_deltas.py`
3) `scripts/build_features.py`

Лексику можно:
- либо запускать каждый цикл (проще)
- либо реже (например раз в 10–60 минут):
4) `scripts/build_lex_features.py`

---

## Быстрый тест (ручной)
1) выполнить несколько SQL-запросов (создать нагрузку)
2) запустить:
```bash
python3 scripts/collector.py
sleep 10
python3 scripts/collector.py
python3 scripts/build_deltas.py
python3 scripts/build_features.py
python3 scripts/build_lex_features.py
```

## ML / Детектор (новая архитектура)

### `scripts/boot.py` — оркестратор
- ждёт доступности PostgreSQL, создаёт схему/таблицы/VIEW в `monitoring` (см. также `sql/`)
- если `MODEL_FILE` не найден — собирает baseline (без генерации нагрузки) и запускает `scripts/train_model.py`
- основной цикл: `collector → build_deltas → build_features → build_lex_features → detect_anomalies` каждые `COLLECT_INTERVAL` секунд
- плановое переобучение: каждые `RETRAIN_INTERVAL` секунд

### `scripts/train_model.py` — обучение модели
- источник: `monitoring.features_with_lex`
- фильтрация системных запросов: `scripts/detector_alerts.py:is_system_query`
- фичи для модели: `scripts/detector_features.py:ALL_FEATURES`
- модель: `SimpleImputer → StandardScaler → IsolationForest`
- порог аномалии: квантиль `decision_function` на трейне (`MODEL_ALERT_QUANTILE`), сохраняется вместе с пайплайном в `MODEL_FILE`

### `scripts/detect_anomalies.py` / `scripts/detector_runner.py` — скоринг, алерты, дрифт
- watermark + streak: `monitoring.detector_state` (строка `id=1`)
- берёт новые окна из `monitoring.features_with_lex` по `window_end > last_window_end` (лимит `DETECT_BATCH_LIMIT`)
- скоринг: `model.decision_function`, аномалия если `score <= threshold` (`ALERT_SCORE_THRESHOLD=auto` или число)
- пишет только аномалии в `monitoring.anomaly_scores` (idempotent по PK)
- Telegram (опционально): `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`
- дрифт: streak++ если в прогоне были “существенные” алерты (`DRIFT_SIGNIF_*`); при `DRIFT_CONSECUTIVE_LIMIT` запускается переобучение

---

## Конфигурация
См. `.env.example`. Ключевые переменные:
- `DB_HOST/DB_PORT/DB_NAME/DB_USERNAME/DB_PASSWORD`
- `MODEL_FILE`, `MODEL_VERSION`
- `ALERT_SCORE_THRESHOLD` (`auto` или число)
- `COLLECT_INTERVAL`, `RETRAIN_INTERVAL`, `DETECT_BATCH_LIMIT`
- `MODEL_*` (параметры обучения), `DRIFT_*` (параметры дрифта)
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (опционально)

---

## Быстрый запуск

### Docker
1. Скопируйте `.env.example` → `.env` и настройте DB_*. Важно: `DB_HOST` должен быть доступен из контейнера (например, имя контейнера Postgres в docker-сети или `host.docker.internal` на macOS/Windows).
2. Включите `pg_stat_statements` на стороне PostgreSQL (`shared_preload_libraries = 'pg_stat_statements'`). Для авто-инициализации `monitoring` пользователю БД нужны права на `CREATE EXTENSION/SCHEMA/TABLE`.
3. Поднимите сервис:
```bash
docker network create anomaly-net
docker compose up --build
```

Логи:
- `docker compose logs -f ml_service`
- `docker logs -f anomaly_detector`

### Проверка детекции аномалий
1. Выполните нетипичный/тяжёлый запрос, например:
```sql
SELECT pg_sleep(5);
```
2. Подождите 1–2 цикла `COLLECT_INTERVAL` (снапшот → дельта → фичи → детект).
3. Проверьте Telegram или таблицу:
```sql
SELECT *
FROM monitoring.anomaly_scores
ORDER BY scored_at DESC
LIMIT 20;
```

### Тестирование дрифта
Дрифт считается по серии “существенных” алертов (см. `DRIFT_SIGNIF_*` и `DRIFT_CONSECUTIVE_LIMIT`).  
Чтобы спровоцировать — выполняйте тяжёлый запрос несколько циклов подряд: при достижении лимита детектор запустит `scripts/train_model.py`.

### Сброс состояния детектора
Сброс watermark/streak (без удаления данных пайплайна):
```sql
UPDATE monitoring.detector_state
SET last_window_end = NULL,
    bad_runs_streak = 0,
    updated_at = now()
WHERE id = 1;
```
