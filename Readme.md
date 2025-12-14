# PostgreSQL SQL Anomaly Detector — Data Pipeline (pg_stat_statements → Features)

Этот репозиторий содержит конвейер подготовки данных для ML-детекции аномалий запросов к PostgreSQL.  
Источник данных — расширение `pg_stat_statements`, которое хранит **накопительные** счётчики по запросам (queryid).

## TL;DR (что делает пайплайн)
Мы превращаем накопительные счётчики из `pg_stat_statements` в:
1) **снапшоты** (raw snapshots) — “фотографии” статистики на момент времени  
2) **дельты** (deltas) — “что изменилось за окно” между двумя снапшотами  
3) **оконные фичи** (features) — нормированные признаки для ML  
4) **лексические фичи** (lex features) — справочник признаков по тексту запроса  
5) **единый датасет** для ML — через view `monitoring.features_with_lex`

---

## Схема БД
Все служебные таблицы находятся в схеме `monitoring`:

- `monitoring.pgss_snapshots_raw` — сырые снапшоты `pg_stat_statements`
- `monitoring.pgss_deltas` — дельты счётчиков между двумя снапшотами (одно “окно”)
- `monitoring.features_windows` — признаки по окнам (для ML)
- `monitoring.query_lex_features` — лексические признаки (справочник по queryid)
- `monitoring.features_with_lex` (VIEW) — объединение оконных + лексических фич

---

## Скрипты

### 1) `collector.py` — сбор снапшотов из `pg_stat_statements`
**Задача:** периодически (cron/ручной запуск) сохранять текущее состояние счётчиков.

**Как работает:**
- один раз фиксирует `snapshot_ts` (timestamp запуска)
- читает статистику из `pg_stat_statements`
- вставляет строки в `monitoring.pgss_snapshots_raw`

**Почему это нужно:**
`pg_stat_statements` хранит накопительные счётчики (calls/total_exec_time/blocks/wal).  
Чтобы узнать “нагрузку за интервал”, надо взять разницу между двумя снапшотами.

**Выход:** `monitoring.pgss_snapshots_raw`

---

### 2) `build_deltas.py` — расчёт дельт между двумя снапшотами
**Задача:** из двух последних снапшотов получить “что произошло за окно”.

**Как работает:**
- находит **два разных** последних `snapshot_ts` в `monitoring.pgss_snapshots_raw`
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
- если снапшотов недостаточно — скрипт корректно пишет: `Not enough snapshots to compute deltas.`

**Выход:** `monitoring.pgss_deltas`

---

### 3) `build_features.py` — расчёт признаков окна (для ML)
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

### 4) `build_lex_features.py` — лексические признаки по тексту запроса
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
1) `collector.py`
2) `build_deltas.py`
3) `build_features.py`

Лексику можно:
- либо запускать каждый цикл (проще)
- либо реже (например раз в 10–60 минут):
4) `build_lex_features.py`

---

## Быстрый тест (ручной)
1) выполнить несколько SQL-запросов (создать нагрузку)
2) запустить:
```bash
python3 collector.py
sleep 10
python3 collector.py
python3 build_deltas.py
python3 build_features.py
python3 build_lex_features.py
```


Алгоритм предотвращение дрейфа

Берем две выборки:

new_df (твои свежие 36-50+ окон).

ref_df (старые "эталонные" данные из базы, на которых модель работала нормально).

Сравниваем распределения (Тест Колмогорова-Смирнова): Он берет ключевые метрики (например, exec_time_per_call_ms) и математически проверяет, совпадают ли их графики распределения.

Решение: Если p-value < 0.01 (графики сильно разошлись), он считает это дрейфом.

Счетчик: Если дрейф фиксируется 5 раз подряд (параметр DRIFT_CONSECUTIVE_LIMIT), запускаем автоматическое переобучение (train_model_emergency), чтобы модель привыкла к новой реальности и перестала спамить алертами.