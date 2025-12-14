#!/bin/bash

# Получаем директорию, где лежит скрипт
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Активируем виртуальное окружение
source venv/bin/activate

echo "--- Pipeline Start: $(date) ---"

# 1. Сбор сырых данных (Snapshots)
python3 scripts/collector.py

# 2. Расчет дельт (разницы между снимками)
python3 scripts/build_deltas.py

# 3. Сбор признаков (Features)
python3 scripts/build_features.py

# 4. Сбор лексических признаков (Текст запросов)
python3 scripts/build_lex_features.py
# 5. Вычисляет аномалии и шлет алерты
python3 scripts/detect_anomalies.py

echo "--- Pipeline End: $(date) ---"