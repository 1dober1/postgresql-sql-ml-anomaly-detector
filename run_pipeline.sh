#!/bin/bash

# Определяем, где лежит этот скрипт, и переходим туда
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$PROJECT_DIR"

# Активируем виртуальное окружение
source venv/bin/activate

echo "--- Pipeline Start: $(date) ---"

# Запускаем скрипты по цепочке. Если один упадет, следующие не запустятся (&&)
python3 scripts/collector.py && \
python3 scripts/build_deltas.py && \
python3 scripts/build_features.py && \
python3 scripts/build_lex_features.py

echo "--- Pipeline End: $(date) ---"
