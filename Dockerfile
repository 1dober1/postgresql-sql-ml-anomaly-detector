FROM python:3.10-slim

# 1. Устанавливаем системные зависимости и клиент Postgres (для pgbench)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Рабочая директория
WORKDIR /app

# 3. Ставим Python-библиотеки
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Копируем весь проект внутрь
COPY . .

# 5. Даем права на выполнение скриптам
RUN chmod +x scripts/*.py run_pipeline.sh

# 6. Запускаем умный загрузчик
CMD ["python3", "-u", "scripts/boot.py"]