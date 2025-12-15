# ИСПОЛЬЗУЕМ ПОЛНУЮ ВЕРСИЮ (Решает все проблемы с установкой пакетов)
FROM python:3.10

ENV DEBIAN_FRONTEND=noninteractive

# 1. Устанавливаем базовые утилиты
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    lsb-release \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Добавляем репозиторий PostgreSQL
RUN wget --quiet -O /usr/share/keyrings/postgresql.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql.asc] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client-15 \
    postgresql-15 \
    && rm -rf /var/lib/apt/lists/*


# 4. Проверка (теперь это точно сработает)
RUN pgbench --version

# 5. Сборка проекта
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x scripts/*.py run_pipeline.sh

CMD ["python3", "-u", "scripts/boot.py"]