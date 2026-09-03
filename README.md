# Pytex HW2

## Локальный запуск

Требования: Python 3.13, [uv](https://docs.astral.sh/uv/) и Docker с Docker Compose.

1. Установите зависимости:

   ```bash
   uv sync
   ```

2. Создайте локальную конфигурацию:

   ```bash
   cp .env.example .env
   ```

3. Запустите инфраструктуру:

   ```bash
   docker compose up -d db payment-api protection-api redis
   ```

4. Примените миграции:

   ```bash
   uv run alembic upgrade head
   ```

5. Запустите приложение в режиме разработки с автоматической перезагрузкой:

   ```bash
   uv run python run.py
   ```

API доступен по адресу http://127.0.0.1:8000, Swagger UI - http://127.0.0.1:8000/docs.

### Фоновые задачи TaskIQ

Для ДЗ №4 используются Redis Streams и три независимые очереди: `reports`, `cleanup`, `insurance`.
После запуска инфраструктуры откройте четыре дополнительных терминала:

```bash
uv run taskiq worker --workers 2 --max-async-tasks 1 --max-threadpool-threads 1 \
  app.background.brokers:reports_broker app.background.jobs
uv run taskiq worker --workers 1 --max-async-tasks 1 \
  app.background.brokers:cleanup_broker app.background.jobs
uv run taskiq worker --workers 1 --max-async-tasks 10 \
  app.background.brokers:insurance_broker app.background.jobs
uv run taskiq scheduler --skip-first-run app.background.brokers:scheduler app.background.jobs
```

Scheduler должен запускаться только в одном экземпляре. PDF-отчёты сохраняются в каталог `reports/`.

Весь стек вместе с миграцией, API, workers и scheduler можно запустить одной командой:

```bash
docker compose up --build
```

## Линтер

```bash
uv run ruff check .
```

## Тесты

Для тестов нужен запущенный Docker Desktop. Вручную запускать `db` и применять миграции не нужно: pytest создаст изолированный PostgreSQL-контейнер и применит Alembic-миграции.

```bash
uv run pytest
```

## Pre-commit

Установите hook после `uv sync`:

```bash
uv run pre-commit install
```

Перед каждым коммитом Ruff исправит поддерживаемые ошибки и отформатирует индексированные Python-файлы. Для ручного запуска hook на всех файлах:

```bash
uv run pre-commit run --all-files
```

## Сервисы

| Сервис | Адрес |
| --- | --- |
| PostgreSQL | `localhost:7432` |
| Redis | `localhost:7379` |
| Payment API | http://localhost:9001 |
| Protection API | http://localhost:9002 |
| FastAPI | http://localhost:8000 |

Остановить инфраструктуру:

```bash
docker compose down
```
