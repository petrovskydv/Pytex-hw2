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

## Линтер

```bash
uv run ruff check .
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

Остановить инфраструктуру:

```bash
docker compose down
```
