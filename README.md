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
   docker compose up -d db payment-api protection-api redis kafka
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

Для запуска фоновых задач используются три независимые очереди: `reports`, `cleanup`, `insurance`.
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

### Kafka

Для событий о покупках используется Kafka в single-node KRaft-конфигурации. С хоста broker доступен как
`localhost:9092`, из сервисов Docker Compose — как `kafka:19092`.

Параметры Kafka недели 5 вынесены в секцию `KAFKA__*`: topic `tickets.purchased`, `linger_ms=75`, batch до 10
сообщений с ожиданием не более 500 мс. Таймаут WebSocket-отправки задаётся отдельно через
`WEBSOCKET__SEND_TIMEOUT_SECONDS` и не может превышать 2 секунды.

Для работы с Kafka используется FastStream поверх `aiokafka`.

### Генератор тестовых покупок

Основное приложение запускает через FastAPI lifespan фоновый генератор тестовых покупок. Каждое событие содержит
`payment_id`, `event_id`, `tickets_count`, `total_amount` и `paid_at`. По умолчанию событие создаётся каждые 50 мс,
а `event_id` выбирается из диапазона 1–5, поэтому за окно 500 мс может появиться несколько покупок, в том числе
для одного мероприятия.

Параметры генератора задаются через `PURCHASE_GENERATOR__INTERVAL_SECONDS` и
`PURCHASE_GENERATOR__EVENT_ID_MAX`.

### Публикация событий о покупках

Основное приложение создаёт FastStream `KafkaBroker` в lifespan и публикует каждую сгенерированную покупку как факт
`tickets.purchased`. Producer использует `KAFKA__LINGER_MS=75`; публикация не ждёт broker confirmation, чтобы события
могли накапливаться в producer buffer. При shutdown сначала останавливается генератор, затем Kafka broker закрывает
producer.

### Сервис мониторинга покупок

Monitoring — отдельное FastAPI-приложение с собственными подключениями к PostgreSQL и Kafka. FastStream subscriber
получает `tickets.purchased` батчами до 10 сообщений или 500 мс и использует manual acknowledgement. Батч сразу
агрегируется по `event_id` и сохраняется одной транзакцией PostgreSQL. Kafka offset подтверждается только после
успешного commit БД; затем агрегаты передаются через локальную `asyncio.Queue` отдельному WebSocket worker, который
конкурентно рассылает их клиентам `WS /ws/payments` с таймаутом не более 2 секунд на клиента.

`faststream[kafka]` является общей runtime-зависимостью основного API и monitoring-сервиса и хранится в корневом
`pyproject.toml`; отдельного `requirements.txt` для monitoring нет.

Для запуска с хоста:

```bash
uv run uvicorn monitoring.main:app --host 127.0.0.1 --port 8001
```

Весь стек вместе с миграцией, API, monitoring, workers, scheduler и Kafka можно запустить одной командой:

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
| Kafka | `localhost:9092` |
| Payment API | http://localhost:9001 |
| Protection API | http://localhost:9002 |
| FastAPI | http://localhost:8000 |
| Monitoring FastAPI / WebSocket | http://localhost:8001 / `ws://localhost:8001/ws/payments` |

Остановить инфраструктуру:

```bash
docker compose down
```
