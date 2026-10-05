# Notification Service

Event-driven сервис отправки уведомлений на Go. Public API принимает HTTP-запрос, формирует идемпотентное событие и публикует его в Kafka. Отдельный worker читает события, валидирует их, защищает обработку lease-механизмом в PostgreSQL и вызывает адаптер нужного канала: Telegram, email или SMS.

> Сейчас адаптеры отправки являются заглушками: они не обращаются к реальным внешним провайдерам, а только имитируют временные ошибки. Проект демонстрирует взаимодействие API, Kafka, worker и PostgreSQL, а также обработку повторов, DLQ и сбоев.

> Это Go-версия проекта (ветка `go-rewrite`). Исходная реализация на Python находится в ветке `main`. Обе версии совместимы по Kafka-топикам, формату событий, `event_id` и схеме БД, см. [Совместимость с Python-версией](#совместимость-с-python-версией).

## Содержание

- [Возможности](#возможности)
- [Технологии](#технологии)
- [Архитектура](#архитектура)
- [Как проходит обработка события](#как-проходит-обработка-события)
- [Структура проекта](#структура-проекта)
- [API](#api)
- [Формат Kafka-события](#формат-kafka-события)
- [Kafka topics](#kafka-topics)
- [Модель данных PostgreSQL](#модель-данных-postgresql)
- [Быстрый запуск через Docker Compose](#быстрый-запуск-через-docker-compose)
- [Локальный запуск приложений](#локальный-запуск-приложений)
- [Конфигурация](#конфигурация)
- [Миграции](#миграции)
- [Тестирование и проверки](#тестирование-и-проверки)
- [Надёжность и гарантии доставки](#надёжность-и-гарантии-доставки)
- [Подключение реальных провайдеров](#подключение-реальных-провайдеров)
- [Совместимость с Python-версией](#совместимость-с-python-версией)

## Возможности

- HTTP API, Kafka producer/consumer и доступ к PostgreSQL с отменой и таймаутами через `context`;
- ручной commit Kafka offset только после завершения обработки;
- обязательный HTTP-заголовок `Idempotency-Key`;
- стабильный `event_id` для повторного запроса;
- идемпотентный Kafka producer с `acks=all`;
- строгая валидация входных HTTP- и Kafka-событий;
- ограниченные по количеству и времени повторы отправки;
- отдельные DLQ для ошибок каналов;
- poison DLQ для невалидных Kafka-сообщений;
- lease-механизм для восстановления зависших записей `PENDING`;
- readiness- и liveness-проверки API, Swagger UI и ReDoc;
- автоматические миграции при старте worker, встроенные в бинарник;
- graceful shutdown по `SIGTERM` и `SIGINT`;
- статические бинарники и минимальные distroless-образы;
- автоматический перезапуск контейнеров приложения после сбоя.

## Технологии

| Компонент | Технология |
|---|---|
| Язык | Go 1.23+ |
| HTTP API | `net/http` из стандартной библиотеки |
| Валидация | собственная, в `internal/notification` и `internal/api` |
| Конфигурация | `caarlos0/env`, `joho/godotenv` |
| Брокер сообщений | Apache Kafka 3.9.1 |
| Kafka-клиент | `twmb/franz-go` 1.18, чистый Go без cgo |
| База данных | PostgreSQL 15 |
| Драйвер | `jackc/pgx` v5, SQL без ORM |
| Миграции | `pressly/goose` v3, SQL-файлы встроены в бинарник |
| Повторы | собственный exponential backoff |
| Логирование | `log/slog`, формат `key=value` |
| Инфраструктура | Docker Compose, образы `distroless/static` |
| Тесты | `go test`, `testing`, `net/http/httptest` |

Версии библиотек выбраны так, чтобы проект собирался на Go 1.23. Docker-образы собираются на Go 1.26. Если локально стоит Go 1.26 или новее, зависимости можно обновить: `go get -u ./... && go mod tidy`.

## Архитектура

```mermaid
flowchart LR
    Client["HTTP-клиент"] -->|"POST /message/*<br/>Idempotency-Key"| API["Public API<br/>net/http"]
    API --> UC["NotificationPublisher"]
    UC --> AP["Kafka producer<br/>franz-go"]
    AP --> Topics["Kafka<br/>notification.tg<br/>notification.email<br/>notification.sms"]

    Topics --> Consumer["Kafka consumer<br/>group: notifications"]
    Consumer --> Processor["worker.Processor"]
    Processor --> Validator["notification.ParseEvent"]
    Processor <--> DB[("PostgreSQL<br/>notification_log")]
    Processor --> Sender["Sender adapter<br/>TG / email / SMS"]
    Processor -->|"ошибка после retry"| DP["DLQ producer"]
    Validator -->|"невалидное событие"| DP
    DP --> DLQ["Kafka DLQ topics"]

    Processor -->|"обработка завершена"| Consumer
    Consumer -. "manual offset commit" .-> Topics
```

Public API и worker являются независимыми процессами:

- API не отправляет уведомление самостоятельно и не зависит от PostgreSQL;
- Kafka отделяет приём запроса от фоновой обработки;
- worker хранит состояние обработки в PostgreSQL;
- ошибки внешнего канала не блокируют HTTP-запрос, уже принятый API;
- Kafka offset подтверждается только после успешной отправки либо подтверждённой публикации ошибки в DLQ.

## Как проходит обработка события

### Успешный сценарий

1. Клиент отправляет запрос в Public API с `Idempotency-Key`.
2. API проверяет тело запроса и допустимость канала для выбранного типа поздравления.
3. Из типа события и ключа идемпотентности вычисляется стабильный UUID `event_id`.
4. Событие публикуется в `notification.<channel>`.
5. Worker получает событие, но пока не подтверждает Kafka offset.
6. Worker проверяет схему: тип события, канал, длину текста, временную зону `created_at` и отсутствие лишних полей.
7. Worker атомарно получает lease на `event_id` в таблице `notification_log`.
8. Вызывается адаптер нужного канала с тем же `event_id` в качестве `idempotencyKey`.
9. Запись переводится в статус `completed`.
10. Worker вручную подтверждает Kafka offset.

### Временная ошибка провайдера

1. Отправка повторяется ограниченное количество раз.
2. Каждый вызов sender ограничен отдельным таймаутом.
3. Между попытками используется exponential backoff.
4. После исчерпания попыток событие публикуется в `notification.<channel>.dlq`.
5. Только после подтверждения доставки в DLQ запись переводится в `failed`, а Kafka offset подтверждается.
6. Если публикация в DLQ не удалась, ошибка возвращается выше и исходный offset не подтверждается.

### Невалидное Kafka-сообщение

Невалидный JSON, неправильная схема, отсутствующий ключ, несовпадение `event_id`, topic или канала приводят к публикации сообщения в `notification.invalid.dlq`. Offset исходного сообщения подтверждается только после подтверждённой записи в poison DLQ.

### Аварийное завершение или остановка worker

Если worker завершился после создания `PENDING`, но до финального статуса:

1. Kafka повторно доставит неподтверждённое событие;
2. новый worker дождётся окончания текущего lease;
3. просроченный `PENDING` будет атомарно захвачен повторно;
4. обработка продолжится без вечной блокировки события.

Так же обрабатывается штатная остановка. По `SIGTERM` worker отменяет текущую отправку, не подтверждает offset и не помечает событие как `failed`.

## Структура проекта

```text
Notification-Service/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .env.example
├── go.mod
├── go.sum
├── cmd/
│   ├── publicapi/
│   │   └── main.go
│   └── worker/
│       └── main.go
├── internal/
│   ├── api/
│   │   ├── handler.go
│   │   ├── schema.go
│   │   ├── publisher.go
│   │   ├── docs.go
│   │   └── openapi.yaml
│   ├── config/
│   │   └── config.go
│   ├── kafka/
│   │   ├── consumer.go
│   │   ├── producer.go
│   │   └── topics.go
│   ├── notification/
│   │   └── event.go
│   ├── sender/
│   │   └── sender.go
│   ├── storage/
│   │   └── repository.go
│   └── worker/
│       └── processor.go
└── migrations/
    ├── migrations.go
    ├── 00001_create_notification_log.sql
    └── 00002_add_processing_lease.sql
```

Тесты `*_test.go` лежат рядом с кодом и в дереве не показаны.

### Общие файлы

| Путь | Назначение |
|---|---|
| `Dockerfile` | Многоэтапная сборка: статический бинарник `cmd/${APP}` в образе `distroless/static` |
| `docker-compose.yml` | PostgreSQL, Kafka, Public API, worker, healthchecks, зависимости и volumes |
| `.dockerignore` | В build context попадают только `go.mod`, `go.sum`, `cmd/`, `internal/` и `migrations/` |
| `.env.example` | Пример настроек для локального запуска и Docker Compose |
| `internal/config/config.go` | Загрузка настроек из окружения и `.env`, проверка их согласованности |
| `internal/notification/event.go` | Строгий контракт Kafka-события и доменные ошибки sender |
| `internal/kafka/topics.go` | Имена `notification.<channel>`, DLQ-топиков и списка входных топиков |
| `internal/kafka/producer.go` | Публикация с ожиданием подтверждения Kafka и ping для readiness |

### Public API

| Путь | Назначение |
|---|---|
| `cmd/publicapi/main.go` | HTTP-сервер, graceful shutdown и подкоманда `healthcheck` для Docker |
| `internal/api/handler.go` | Маршруты, разбор запроса, ответы `202`, `400`, `413`, `422`, `503` |
| `internal/api/schema.go` | Схема запроса, проверка `Idempotency-Key` и ошибки валидации в формате FastAPI |
| `internal/api/publisher.go` | Допустимые каналы, формирование события и стабильного `event_id` |
| `internal/api/docs.go` | Страницы `/docs`, `/redoc` и выдача спецификации |
| `internal/api/openapi.yaml` | Спецификация OpenAPI 3.1 |

### Notification worker

| Путь | Назначение |
|---|---|
| `cmd/worker/main.go` | Миграции, сборка зависимостей, цикл consumer → process → commit и корректное закрытие ресурсов |
| `internal/kafka/consumer.go` | Чтение, декодирование, poison-envelope и ручной commit |
| `internal/worker/processor.go` | Валидация, lease, retry, sender, DLQ и финальный статус |
| `internal/storage/repository.go` | Атомарный lease, повторный захват и обновление статуса |
| `internal/sender/sender.go` | Заглушки email/SMS/Telegram sender |
| `migrations/` | SQL-миграции, встроенные в бинарник worker через `go:embed` |

## API

После запуска:

- Swagger UI: [http://localhost:8001/docs](http://localhost:8001/docs)
- ReDoc: [http://localhost:8001/redoc](http://localhost:8001/redoc)
- OpenAPI-спецификация: [http://localhost:8001/openapi.yaml](http://localhost:8001/openapi.yaml)
- liveness: [http://localhost:8001/live](http://localhost:8001/live)
- readiness: [http://localhost:8001/health](http://localhost:8001/health)

Спецификация `internal/api/openapi.yaml` написана вручную, в Python-версии её генерировал FastAPI. При изменении API обновляйте её вместе с кодом. Страницы `/docs` и `/redoc`, как и в FastAPI, загружают скрипты с jsDelivr.

### `POST /message/birthday`

Поздравление с днём рождения:

- разрешены `tg` и `sms`;
- `email` возвращает `400 Bad Request`.

```bash
curl -X POST http://localhost:8001/message/birthday \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: birthday-user-42-2026" \
  -d '{
    "msg_type": "tg",
    "msg_text": "Поздравляем с днём рождения!"
  }'
```

### `POST /message/christmas`

Рождественское поздравление:

- разрешены `tg` и `email`;
- `sms` возвращает `400 Bad Request`.

```bash
curl -X POST http://localhost:8001/message/christmas \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: christmas-user-42-2026" \
  -d '{
    "msg_type": "email",
    "msg_text": "Поздравляем с Рождеством!"
  }'
```

Успешный ответ имеет статус `202 Accepted`:

```json
{
  "status": "accepted",
  "event_id": "bb266a9f-8d42-52aa-832c-77ef353479dd"
}
```

`event_id` детерминирован по типу события и `Idempotency-Key`. Повтор одного endpoint с тем же ключом вернёт тот же идентификатор. Для новой логической операции используйте новый ключ.

### Схема HTTP-запроса

| Поле | Тип | Ограничения |
|---|---|---|
| `msg_type` | `tg`, `email` или `sms` | Допустимость зависит от endpoint |
| `msg_text` | string | От 10 до 400 символов |
| `Idempotency-Key` | HTTP header | Обязательный, от 8 до 128 символов |

Длина строк считается в символах, а не в байтах.

### Ошибки

| Статус | Когда | Тело |
|---|---|---|
| `400` | Канал не разрешён для типа поздравления | `{"detail": "..."}` |
| `413` | Тело запроса больше 64 КБ | `{"detail": "Request body too large"}` |
| `422` | Ошибка в заголовке или теле запроса | Список ошибок, см. ниже |
| `500` | Kafka не подтвердила публикацию | `{"detail": "Internal Server Error"}` |

Ошибки `422` собираются по всем полям сразу и имеют ту же структуру, что в FastAPI. Тексты сообщений короче, чем у FastAPI.

```json
{
  "detail": [
    {"loc": ["header", "Idempotency-Key"], "msg": "Field required", "type": "missing"},
    {"loc": ["body", "msg_text"], "msg": "String should have at least 10 characters", "type": "string_too_short"}
  ]
}
```

### Health endpoints

| Endpoint | Назначение | Ответ |
|---|---|---|
| `GET /live` | Проверяет, что процесс API работает | `200 {"status":"ok"}` |
| `GET /health` | Проверяет доступность Kafka через общий producer | `200`, либо `503` при недоступной Kafka |

## Формат Kafka-события

```json
{
  "event_id": "bb266a9f-8d42-52aa-832c-77ef353479dd",
  "event_type": "birthday",
  "channel": "tg",
  "msg_text": "Поздравляем с днём рождения!",
  "created_at": "2026-07-30T15:00:00.123456Z"
}
```

| Поле | Назначение |
|---|---|
| `event_id` | Идентификатор и ключ Kafka-сообщения |
| `event_type` | `birthday` или `christmas` |
| `channel` | `tg`, `email` или `sms` |
| `msg_text` | Текст длиной 10–400 символов |
| `created_at` | Время создания в формате RFC 3339 с обязательной временной зоной |

API записывает `created_at` в UTC с точностью до микросекунд. Worker принимает любое время RFC 3339 с временной зоной, например `2026-07-30T15:00:00+00:00`.

Worker дополнительно проверяет:

- совпадение Kafka key и `payload.event_id`;
- соответствие topic значению `payload.channel`;
- отсутствие неизвестных полей, имена полей сравниваются с учётом регистра;
- наличие всех полей и корректность их типов и ограничений.

## Kafka topics

Основные topics:

| Topic | Назначение |
|---|---|
| `notification.tg` | Telegram-уведомления |
| `notification.email` | Email-уведомления |
| `notification.sms` | SMS-уведомления |

DLQ topics:

| Topic | Назначение |
|---|---|
| `notification.tg.dlq` | Ошибки Telegram после исчерпания retry |
| `notification.email.dlq` | Ошибки email после исчерпания retry |
| `notification.sms.dlq` | Ошибки SMS после исчерпания retry |
| `notification.invalid.dlq` | Невалидный JSON, схема, key, topic или channel |

Consumer group worker: `notifications`.

Topics создаются Kafka автоматически при первой публикации: producer запрашивает их создание у брокера, как это делал librdkafka. Если в вашем окружении `auto.create.topics.enable=false`, создайте основные и DLQ topics заранее. Если worker запущен раньше, чем появились topics, он ждёт их и начинает читать в течение нескольких секунд после создания.

## Модель данных PostgreSQL

Таблица `notification_log`:

| Колонка | Тип | Назначение |
|---|---|---|
| `event_id` | `VARCHAR`, PK | Идентификатор события и ключ дедупликации |
| `status` | enum | `pending`, `completed` или `failed` |
| `updated_at` | `TIMESTAMPTZ` | Время последнего обновления |
| `locked_until` | `TIMESTAMPTZ`, nullable | Срок действия processing lease |
| `attempt_count` | integer | Количество захватов события для обработки |
| `last_error` | text, nullable | Последняя ожидаемая ошибка отправки |

Состояния:

```mermaid
stateDiagram-v2
    [*] --> pending: acquire lease
    pending --> completed: sender success
    pending --> failed: retries exhausted + DLQ confirmed
    pending --> pending: stale lease reacquired
    failed --> pending: redelivery/reprocessing
    completed --> completed: duplicate event skipped
```

## Быстрый запуск через Docker Compose

### Требования

- Docker Engine или Docker Desktop;
- Docker Compose v2;
- свободные порты `8001`, `5433` и `9094`.

Проверка:

```bash
docker --version
docker compose version
```

### Запуск

Из корня проекта:

```bash
docker compose up --build -d
```

При старте будут подняты:

| Сервис | Внутренний адрес | Адрес с host |
|---|---|---|
| Public API | `public_api:8001` | `http://localhost:8001` |
| Kafka | `kafka:9092` | `localhost:9094` |
| PostgreSQL | `notification_db:5432` | `localhost:5433` |
| Notification worker | отдельный процесс без HTTP-порта | — |

Оба приложения собираются из одного `Dockerfile` с разным аргументом `APP`. Worker при старте сам применяет миграции и только после этого запускает consumer. Если несколько worker стартуют одновременно, миграции применит один из них: на время миграций goose берёт advisory lock в PostgreSQL.

### Проверка состояния

```bash
docker compose ps
docker compose logs -f public_api notification_service
curl http://localhost:8001/live
curl http://localhost:8001/health
```

Полезные команды:

```bash
# Пересобрать и перезапустить приложения после изменения кода
docker compose up --build -d public_api notification_service

# Посмотреть логи Kafka
docker compose logs -f kafka

# Остановить проект, сохранив данные PostgreSQL
docker compose down
```

Удаление volume PostgreSQL необратимо удалит локальные данные проекта:

```bash
docker compose down -v
```

### Переменные Docker Compose

Compose содержит безопасные только для локальной разработки значения по умолчанию. Учётные данные PostgreSQL и параметры retry и lease можно переопределить через переменные shell или корневой файл `.env`:

```dotenv
DB_USER=user
DB_PASS=change-me
DB_NAME=notification_db
KAFKA_DELIVERY_TIMEOUT_MS=10000
KAFKA_MAX_POLL_INTERVAL_MS=180000
PROCESSING_LEASE_SECONDS=60
SENDER_MAX_ATTEMPTS=3
SENDER_ATTEMPT_TIMEOUT_SECONDS=10
SENDER_RETRY_MIN_SECONDS=1
SENDER_RETRY_MAX_SECONDS=5
```

Адреса PostgreSQL и Kafka внутри compose-сети зафиксированы: `notification_db:5432` и `kafka:9092`. Поэтому один и тот же `.env` с `localhost`-адресами подходит и для compose, и для локального запуска.

Не используйте значения по умолчанию для production. Не добавляйте реальные секреты в Git.

Проверить итоговую compose-конфигурацию без запуска:

```bash
docker compose config
```

## Локальный запуск приложений

Нужен Go 1.23 или новее:

```bash
go version
```

Для локального запуска нужны доступные PostgreSQL и Kafka. Их можно установить отдельно либо поднять только инфраструктуру:

```bash
docker compose up -d notification_db kafka
```

Скопируйте пример настроек в корень проекта:

```bash
cp .env.example .env
```

PowerShell:

```powershell
Copy-Item .env.example .env
```

Значения из примера уже указывают на инфраструктуру из compose: PostgreSQL на `localhost:5433`, Kafka на `localhost:9094`. Оба приложения читают `.env` из текущего каталога, поэтому запускайте их из корня проекта. Переменные окружения shell имеют приоритет над `.env`.

Worker применит миграции и начнёт читать Kafka:

```bash
go run ./cmd/worker
```

Public API в другом терминале:

```bash
go run ./cmd/publicapi
```

Собрать бинарники в каталог `bin/`:

```bash
go build -o bin/ ./cmd/...
```

`Ctrl+C` останавливает приложения штатно, с закрытием Kafka-клиентов и пула соединений.

## Конфигурация

### Public API

| Переменная | По умолчанию | Назначение |
|---|---:|---|
| `APP_HOST` | `0.0.0.0` | Адрес HTTP-сервера |
| `APP_PORT` | `8001` | Порт HTTP-сервера |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers через запятую |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `10000` | Предельное время подтверждения публикации |

### Notification worker

| Переменная | По умолчанию | Назначение |
|---|---:|---|
| `DB_HOST` | обязательна | PostgreSQL host |
| `DB_PORT` | обязательна | PostgreSQL port |
| `DB_USER` | обязательна | PostgreSQL user |
| `DB_PASS` | обязательна | PostgreSQL password |
| `DB_NAME` | обязательна | PostgreSQL database |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers через запятую |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `10000` | Таймаут доставки события в DLQ |
| `KAFKA_MAX_POLL_INTERVAL_MS` | `180000` | Rebalance timeout consumer group |
| `PROCESSING_LEASE_SECONDS` | `60` | Срок действия lease |
| `SENDER_MAX_ATTEMPTS` | `3` | Максимальное число попыток sender |
| `SENDER_ATTEMPT_TIMEOUT_SECONDS` | `10` | Таймаут одной попытки |
| `SENDER_RETRY_MIN_SECONDS` | `1` | Минимальная задержка backoff |
| `SENDER_RETRY_MAX_SECONDS` | `5` | Максимальная задержка backoff |

Настройки worker проверяются при старте:

- числовые параметры должны быть положительными, задержки backoff неотрицательными;
- `SENDER_RETRY_MIN_SECONDS` не может превышать `SENDER_RETRY_MAX_SECONDS`;
- lease должен покрывать все sender attempts, задержки и публикацию в DLQ;
- `KAFKA_MAX_POLL_INTERVAL_MS` должен быть больше processing lease.

Некорректная комбинация завершит приложение при старте, предотвращая работу с опасными параметрами.

В franz-go нет локального `max.poll.interval.ms`: heartbeat отправляется в фоне, а пока запись обрабатывается, партицию нельзя отобрать при rebalance. Значение `KAFKA_MAX_POLL_INTERVAL_MS` передаётся как rebalance timeout: столько группа ждёт worker во время rebalance, прежде чем исключить его. librdkafka отправлял брокеру это же значение, поэтому требование «больше processing lease» сохраняет смысл.

## Миграции

Миграции лежат в `migrations/` в виде SQL-файлов, встраиваются в бинарник worker и применяются при каждом его старте. Версия схемы хранится в таблице `goose_db_version`.

Для ручных операций используйте goose CLI с host-машины, база из compose доступна на `localhost:5433`:

```bash
go run github.com/pressly/goose/v3/cmd/goose@v3.26.0 -dir migrations postgres "postgres://user:password@localhost:5433/notification_db" status
```

Вместо `status` можно указать `up` или `down`. `down` откатывает последнюю миграцию.

Откат миграций может удалить колонки или таблицы. Перед downgrade сделайте резервную копию БД.

Если база уже создана Python-версией через Alembic, worker примет её как есть. Миграции написаны через `IF NOT EXISTS`, и goose только запишет свои версии в `goose_db_version`. Таблица `alembic_version` остаётся и ни на что не влияет.

## Тестирование и проверки

Тесты не требуют запущенных контейнеров: Kafka-клиент, БД и sender подменяются через интерфейсы.

```bash
go test ./...
```

Проверяемые сценарии включают:

- ручной commit только после успешной обработки и отсутствие commit при ошибке;
- ошибку Kafka commit;
- декодирование невалидных Kafka-сообщений и poison DLQ;
- несовпадение topic и канала события;
- ограниченный sender retry, расписание backoff и таймаут одной попытки;
- подтверждённую публикацию в channel DLQ;
- восстановление занятого lease;
- отсутствие `failed` и DLQ при неожиданной программной ошибке и при остановке worker;
- стабильный `event_id`, совпадающий с Python-версией;
- валидацию HTTP-запросов, ответы `202`, `400`, `422`, `503` и страницы документации;
- строгую валидацию Kafka-события;
- проверку согласованности настроек worker.

Дополнительные проверки:

```bash
go vet ./...
gofmt -l .
go test -race ./...
```

`-race` требует cgo и C-компилятор. На Windows с Go 1.23 race-детектор может не запуститься с ошибкой `ThreadSanitizer failed to allocate`. Это ограничение окружения, запустите проверку в Linux или WSL.

Полный интеграционный тест требует реальных Kafka и PostgreSQL:

1. поднять compose;
2. дождаться healthy-состояния Kafka, PostgreSQL и API;
3. отправить запрос с уникальным `Idempotency-Key`;
4. проверить логи worker;
5. проверить `notification_log` и соответствующий Kafka topic/DLQ.

Пример проверки таблицы:

```bash
docker compose exec notification_db \
  psql -U user -d notification_db \
  -c "SELECT event_id, status, attempt_count, last_error FROM notification_log;"
```

## Надёжность и гарантии доставки

### Что гарантирует проект

- Kafka producer ожидает подтверждения брокера и не считает вызов успешным сразу после помещения сообщения в локальный буфер.
- Producer использует `acks=all` и идемпотентную запись, которая в franz-go включена по умолчанию.
- Автоматический commit offset отключён.
- Offset подтверждается после `completed` либо после подтверждённой публикации в DLQ и записи `failed`.
- Неудачный Kafka commit считается ошибкой и приводит к перезапуску worker.
- Пока запись обрабатывается, rebalance не отбирает её партицию.
- Просроченный `PENDING` можно обработать повторно.
- Уже завершённый `event_id` пропускается.
- Неожиданные программные ошибки поднимаются до worker и не превращаются в `failed` с commit.
- Повторяется только истёкший таймаут попытки; отмена при остановке worker не считается ошибкой отправки.

### Семантика доставки

Между Kafka и worker используется модель **at-least-once**. Повторная доставка возможна, например если процесс завершился после внешней отправки, но до обновления PostgreSQL или commit Kafka offset.

Для ограничения дублей:

- API формирует стабильный `event_id`;
- все события одного запроса используют этот идентификатор как Kafka key;
- PostgreSQL хранит финальный статус по `event_id`;
- sender получает `event_id` как `idempotencyKey`.

Реальный внешний провайдер должен поддерживать идемпотентность по этому ключу либо проекту потребуется собственный transactional outbox/provider-delivery журнал. Абсолютная exactly-once доставка внешнего побочного эффекта без участия провайдера не гарантируется.

### DLQ

Публикация в DLQ является частью обработки, а не fire-and-forget операцией. При недоступной Kafka исходный offset останется неподтверждённым и событие будет получено снова.

Проект публикует сообщения в DLQ, но не содержит отдельного DLQ consumer. Повторная обработка, алерты и ручной replay должны быть реализованы отдельным сервисом или эксплуатационной процедурой.

## Подключение реальных провайдеров

Точка расширения:

```text
internal/sender/sender.go
```

Адаптер реализует интерфейс `worker.Sender` из `internal/worker/processor.go`:

```go
type Sender interface {
	SendTg(ctx context.Context, ev notification.Event, idempotencyKey string) error
	SendEmail(ctx context.Context, ev notification.Event, idempotencyKey string) error
	SendSMS(ctx context.Context, ev notification.Event, idempotencyKey string) error
}
```

Его экземпляр передаётся в `worker.NewProcessor` в `cmd/worker/main.go` вместо `sender.Stub{}`.

При реализации реального sender:

1. передавайте `ctx` во все сетевые вызовы: через него работают таймаут попытки и остановка worker;
2. передавайте `idempotencyKey` провайдеру, если он поддерживается;
3. не добавляйте бесконечные retry внутри адаптера;
4. оборачивайте только ожидаемые временные ошибки в `notification.ErrTgUnavailable`, `ErrSMTPUnavailable` или `ErrSMSUnavailable`, например `fmt.Errorf("%w: %v", notification.ErrSMTPUnavailable, err)`;
5. не подавляйте ошибки программирования и ошибки конфигурации;
6. не логируйте токены, пароли и полный персональный контент;
7. добавьте contract- и интеграционные тесты;
8. храните секреты во внешнем secret store, а не в репозитории.

После замены заглушек общий pipeline, lease, retry, DLQ и commit-протокол менять не требуется.

## Совместимость с Python-версией

Go- и Python-версии можно заменять по одному сервису, например перевести на Go только API или только worker. У них общие:

- Kafka-топики, consumer group и поля событий и DLQ-сообщений;
- `event_id`: UUID v5 от `notification:<event_type>:<Idempotency-Key>` в пространстве имён URL;
- схема `notification_log`: миграции goose принимают базу, созданную Alembic;
- HTTP-эндпоинты, коды ответов и структура ошибок `422`;
- переменные окружения.

Отличия:

- для ошибок схемы `raw_payload` в poison DLQ содержит исходный JSON, а не Python-представление словаря;
- тексты ошибок валидации и `last_error` отличаются;
- спецификация OpenAPI отдаётся по `/openapi.yaml`, а не `/openapi.json`;
- `APP_HOST` и `APP_PORT` задают адрес сервера напрямую, а `APP_NAME` не используется.
