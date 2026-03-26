# SpreadCalculator

Сервис слушает события из `quotes:events`, пересчитывает спреды между заданными биржами и пишет результат в Redis.

## Как работает

1. Читает котировки из `quotes:{exchange}` (`HASH`, `field=asset_id`, `value=JSON`).
2. Читает stream `quotes:events` и пересчитывает спреды только для событий с `type=quotes_updated`.
3. Пишет спреды в `spreads:{exchange_buy}:{exchange_sell}` (`HASH`, `field=asset_id`, `value=JSON`).
4. Удаляет спреды, которые больше невалидны.
5. Публикует изменения спредов в `spreads:events`.

## Зависимости

- Python 3.13+
- Redis
- Poetry (рекомендуется) или pip

## Установка

### Poetry

```bash
poetry install
```

### pip

```bash
python -m pip install -r requirements.txt
```

## Конфигурация

Скопируйте `.env.example` в `.env` и настройте:

- `SPREAD_EXCHANGES` - список бирж через запятую, например `bitget,kucoin`
- `INPUT_REDIS_*` - Redis с котировками
- `OUTPUT_REDIS_*` - Redis для записи спредов
- `QUOTE_KEY_PREFIX`, `QUOTE_EVENTS_KEY`, `QUOTE_UPDATED_TYPE`
- `SPREAD_KEY_PREFIX`, `SPREAD_EVENTS_KEY`
- `STREAM_BLOCK_MS`, `STREAM_BATCH_SIZE`, `STREAM_MAX_LEN`
- `LOG_LEVEL`, `LOG_TO_FILE`

## Запуск

```bash
python -m src.main
```

## Формат записей

### Вход (quotes)

- key: `quotes:{exchange}`
- field: `{address}:{network}` (`asset_id`)
- value: JSON котировки

### Выход (spreads)

- key: `spreads:{exchange_buy}:{exchange_sell}`
- field: `{address}:{network}` (`asset_id`)
- value: JSON спреда

## Docker

Redis в `docker-compose.yml` не поднимается, используется уже запущенный внешний Redis.
Пакеты `easy_logging`, `quotes`, `spreads` берутся из директории `vendor/`.

1. Сборка и запуск:

```bash
docker compose up --build -d
```

2. Если Redis на хост-машине, обычно нужно в `.env` указать:
   - `INPUT_REDIS_HOST=host.docker.internal`
   - `OUTPUT_REDIS_HOST=host.docker.internal`

3. Логи:

```bash
docker compose logs -f spread-calculator
```
