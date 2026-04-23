# Telegram бот «Подписка + регистрация + админ-рассылка»

Бот на **Python 3.11+ / aiogram 3.x** для регистрации участников мастер-класса и проверки подписки на 2 канала, с **админ-панелью**, **рассылками** и хранением данных в **PostgreSQL (Railway plugin)**.

## Переменные окружения

Задаются в Railway → **Variables** (не хранить в репозитории).

- **BOT_TOKEN**: токен из BotFather
- **ADMIN_IDS**: Telegram ID админов через запятую (пример: `123,456`)
- **CHANNEL_1**: `@nicholasvasilkov`
- **CHANNEL_2**: `@proai_by`
- **DATABASE_URL**: создаётся автоматически после подключения Railway PostgreSQL
- **LOG_FILE** (опционально): например `/data/bot.log` (если подключён Railway Volume к `/data`)

Пример в `.env.example`.

## Локальный запуск (для разработки)

1. Установите Python 3.11+
2. Установите зависимости:

```bash
pip install -r requirements.txt
```

3. Создайте `.env` по образцу `.env.example`
4. Запустите:

```bash
python bot.py
```

## Деплой на Railway (worker + PostgreSQL)

1. Создайте проект в Railway.
2. Подключите GitHub-репозиторий (Deployments → подключение репозитория).
3. Добавьте базу: **New → Database → PostgreSQL**.
4. В Variables добавьте:
   - `BOT_TOKEN`
   - `ADMIN_IDS`
   - `CHANNEL_1`
   - `CHANNEL_2`
   - `DATABASE_URL` появится автоматически от PostgreSQL-плагина
5. (Опционально) Добавьте Volume и примонтируйте к `/data`, затем задайте `LOG_FILE=/data/bot.log`.
6. Railway запустит воркер из `Procfile`:
   - `worker: python bot.py`

## Важно для проверки подписки

Для корректной проверки через `getChatMember` бот **должен быть добавлен администратором** в оба канала.

## Что хранится в БД

Таблица `users`:
- `user_id`, `username`, `first_name`, `is_subscribed`, `is_blocked`, `created_at`

Таблица `broadcasts` (история рассылок):
- `admin_id`, `content`, `sent_count`, `failed_count`, `created_at`

Таблицы создаются автоматически при старте (через `Base.metadata.create_all`).

