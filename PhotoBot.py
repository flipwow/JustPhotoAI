"""PhotoBot: асинхронная генерация изображений через ProxyAPI.

Зависимости: aiogram 3.x, openai, aiosqlite.
Для PostgreSQL достаточно реализовать интерфейс DatabaseBackend, не меняя обработчики.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
from io import BytesIO
import logging
import os
import secrets
import time
import uuid

from pathlib import Path
from typing import Any, Protocol

import aiosqlite
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, Router
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.enums import ChatAction
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message
from openai import APIConnectionError, APIStatusError, APITimeoutError, AsyncOpenAI

# Конфигурация
BOT_TOKEN = "9051715873:AAF7yg49Pm8FM8SwgIVQDWe_cqGKF_rHikQ"
PROXYAPI_KEY = "sk-y8pOLcyDWyuCHidk3Dch1yCqWlsdi0EH"
ALLOWED_USERS = [8990488378]
PROXYAPI_BASE_URL = "https://api.proxyapi.ru/v1"
IMAGE_MODEL = "black-forest-labs/flux.2-klein-4b"

# Все разрешённые в этом списке пользователи — администраторы с безлимитным доступом.
ADMIN_USERS = set(ALLOWED_USERS)
DATABASE_PATH = Path(__file__).resolve().with_name("photobot.sqlite3")
BILLING_ENABLED = False
DEFAULT_TARIFF = "free"

# Лимиты и параметры тарифов сосредоточены здесь и в таблице tariffs.
# При выключенном биллинге кредиты не ограничивают генерацию и не списываются.
TARIFFS: dict[str, dict[str, Any]] = {
    "free": {"name": "Free", "credits": 10, "price_stars": 0},
    "basic": {"name": "Basic", "credits": 100, "price_stars": 0},
    "pro": {"name": "Pro", "credits": 500, "price_stars": 0},
    "premium": {"name": "Premium", "credits": 2000, "price_stars": 0},
}

MAX_PROMPT_LENGTH = 2000
MAX_REFERENCE_PHOTO_BYTES = 20 * 1024 * 1024
DUPLICATE_PROMPT_WINDOW_SECONDS = 5.0
WEBHOOK_PATH = "/telegram/webhook"
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET") or secrets.token_urlsafe(32)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("photobot")


class InsufficientCredits(Exception):
    """У пользователя недостаточно кредитов."""


class DatabaseBackend(Protocol):
    """Контракт для SQLite и будущего PostgreSQL-адаптера."""

    async def initialize(self) -> None: ...
    async def ensure_user(self, user_id: int, username: str | None) -> None: ...
    async def has_credit(self, user_id: int) -> bool: ...
    async def complete_generation(
        self, request_id: str, user_id: int, prompt: str, admin: bool
    ) -> None: ...
    async def record_failure(
        self, request_id: str, user_id: int, prompt: str, error_code: str
    ) -> None: ...
    async def close(self) -> None: ...


class SQLiteDatabase:
    """Асинхронное SQLite-хранилище с транзакционным списанием кредитов."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        self.connection = await aiosqlite.connect(self.path)
        self.connection.row_factory = aiosqlite.Row
        await self.connection.execute("PRAGMA foreign_keys = ON")
        await self.connection.execute("PRAGMA journal_mode = WAL")
        await self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS tariffs (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                credits INTEGER NOT NULL CHECK (credits >= 0),
                price_stars INTEGER NOT NULL DEFAULT 0 CHECK (price_stars >= 0),
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                tariff_code TEXT NOT NULL DEFAULT 'free' REFERENCES tariffs(code),
                credits INTEGER NOT NULL DEFAULT 0 CHECK (credits >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(user_id),
                tariff_code TEXT NOT NULL REFERENCES tariffs(code),
                status TEXT NOT NULL DEFAULT 'inactive',
                starts_at TEXT,
                expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(user_id),
                request_id TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,
                amount INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'completed',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS generation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL UNIQUE,
                user_id INTEGER NOT NULL REFERENCES users(user_id),
                prompt TEXT NOT NULL,
                status TEXT NOT NULL,
                error_code TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(user_id),
                provider TEXT NOT NULL DEFAULT 'telegram_stars',
                currency TEXT NOT NULL DEFAULT 'XTR',
                amount INTEGER NOT NULL CHECK (amount >= 0),
                payload TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                provider_charge_id TEXT UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                paid_at TEXT
            );
            """)
        for code, tariff in TARIFFS.items():
            await self.connection.execute(
                """INSERT INTO tariffs(code, name, credits, price_stars)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(code) DO UPDATE SET name=excluded.name,
                     credits=excluded.credits, price_stars=excluded.price_stars""",
                (code, tariff["name"], tariff["credits"], tariff["price_stars"]),
            )
        await self.connection.commit()

    def _db(self) -> aiosqlite.Connection:
        if self.connection is None:
            raise RuntimeError("Database is not initialized")
        return self.connection

    async def ensure_user(self, user_id: int, username: str | None) -> None:
        async with self._lock:
            db = self._db()
            await db.execute(
                """INSERT OR IGNORE INTO users(user_id, username, tariff_code, credits)
                   VALUES (?, ?, ?, COALESCE((SELECT credits FROM tariffs WHERE code=?), 0))""",
                (user_id, username, DEFAULT_TARIFF, DEFAULT_TARIFF),
            )
            await db.execute(
                "UPDATE users SET username=?, last_seen_at=CURRENT_TIMESTAMP WHERE user_id=?",
                (username, user_id),
            )
            await db.commit()

    async def has_credit(self, user_id: int) -> bool:
        async with self._lock:
            cursor = await self._db().execute(
                "SELECT credits FROM users WHERE user_id=?", (user_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return row is not None and int(row["credits"]) > 0

    async def complete_generation(
        self, request_id: str, user_id: int, prompt: str, admin: bool
    ) -> None:
        async with self._lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    "SELECT 1 FROM generation_history WHERE request_id=?", (request_id,)
                )
                already_completed = await cursor.fetchone()
                await cursor.close()

                if already_completed is not None:
                    await db.commit()
                    return
                if BILLING_ENABLED and not admin:
                    cursor = await db.execute(
                        "SELECT 1 FROM transactions WHERE request_id=? AND kind='generation_debit'",
                        (request_id,),
                    )
                    already_debited = await cursor.fetchone()
                    await cursor.close()
                    if not already_debited:
                        cursor = await db.execute(
                            "UPDATE users SET credits=credits-1 WHERE user_id=? AND credits>=1",
                            (user_id,),
                        )
                        if cursor.rowcount != 1:
                            await cursor.close()
                            raise InsufficientCredits
                        await cursor.close()
                        await db.execute(
                            "INSERT INTO transactions(user_id, request_id, kind, amount) VALUES (?, ?, 'generation_debit', 1)",
                            (user_id, request_id),
                        )
                await db.execute(
                    "INSERT INTO generation_history(request_id, user_id, prompt, status) VALUES (?, ?, ?, 'success')",
                    (request_id, user_id, prompt),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def record_failure(
        self, request_id: str, user_id: int, prompt: str, error_code: str
    ) -> None:
        async with self._lock:
            await self._db().execute(
                """INSERT OR IGNORE INTO generation_history
                   (request_id, user_id, prompt, status, error_code)
                   VALUES (?, ?, ?, 'failed', ?)""",
                (request_id, user_id, prompt, error_code),
            )
            await self._db().commit()

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None


class AccessMiddleware(BaseMiddleware):
    """Неавторизованные пользователи отсекаются до обработчиков и внешних API."""

    async def __call__(self, handler: Any, event: Any, data: dict[str, Any]) -> Any:
        user = getattr(event, "from_user", None)
        if isinstance(event, Message):
            tracked_chat_messages.setdefault(event.chat.id, set()).add(event.message_id)
        if user is not None and user.id not in ALLOWED_USERS:
            if isinstance(event, Message):
                await event.answer("⛔ Доступ запрещён")
            return None
        return await handler(event, data)


class TelegramStarsScaffold:
    """Неактивный каркас Stars XTR. Не создаёт счета и не принимает оплату."""

    enabled = False
    currency = "XTR"

    async def create_invoice(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("Telegram Stars payments are disabled")


bot = Bot(token=BOT_TOKEN)
dispatcher = Dispatcher()
router = Router()
database: DatabaseBackend = SQLiteDatabase(DATABASE_PATH)
client: AsyncOpenAI | None = None
user_locks: dict[int, asyncio.Lock] = {}
recent_prompts: dict[int, tuple[str, float]] = {}
tracked_chat_messages: dict[int, set[int]] = {}


async def track_bot_messages(
    handler: Any, bot_instance: Bot, method: Any, data: dict[str, Any]
) -> Any:

    result = await handler(bot_instance, method, data)
    if isinstance(result, Message):
        tracked_chat_messages.setdefault(result.chat.id, set()).add(result.message_id)
    return result


async def _ensure_database_user(message: Message) -> None:

    if message.from_user is not None:
        await database.ensure_user(message.from_user.id, message.from_user.username)


@router.message(Command("start"))
async def start_handler(message: Message) -> None:
    await _ensure_database_user(message)
    await message.answer(
        "Привет! Я PhotoBot и создаю изображения по текстовому описанию.\n"
        "Отправьте описание (до 2000 символов), чтобы сгенерировать изображение.\n"
        "Команды: /help — инструкция, /id — ваш Telegram ID, /clear — удалить сообщения, доступные боту."
    )


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    await _ensure_database_user(message)
    await message.answer(
        "Отправьте текстовое описание изображения — бот сгенерирует его.\n"
        "Максимальная длина описания: 2000 символов.\n"
        "/start — приветствие, /help — помощь, /id — ваш Telegram ID, /clear — удалить сообщения, доступные боту.\n"
        "Очистка действует на сообщения, полученные или отправленные ботом с момента его запуска; Telegram ограничивает удаление старых сообщений.\n"
        "Для обработки изображения отправьте фото с текстом в подписи."
    )


@router.message(Command("id"))
async def id_handler(message: Message) -> None:
    if message.from_user is not None:
        await _ensure_database_user(message)
        await message.answer(f"Ваш Telegram ID: {message.from_user.id}")


@router.message(Command("clear"))
async def clear_handler(message: Message) -> None:
    if message.from_user is None:
        return

    # Сбрасываем антидубликатную защиту для пользователя.
    recent_prompts.pop(message.from_user.id, None)

    # Telegram Bot API не позволяет получить список всей истории чата, поэтому
    # удаляем все сообщения, ID которых бот видел в течение текущего запуска.
    message_ids = tracked_chat_messages.pop(message.chat.id, set())
    message_ids.add(message.message_id)
    for message_id in message_ids:
        try:
            await bot.delete_message(message.chat.id, message_id)
        except Exception:
            logger.debug(
                "Could not delete chat message (chat_id=%s, message_id=%s)",
                message.chat.id,
                message_id,
            )


def _error_code(exc: Exception) -> str:
    """Нейтральный код для БД и логов, без токенов/ответов ProxyAPI."""
    if isinstance(exc, (APITimeoutError, asyncio.TimeoutError)):
        return "timeout"
    if isinstance(exc, APIConnectionError):
        return "connection_error"
    if isinstance(exc, APIStatusError):
        return f"proxy_http_{exc.status_code}"
    if isinstance(exc, (binascii.Error, ValueError)):
        return "invalid_image_data"
    return "generation_error"


@router.message()
async def generate_handler(message: Message) -> None:
    global client
    if message.from_user is None:
        return

    reference_photo = message.photo[-1] if message.photo else None
    if reference_photo is not None:
        user_prompt = (message.caption or "").strip()
        if not user_prompt:
            await message.answer(
                "Добавьте описание в подпись к фото, например: «Сделай фон зимним»."
            )
            return
        if (
            reference_photo.file_size is not None
            and reference_photo.file_size > MAX_REFERENCE_PHOTO_BYTES
        ):
            await message.answer("Фото слишком большое. Максимальный размер — 20 МБ.")
            return
    else:
        if not message.text:
            return
        user_prompt = message.text.strip()

    if not user_prompt:
        return
    if len(user_prompt) > MAX_PROMPT_LENGTH:
        await message.answer("Промт слишком длинный. Максимум — 2000 символов.")
        return

    user_id = message.from_user.id
    await _ensure_database_user(message)
    lock = user_locks.setdefault(user_id, asyncio.Lock())
    if lock.locked():
        await message.answer("⏳ Ваш запрос уже обрабатывается. Дождитесь результата.")
        return

    prompt_hash = hashlib.sha256(user_prompt.encode("utf-8")).hexdigest()
    previous = recent_prompts.get(user_id)
    now = time.monotonic()
    if (
        previous
        and previous[0] == prompt_hash
        and now - previous[1] < DUPLICATE_PROMPT_WINDOW_SECONDS
    ):
        await message.answer(
            "⏳ Такой запрос уже недавно отправлен. Подождите немного."
        )
        return

    is_admin = user_id in ADMIN_USERS
    if BILLING_ENABLED and not is_admin and not await database.has_credit(user_id):
        await message.answer("Недостаточно кредитов для генерации.")
        return

    async with lock:
        # Повторная проверка блокирует и запросы, вставшие в очередь одновременно.
        recent = recent_prompts.get(user_id)
        if (
            recent
            and recent[0] == prompt_hash
            and time.monotonic() - recent[1] < DUPLICATE_PROMPT_WINDOW_SECONDS
        ):
            await message.answer(
                "⏳ Такой запрос уже недавно отправлен. Подождите немного."
            )
            return
        # Повторно проверяем кредитный баланс внутри пользовательской блокировки.
        if BILLING_ENABLED and not is_admin and not await database.has_credit(user_id):
            await message.answer("Недостаточно кредитов для генерации.")
            return
        recent_prompts[user_id] = (prompt_hash, time.monotonic())
        request_id = str(uuid.uuid4())
        status_message = await message.answer(
            "🎨 Создаю изображение, пожалуйста, подождите..."
        )
        try:
            if client is None:
                raise RuntimeError("Image client is not initialized")

            await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_PHOTO)
            if reference_photo is not None:
                downloaded = await bot.download(reference_photo.file_id)
                if downloaded is None:
                    raise ValueError("Could not download reference photo")
                reference_bytes = downloaded.getvalue()
                if not reference_bytes:
                    raise ValueError("Reference photo is empty")
                if len(reference_bytes) > MAX_REFERENCE_PHOTO_BYTES:
                    raise ValueError("Reference photo exceeds size limit")
                response = await client.images.edit(
                    model=IMAGE_MODEL,
                    prompt=user_prompt,
                    image=("reference.jpg", BytesIO(reference_bytes), "image/jpeg"),
                    n=1,
                )
            else:
                response = await client.images.generate(
                    model=IMAGE_MODEL,
                    prompt=user_prompt,
                    n=1,
                )
            if not response.data or not response.data[0].b64_json:
                raise ValueError("Image response is empty")

            image_bytes = base64.b64decode(response.data[0].b64_json, validate=True)
            if not image_bytes:
                raise ValueError("Decoded image is empty")
            image = BufferedInputFile(image_bytes, filename="photobot.png")
            await database.complete_generation(
                request_id, user_id, user_prompt, is_admin
            )
            caption = user_prompt[:1024]
            await message.answer_photo(image, caption=caption)
            if len(user_prompt) > 1024:
                await message.answer(user_prompt)
            try:
                await status_message.delete()
            except Exception:
                logger.warning(
                    "Could not delete progress message (user_id=%s)", user_id
                )
        except InsufficientCredits:
            await database.record_failure(
                request_id, user_id, user_prompt, "insufficient_credits"
            )
            await message.answer("Недостаточно кредитов для генерации.")
            try:
                await status_message.delete()
            except Exception:
                pass
        except (
            APITimeoutError,
            asyncio.TimeoutError,
            APIConnectionError,
            APIStatusError,
            ValueError,
            binascii.Error,
            RuntimeError,
        ) as exc:
            code = _error_code(exc)
            logger.warning(
                "Image generation failed (user_id=%s, code=%s)", user_id, code
            )
            try:
                await database.record_failure(request_id, user_id, user_prompt, code)
            except Exception:
                logger.exception(
                    "Could not record generation failure (user_id=%s)", user_id
                )
            await message.answer(
                "Не удалось создать изображение из-за временной ошибки. Попробуйте позже."
            )
            try:
                await status_message.delete()
            except Exception:
                pass
        except Exception as exc:
            code = _error_code(exc)
            logger.error(
                "Unexpected generation error (user_id=%s, code=%s)", user_id, code
            )
            try:
                await database.record_failure(request_id, user_id, user_prompt, code)
            except Exception:
                logger.exception(
                    "Could not record generation failure (user_id=%s)", user_id
                )
            await message.answer("Не удалось обработать запрос. Попробуйте позже.")
            try:
                await status_message.delete()
            except Exception:
                pass


dispatcher.include_router(router)
dispatcher.message.middleware(AccessMiddleware())
bot.session.middleware.register(track_bot_messages)


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def initialize_bot(app: web.Application) -> None:
    """Подключается к БД и Telegram в фоне, чтобы HTTP-порт открылся сразу."""
    global client
    while True:
        try:
            if database.connection is None:
                await database.initialize()
            if client is None:
                client = AsyncOpenAI(
                    api_key=PROXYAPI_KEY,
                    base_url=PROXYAPI_BASE_URL,
                    timeout=180.0,
                )
            hostname = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
            webhook_url = os.environ.get("WEBHOOK_URL")
            if not webhook_url:
                if not hostname:
                    raise RuntimeError("missing_public_webhook_hostname")
                webhook_url = f"https://{hostname}{WEBHOOK_PATH}"
            await bot.set_webhook(
                url=webhook_url,
                secret_token=WEBHOOK_SECRET,
                allowed_updates=dispatcher.resolve_used_update_types(),
            )
            logger.info("Telegram webhook configured")
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Не логируем текст исключения: в нём не должны появиться секреты.
            logger.error(
                "Webhook initialization failed (error_type=%s); retrying in 10 seconds",
                type(exc).__name__,
            )
            if client is not None:
                await client.close()
                client = None
            if database.connection is not None:
                await database.close()
            await asyncio.sleep(10)


async def on_startup(app: web.Application) -> None:
    app["bot_initialization_task"] = asyncio.create_task(initialize_bot(app))


async def on_cleanup(app: web.Application) -> None:
    global client
    task = app.get("bot_initialization_task")
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    if client is not None:
        await client.close()
        client = None
    await database.close()
    await bot.session.close()


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health_handler)

    SimpleRequestHandler(
        dispatcher=dispatcher,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
    ).register(app, path=WEBHOOK_PATH)
    setup_application(app, dispatcher, bot=bot)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    port = int(os.environ.get("PORT", "10000"))
    web.run_app(create_app(), host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
