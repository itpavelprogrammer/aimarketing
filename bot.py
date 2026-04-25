import asyncio
import csv
import io
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, Filter, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, Text, func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


load_dotenv()


def _env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None or not str(val).strip():
        raise RuntimeError(f"Missing environment variable: {name}")
    return str(val).strip()


def _parse_admin_ids(raw: str) -> set[int]:
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.add(int(part))
    return out


def _normalize_database_url(url: str) -> str:
    # Railway sometimes provides postgres://... for some libraries.
    if url.startswith("postgres://"):
        return "postgresql+asyncpg://" + url.removeprefix("postgres://")
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        return "postgresql+asyncpg://" + url.removeprefix("postgresql://")
    return url


BOT_TOKEN = _env("BOT_TOKEN")
ADMIN_IDS = _parse_admin_ids(_env("ADMIN_IDS", ""))
CHANNEL_1 = _env("CHANNEL_1")
CHANNEL_2 = _env("CHANNEL_2")
CHANNEL_1_ID = int(_env("CHANNEL_1_ID"))
CHANNEL_2_ID = int(_env("CHANNEL_2_ID"))
DATABASE_URL = _normalize_database_url(_env("DATABASE_URL"))
LOG_FILE = os.getenv("LOG_FILE")

SUPPORT_USERNAME = "@the_inventor_of_the_bicycle"


def setup_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if LOG_FILE:
        try:
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
            handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
        except Exception:
            # If volume isn't mounted or path invalid, keep stdout logging.
            pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_subscribed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_blocked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class Broadcast(Base):
    __tablename__ = "broadcasts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    admin_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    sent_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


def build_engine() -> AsyncEngine:
    return create_async_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        future=True,
    )


async def init_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


class BroadcastFSM(StatesGroup):
    waiting_content = State()
    confirm = State()


class GreetingFSM(StatesGroup):
    waiting_text = State()


@dataclass(frozen=True)
class App:
    bot: Bot
    dp: Dispatcher
    engine: AsyncEngine
    sessionmaker: async_sessionmaker[AsyncSession]


class StateDebugMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            state: FSMContext | None = data.get("state")
            s = await state.get_state() if state else None
            uid = None
            if hasattr(event, "from_user") and getattr(event, "from_user"):
                uid = event.from_user.id
            logging.info("FSM state | uid=%s | state=%s | event=%s", uid, s, type(event).__name__)
        except Exception:
            pass
        return await handler(event, data)


def admin_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🛠 АДМИН-ПАНЕЛЬ")]],
        resize_keyboard=True,
        selective=True,
    )


def main_reply_kb(is_admin_user: bool) -> ReplyKeyboardMarkup:
    keyboard = [[KeyboardButton(text="🆘 ПОДДЕРЖКА")]]
    if is_admin_user:
        keyboard.append([KeyboardButton(text="🛠 АДМИН-ПАНЕЛЬ")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, selective=True)


def support_inline_kb():
    kb = InlineKeyboardBuilder()
    handle = SUPPORT_USERNAME.strip()
    if handle.startswith("@"):
        kb.button(text="🆘 ПОДДЕРЖКА", url=f"https://t.me/{handle[1:]}")
    else:
        kb.button(text="🆘 ПОДДЕРЖКА", url=f"https://t.me/{handle}")
    kb.adjust(1)
    return kb.as_markup()


def start_inline_kb():
    # Legacy (registration step removed)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Проверить подписку", callback_data="check_subscription")
    kb.adjust(1)
    return kb.as_markup()


def subscribe_inline_kb(channel_1_url: str, channel_2_url: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Канал Николая Василькова", url=channel_1_url)
    kb.button(text="📢 Канал Павла Ганарацкого", url=channel_2_url)
    kb.button(text="✅ Проверить подписку", callback_data="check_subscription")
    kb.adjust(1)
    return kb.as_markup()


def admin_panel_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="admin:stats")
    kb.button(text="✏️ Изменить приветствие", callback_data="admin:greeting")
    kb.button(text="📨 Сделать рассылку", callback_data="admin:broadcast")
    kb.button(text="📥 Экспорт пользователей (CSV)", callback_data="admin:export")
    kb.button(text="❌ Закрыть", callback_data="admin:close")
    kb.adjust(1)
    return kb.as_markup()


def broadcast_confirm_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Отправить всем", callback_data="broadcast:send")
    kb.button(text="❌ Отмена", callback_data="broadcast:cancel")
    kb.adjust(1)
    return kb.as_markup()

def greeting_cancel_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="greeting:cancel")
    kb.adjust(1)
    return kb.as_markup()


START_TEXT = (
    '🤖 <b>МАСТЕР-КЛАСС «ИИ В МАРКЕТИНГЕ»</b>\n\n'
    '🗓️ <b>6 мая, 18:00 · Минск · Бесплатно</b>\n\n'
    'Как использовать искусственный интеллект, чтобы привлекать больше клиентов, автоматизировать маркетинг и расти быстрее конкурентов?\n\n'
    'Приглашаем на практический мастер-класс, где вы не просто узнаете про AI — а примените его для своих задач.\n\n'
    '📍 <a href="https://maps.app.goo.gl/hZEpUR3KY3xJa4nRA">Альфа-хаб</a>, Немига 5\n'
    '(цокольный этаж, на ресепшен — «учебный класс»)\n'
    '⏱️ Длительность: 1,5 часа\n'
    '👥 Для кого: маркетологи, предприниматели, владельцы бизнеса\n\n'
    '<b>Что вас ждёт:</b>\n'
    '💼 Реальные кейсы применения ИИ в маркетинге и продажах\n'
    '🛠 Инструменты, которые работают прямо сейчас\n'
    '⚡️ Практика: вместе создадим воронку лидогенерации с AI\n'
    '💬 Разберём ваши задачи и ответим на вопросы\n\n'
    '<b>Ведущие:</b>\n'
    '🎨 Павел Ганарацкий @proai_by — концептуальный художник, автор курса «Нейромотивация»\n'
    '🚀 Николай Васильков @nicholasvasilkov — AI-предприниматель, основатель Vasilkov.Digital и Smart Response\n\n'
    'Этот мастер-класс — для тех, кто хочет не просто «разобраться в ИИ», а начать использовать его в бизнесе уже на следующий день.\n\n'
    '🔥 <b>Места ограничены — регистрация обязательна</b> 👇\n\n'
    'Увидимся 6 мая! 🎉'
)

DEFAULT_GREETING_KEY = "greeting_html"

SUBSCRIBE_TEXT = "⬇️"

async def get_setting(session: AsyncSession, key: str) -> Optional[str]:
    row = await session.get(Setting, key)
    return row.value if row else None


async def set_setting(session: AsyncSession, key: str, value: str) -> None:
    existing = await session.get(Setting, key)
    now = datetime.now(timezone.utc)
    if existing:
        existing.value = value
        existing.updated_at = now
    else:
        session.add(Setting(key=key, value=value, updated_at=now))
    await session.commit()


async def get_greeting_text(session: AsyncSession) -> str:
    val = await get_setting(session, DEFAULT_GREETING_KEY)
    return val if val else START_TEXT


def _channel_url(handle_or_url: str) -> str:
    s = handle_or_url.strip()
    if s.startswith("https://t.me/"):
        return s
    if s.startswith("@"):
        # Deeplink tends to open Telegram app (instead of t.me preview),
        # but on some clients it may still fallback to browser.
        return f"tg://resolve?domain={s[1:]}"
    return f"https://t.me/{s}"


async def upsert_user(session: AsyncSession, msg: Message) -> None:
    user = msg.from_user
    if user is None:
        return
    existing = await session.get(User, user.id)
    if existing is None:
        session.add(
            User(
                user_id=user.id,
                username=user.username,
                first_name=user.first_name,
                is_subscribed=False,
                is_blocked=False,
            )
        )
        await session.commit()
        logging.info("New user: %s (@%s)", user.id, user.username)
        return

    changed = False
    if existing.username != user.username:
        existing.username = user.username
        changed = True
    if existing.first_name != user.first_name:
        existing.first_name = user.first_name
        changed = True
    if existing.is_blocked:
        existing.is_blocked = False
        changed = True
    if changed:
        await session.commit()


async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def check_membership(
    bot: Bot, user_id: int, channel_ids: Iterable[int]
) -> tuple[bool, list[str], bool]:
    """
    Returns: (ok_all, missing_titles, bot_not_admin)
    """
    missing: list[str] = []
    bot_not_admin = False

    id_to_label = {
        CHANNEL_1_ID: CHANNEL_1,
        CHANNEL_2_ID: CHANNEL_2,
    }

    for ch_id in channel_ids:
        try:
            member = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
        except TelegramBadRequest as e:
            # Most common: bot isn't admin in channel or channel username invalid.
            logging.warning("getChatMember failed for %s: %s", ch_id, e)
            bot_not_admin = True
            missing.append(id_to_label.get(ch_id, str(ch_id)))
            continue

        if member.status not in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}:
            missing.append(id_to_label.get(ch_id, str(ch_id)))

    return (len(missing) == 0 and not bot_not_admin), missing, bot_not_admin


def format_missing(missing: list[str]) -> str:
    lines = ["❗️ Похоже, вы ещё не подписались на все каналы:\n"]
    for ch in missing:
        lines.append(f"❌ {ch}")
    lines.append("\nПодпишитесь и нажмите «Проверить подписку» ещё раз.")
    return "\n".join(lines)


async def stats(session: AsyncSession) -> tuple[int, int]:
    total = (await session.execute(select(func.count()).select_from(User))).scalar_one()
    subscribed = (
        await session.execute(select(func.count()).select_from(User).where(User.is_subscribed.is_(True)))
    ).scalar_one()
    return int(total), int(subscribed)


CHECK_COOLDOWN_SECONDS = 3.0
_last_check_ts: dict[int, float] = {}


def can_check_now(user_id: int) -> bool:
    now = time.monotonic()
    prev = _last_check_ts.get(user_id, 0.0)
    if now - prev < CHECK_COOLDOWN_SECONDS:
        return False
    _last_check_ts[user_id] = now
    return True


def register_handlers(app: App) -> None:
    dp = app.dp
    broadcast_drafts: dict[int, dict[str, int]] = {}
    broadcast_waiting_content: set[int] = set()

    class BroadcastWaitingFilter(Filter):
        async def __call__(self, message: Message) -> bool:
            return bool(message.from_user and message.from_user.id in broadcast_waiting_content)

    class SupportFallbackFilter(Filter):
        async def __call__(self, message: Message, state: FSMContext) -> bool:
            if await state.get_state() is not None:
                return False
            if not message.from_user:
                return False
            uid = message.from_user.id
            if uid in ADMIN_IDS:
                return False
            if uid in broadcast_waiting_content or uid in broadcast_drafts:
                return False
            if message.text and message.text.startswith("/"):
                return False
            return True

    @dp.message(Command("start"))
    async def cmd_start(message: Message) -> None:
        async with app.sessionmaker() as session:
            await upsert_user(session, message)
            greeting = await get_greeting_text(session)

        is_admin_user = bool(message.from_user and await is_admin(message.from_user.id))
        reply_markup = main_reply_kb(is_admin_user)
        await message.answer(
            greeting,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        await message.answer(
            SUBSCRIBE_TEXT,
            parse_mode=ParseMode.HTML,
            reply_markup=subscribe_inline_kb(_channel_url(CHANNEL_1), _channel_url(CHANNEL_2)),
            disable_web_page_preview=True,
        )

    @dp.message(F.text == "🆘 ПОДДЕРЖКА")
    async def support_button(message: Message) -> None:
        await message.answer(
            "Напишите ваш вопрос в поддержку — мы поможем.",
            reply_markup=support_inline_kb(),
            disable_web_page_preview=True,
        )

    @dp.message(Command("admin"))
    async def cmd_admin(message: Message) -> None:
        if not message.from_user or not await is_admin(message.from_user.id):
            return
        await show_admin_panel(message)

    @dp.message(F.text == "🛠 АДМИН-ПАНЕЛЬ")
    async def admin_button(message: Message) -> None:
        if not message.from_user or not await is_admin(message.from_user.id):
            return
        await show_admin_panel(message)

    async def show_admin_panel(message: Message) -> None:
        async with app.sessionmaker() as session:
            total, subscribed = await stats(session)
        text = (
            "🛠 <b>Админ-панель</b>\n\n"
            f"👥 Всего пользователей: <b>{total}</b>\n"
            f"✅ Подписаны на оба канала: <b>{subscribed}</b>"
        )
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=admin_panel_kb())

    @dp.callback_query(F.data == "check_subscription")
    async def cb_check_subscription(call: CallbackQuery) -> None:
        if not call.message or not call.from_user:
            return
        user_id = call.from_user.id
        if not can_check_now(user_id):
            await call.answer("Подождите пару секунд и попробуйте снова.", show_alert=False)
            return

        await call.answer("Проверяю подписку…", show_alert=False)
        try:
            ok, missing, bot_not_admin = await check_membership(app.bot, user_id, [CHANNEL_1_ID, CHANNEL_2_ID])
        except Exception as e:
            logging.exception("Subscription check failed: %s", e)
            await call.message.answer(
                "⚠️ Не получилось выполнить проверку подписки. Попробуйте ещё раз через пару секунд.",
                reply_markup=subscribe_inline_kb(_channel_url(CHANNEL_1), _channel_url(CHANNEL_2)),
                disable_web_page_preview=True,
            )
            return

        if bot_not_admin:
            # Notify admins that bot isn't admin in channel(s).
            note = (
                "⚠️ Не удалось проверить подписку через getChatMember.\n"
                "Проверьте, что бот добавлен администратором в оба канала и что CHANNEL_1/CHANNEL_2 указаны верно.\n\n"
                f"Проблемные каналы: {', '.join(missing) if missing else '—'}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await app.bot.send_message(admin_id, note)
                except Exception:
                    pass

            await call.message.answer(
                "⚠️ Сейчас не могу проверить подписку (техническая проблема). "
                "Мы уже сообщили администраторам — попробуйте чуть позже."
            )
            return

        if ok:
            async with app.sessionmaker() as session:
                await session.execute(
                    update(User).where(User.user_id == user_id).values(is_subscribed=True, is_blocked=False)
                )
                await session.commit()

            await call.message.answer(
                "✅ Отлично, всё готово!\n\n"
                "Вы зарегистрированы на мастер\u2011класс «ИИ в маркетинге» и успешно подписались на каналы.\n\n"
                "📅 До встречи 6 мая в 18:00 в Минске.\n\n"
                "Ближе к событию я пришлю напоминание о старте, детали по месту проведения "
                "и полезные материалы, чтобы выжать максимум пользы из мастер\u2011класса.\n\n"
                "А пока загляните в каналы — там уже ждут материалы по ИИ и маркетингу. 🚀\n\n"
                "Если среди ваших друзей или коллег есть те, кому сейчас актуален ИИ в маркетинге, "
                "поделитесь с ними ссылкой на регистрацию: https://v0-aimasterclass.vercel.app/",
                disable_web_page_preview=True,
            )
        else:
            await call.message.answer(
                format_missing(missing),
                reply_markup=subscribe_inline_kb(_channel_url(CHANNEL_1), _channel_url(CHANNEL_2)),
                disable_web_page_preview=True,
            )

    @dp.callback_query(F.data == "admin:greeting")
    async def cb_admin_greeting(call: CallbackQuery, state: FSMContext) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        await call.answer()
        await state.clear()
        await state.set_state(GreetingFSM.waiting_text)
        if call.message:
            await call.message.answer(
                "✏️ Отправьте новый текст приветствия (HTML допускается).",
                reply_markup=greeting_cancel_kb(),
                disable_web_page_preview=True,
            )

    @dp.callback_query(F.data == "greeting:cancel")
    async def cb_greeting_cancel(call: CallbackQuery, state: FSMContext) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        await call.answer("Ок", show_alert=False)
        await state.clear()
        if call.message:
            await call.message.edit_reply_markup(reply_markup=None)
            await call.message.answer("Отменено.")

    @dp.message(StateFilter(GreetingFSM.waiting_text))
    async def fsm_greeting_text(message: Message, state: FSMContext) -> None:
        if not message.from_user or not await is_admin(message.from_user.id):
            return
        text = message.html_text or message.text or ""
        if not text.strip():
            await message.answer("Текст пустой. Отправьте новый текст приветствия.")
            return
        async with app.sessionmaker() as session:
            await set_setting(session, DEFAULT_GREETING_KEY, text)
        await state.clear()
        await message.answer("✅ Приветственное сообщение обновлено.")

    @dp.message(SupportFallbackFilter())
    async def fallback_support_prompt(message: Message) -> None:
        await message.answer(
            "Если у вас есть вопрос — напишите в поддержку.",
            reply_markup=support_inline_kb(),
            disable_web_page_preview=True,
        )

    @dp.callback_query(F.data == "admin:close")
    async def cb_admin_close(call: CallbackQuery) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        await call.answer()
        if call.message:
            await call.message.delete()

    @dp.callback_query(F.data == "admin:stats")
    async def cb_admin_stats(call: CallbackQuery) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        async with app.sessionmaker() as session:
            total, subscribed = await stats(session)
        await call.answer()
        if call.message:
            await call.message.answer(
                f"📊 <b>Статистика</b>\n\n"
                f"👥 Всего пользователей: <b>{total}</b>\n"
                f"✅ Подписаны на оба канала: <b>{subscribed}</b>",
                parse_mode=ParseMode.HTML,
            )

    @dp.callback_query(F.data == "admin:export")
    async def cb_admin_export(call: CallbackQuery) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        await call.answer("Готовлю CSV…", show_alert=False)

        async with app.sessionmaker() as session:
            rows = (
                await session.execute(
                    select(
                        User.user_id,
                        User.username,
                        User.first_name,
                        User.is_subscribed,
                        User.is_blocked,
                        User.created_at,
                    ).order_by(User.created_at.asc())
                )
            ).all()

        sio = io.StringIO()
        writer = csv.writer(sio)
        writer.writerow(["user_id", "username", "first_name", "is_subscribed", "is_blocked", "created_at"])
        for r in rows:
            writer.writerow(
                [
                    r.user_id,
                    (r.username or ""),
                    (r.first_name or ""),
                    bool(r.is_subscribed),
                    bool(r.is_blocked),
                    r.created_at.isoformat() if r.created_at else "",
                ]
            )

        data = sio.getvalue().encode("utf-8")
        filename = f"users_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        doc = BufferedInputFile(data, filename=filename)
        await app.bot.send_document(call.from_user.id, doc, caption="📥 Экспорт пользователей (CSV)")

    @dp.callback_query(F.data == "admin:broadcast")
    async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        admin_id = call.from_user.id
        logging.info("Admin %s started broadcast (no FSM)", admin_id)
        await call.answer()
        broadcast_waiting_content.add(admin_id)
        broadcast_drafts.pop(admin_id, None)
        if call.message:
            await call.message.answer("Отправьте сообщение для рассылки (текст / фото / видео).")

    @dp.message(BroadcastWaitingFilter())
    async def capture_broadcast_content(message: Message) -> None:
        if not message.from_user or not await is_admin(message.from_user.id):
            return
        if message.chat is None:
            return
        admin_id = message.from_user.id
        if admin_id not in broadcast_waiting_content:
            return

        broadcast_waiting_content.discard(admin_id)
        broadcast_drafts[admin_id] = {"chat_id": message.chat.id, "message_id": message.message_id}
        logging.info(
            "Admin %s provided broadcast content (chat_id=%s message_id=%s)",
            admin_id,
            message.chat.id,
            message.message_id,
        )

        await message.answer("Предпросмотр ниже. Подтвердите отправку всем пользователям:")
        try:
            await app.bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception as e:
            logging.warning("Preview copy_message failed: %s", e)

        await message.answer("Подтвердите отправку рассылки:", reply_markup=broadcast_confirm_kb())

    @dp.callback_query(F.data == "broadcast:cancel")
    async def cb_broadcast_cancel(call: CallbackQuery) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        admin_id = call.from_user.id
        broadcast_waiting_content.discard(admin_id)
        broadcast_drafts.pop(admin_id, None)
        await call.answer("Отменено", show_alert=False)
        if call.message:
            await call.message.edit_reply_markup(reply_markup=None)
            await call.message.answer("Рассылка отменена.")

    @dp.callback_query(F.data == "broadcast:send")
    async def cb_broadcast_send(call: CallbackQuery) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        admin_id = call.from_user.id
        draft = broadcast_drafts.get(admin_id)
        if not draft:
            await call.answer("Ошибка: сообщение не найдено", show_alert=True)
            return

        chat_id = int(draft["chat_id"])
        message_id = int(draft["message_id"])

        await call.answer("Отправляю рассылку…", show_alert=False)
        if call.message:
            await call.message.edit_reply_markup(reply_markup=None)

        async with app.sessionmaker() as session:
            users = (
                await session.execute(select(User.user_id).where(User.is_blocked.is_(False)).order_by(User.created_at))
            ).scalars().all()

        sent = 0
        failed = 0
        blocked = 0
        for user_id in users:
            try:
                await app.bot.copy_message(chat_id=user_id, from_chat_id=chat_id, message_id=message_id)
                sent += 1
            except TelegramForbiddenError:
                blocked += 1
                async with app.sessionmaker() as session:
                    await session.execute(update(User).where(User.user_id == user_id).values(is_blocked=True))
                    await session.commit()
            except TelegramRetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 0.5)
                failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.04)

        broadcast_drafts.pop(admin_id, None)
        report = f"✅ Рассылка завершена!\n\nОтправлено: {sent}\nЗаблокировали бота: {blocked}\nОшибок: {failed}"
        if call.message:
            await call.message.answer(report)


async def main() -> None:
    setup_logging()
    logging.info("Starting bot…")

    engine = build_engine()
    await init_db(engine)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(StateDebugMiddleware())
    dp.callback_query.outer_middleware(StateDebugMiddleware())
    app = App(bot=bot, dp=dp, engine=engine, sessionmaker=sessionmaker)
    register_handlers(app)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        logging.info("Shutdown requested…")
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _request_stop)
        loop.add_signal_handler(signal.SIGINT, _request_stop)
    except NotImplementedError:
        # Windows event loop may not support signals; Railway Linux will.
        pass

    polling_task = asyncio.create_task(dp.start_polling(bot))
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait({polling_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    await bot.session.close()
    await engine.dispose()
    logging.info("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())

