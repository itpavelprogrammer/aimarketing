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
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
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


def admin_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🛠 АДМИН-ПАНЕЛЬ")]],
        resize_keyboard=True,
        selective=True,
    )


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

SUBSCRIBE_TEXT = (
    "🎁 <b>Остался последний шаг!</b>\n\n"
    "Чтобы получать полезные материалы по ИИ и организационную информацию по мастер-классу, подпишитесь на наши каналы:\n\n"
    "📢 Канал Николая Василькова\n"
    "📢 Канал Павла Ганарацкого\n\n"
    "После подписки нажмите кнопку «Проверить подписку» ⬇️"
)

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

    @dp.message(Command("start"))
    async def cmd_start(message: Message) -> None:
        async with app.sessionmaker() as session:
            await upsert_user(session, message)
            greeting = await get_greeting_text(session)

        reply_markup = admin_reply_kb() if message.from_user and await is_admin(message.from_user.id) else None
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
                "🎉 Отлично! Вы успешно подписались.\n\n"
                "До встречи 6 мая на мастер-классе «ИИ в маркетинге»! 🚀\n\n"
                "Мы пришлём вам напоминание и полезные материалы перед мероприятием."
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
                disable_web_page_preview=True,
            )

    @dp.message(GreetingFSM.waiting_text)
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
        await call.answer()
        await state.clear()
        await state.set_state(BroadcastFSM.waiting_content)
        if call.message:
            await call.message.answer("Отправьте сообщение для рассылки (текст / фото / видео).")

    @dp.message(BroadcastFSM.waiting_content)
    async def fsm_broadcast_content(message: Message, state: FSMContext) -> None:
        if not message.from_user or not await is_admin(message.from_user.id):
            return
        if message.chat is None:
            return

        await state.update_data(
            from_chat_id=message.chat.id,
            message_id=message.message_id,
            preview_text=(message.text or message.caption or ""),
        )
        await state.set_state(BroadcastFSM.confirm)

        await message.answer("Предпросмотр ниже. Подтвердите отправку всем пользователям:")
        try:
            await app.bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception:
            # Fallback: at least show text.
            if message.text:
                await message.answer(message.text)

        await message.answer("✅ Отправлять?", reply_markup=broadcast_confirm_kb())

    @dp.callback_query(F.data == "broadcast:cancel")
    async def cb_broadcast_cancel(call: CallbackQuery, state: FSMContext) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        await call.answer("Отменено", show_alert=False)
        await state.clear()
        if call.message:
            await call.message.edit_reply_markup(reply_markup=None)

    @dp.callback_query(F.data == "broadcast:send")
    async def cb_broadcast_send(call: CallbackQuery, state: FSMContext) -> None:
        if not call.from_user or not await is_admin(call.from_user.id):
            await call.answer()
            return
        data = await state.get_data()
        from_chat_id = data.get("from_chat_id")
        message_id = data.get("message_id")
        preview_text = data.get("preview_text") or ""
        if not from_chat_id or not message_id:
            await call.answer("Нет данных рассылки. Начните заново.", show_alert=True)
            await state.clear()
            return

        await call.answer("Запускаю рассылку…", show_alert=False)
        if call.message:
            await call.message.edit_reply_markup(reply_markup=None)

        async with app.sessionmaker() as session:
            users = (
                await session.execute(select(User.user_id).where(User.is_blocked.is_(False)).order_by(User.created_at))
            ).scalars().all()

        sent = 0
        blocked = 0
        failed = 0

        for uid in users:
            try:
                await app.bot.copy_message(chat_id=uid, from_chat_id=from_chat_id, message_id=message_id)
                sent += 1
            except TelegramForbiddenError:
                blocked += 1
                async with app.sessionmaker() as session:
                    await session.execute(update(User).where(User.user_id == uid).values(is_blocked=True))
                    await session.commit()
            except TelegramRetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 0.2)
                failed += 1
            except Exception:
                failed += 1

            await asyncio.sleep(0.04)  # ~25 msg/sec

        async with app.sessionmaker() as session:
            session.add(
                Broadcast(
                    admin_id=call.from_user.id,
                    content=preview_text[:4000],
                    sent_count=sent,
                    failed_count=failed,
                )
            )
            await session.commit()

        report = (
            "✅ <b>Рассылка завершена</b>\n"
            f"📤 Отправлено: <b>{sent}</b>\n"
            f"🚫 Заблокировали бота: <b>{blocked}</b>\n"
            f"⚠️ Ошибок: <b>{failed}</b>"
        )
        await app.bot.send_message(call.from_user.id, report, parse_mode=ParseMode.HTML)
        await state.clear()


async def main() -> None:
    setup_logging()
    logging.info("Starting bot…")

    engine = build_engine()
    await init_db(engine)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
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

