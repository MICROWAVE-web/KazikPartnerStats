from typing import Dict

from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import BOT_TOKEN, PREFIX, ALLOWED_USER_IDS
from db import init_db, get_reward, set_reward, aggregate_by_btag

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Simple in-memory state to ask for reward input
awaiting_reward_input: Dict[int, bool] = {}


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Генерировать ссылки", callback_data="menu_generate")],
        [InlineKeyboardButton(text="💰 Установить вознаграждение", callback_data="menu_set_reward")],
        [
            InlineKeyboardButton(text="📊 Все время", callback_data="report_all"),
            InlineKeyboardButton(text="⏰ Час", callback_data="report_hour"),
        ],
        [
            InlineKeyboardButton(text="📆 День", callback_data="report_day"),
            InlineKeyboardButton(text="📅 Неделя", callback_data="report_week"),
        ],
        [
            InlineKeyboardButton(text="🗓️ Прошлая неделя", callback_data="report_last_week"),
        ],
        [InlineKeyboardButton(text="↻ Обновить", callback_data="menu_refresh")],
    ])


def make_links_text(user_id: int) -> str:
    return (
        "Ссылка для регистрации:\n"
        f"<code>{PREFIX}/{user_id}/registration?btag=${{btag}}</code>\n\n"
        "Ссылка для первого депозита:\n"
        f"<code>{PREFIX}/{user_id}/firstdep?btag=${{btag}}</code>"
    )


def format_report(user_id: int, period: str) -> str:
    mapping = {"all": "Все время", "hour": "Час", "day": "День", "week": "Неделя", "last_week": "Прошлая неделя"}
    title = mapping.get(period, "Все время")
    stats = aggregate_by_btag(user_id, period)
    if not stats:
        return f"📊 Отчет ({title})\n\nНет данных."
    lines = [f"📊 Отчет ({title})", "", "btag | Реги | Кол-во депов"]
    total_regs = total_deps = 0
    for btag, (regs, deps, reward_sum) in sorted(stats.items()):
        lines.append(
            "\n".join([
                f"<blockquote>BTag: {btag or '-'}",
                f"Реги: {regs}",
                f"Депы: {deps}",
                f"Сумма: {round(reward_sum, 2)}</blockquote>",
            ])
        )
        lines.append("")  # пустая строка между блоками
        # lines.append(f"{btag or '-'} | {regs} | {deps}")
        total_regs += regs
        total_deps += deps
    lines += ["", f"Итого: регистрации {total_regs}, депозиты {total_deps}"]

    return "\n".join([
        f"📊 Отчет ({title})",
        "",
        "🤑 ==== ROYAL ==== 🤑",
        *lines,
        "",
        "Нет данных.",
    ])


def check_access(user_id: int) -> bool:
    return True
    """Проверяет, есть ли у пользователя доступ к боту"""
    if not ALLOWED_USER_IDS:
        return True  # Если список пуст, доступ открыт для всех
    return user_id in ALLOWED_USER_IDS


@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not check_access(message.from_user.id):
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    init_db()
    current = get_reward(message.from_user.id)
    text = (
        "👋 Добро пожаловать! Это партнерский бот.\n\n"
        f"Текущее вознаграждение за первый депозит: {current:.2f}\n\n"
        "Используйте меню ниже."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@dp.message(Command("generate"))
async def cmd_generate(message: Message):
    if not check_access(message.from_user.id):
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    text = make_links_text(message.from_user.id)
    await message.answer(text, parse_mode="HTML")


@dp.callback_query(F.data == "menu_generate")
async def on_menu_generate(callback: CallbackQuery):
    if not check_access(callback.from_user.id):
        await callback.answer("❌ У вас нет доступа к этому боту.", show_alert=True)
        return
    await callback.message.edit_text(make_links_text(callback.from_user.id), reply_markup=main_menu_keyboard(),
                                     parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "menu_set_reward")
async def on_set_reward(callback: CallbackQuery):
    if not check_access(callback.from_user.id):
        await callback.answer("❌ У вас нет доступа к этому боту.", show_alert=True)
        return
    awaiting_reward_input[callback.from_user.id] = True
    await callback.message.edit_text(
        "Введите новое значение вознаграждения (число, например 10 или 12.5)",
        reply_markup=main_menu_keyboard(),
    )
    await callback.answer()


@dp.message()
async def on_any_message(message: Message):
    if not check_access(message.from_user.id):
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    if awaiting_reward_input.get(message.from_user.id):
        text = message.text.strip().replace(",", ".")
        try:
            value = float(text)
        except Exception:
            await message.reply("Пожалуйста, введите корректное число.")
            return
        set_reward(message.from_user.id, value)
        awaiting_reward_input.pop(message.from_user.id, None)
        await message.reply(f"Готово. Новое вознаграждение: {value:.2f}", reply_markup=main_menu_keyboard())


@dp.callback_query(
    F.data.in_({"report_all", "report_hour", "report_day", "report_week", "report_last_week", "menu_refresh"}))
async def on_reports(callback: CallbackQuery):
    if not check_access(callback.from_user.id):
        await callback.answer("❌ У вас нет доступа к этому боту.", show_alert=True)
        return
    data = callback.data
    period_map = {
        "report_all": "all",
        "report_hour": "hour",
        "report_day": "day",
        "report_week": "week",
        "report_last_week": "last_week",
        "menu_refresh": "all",
    }
    period = period_map.get(data, "all")
    text = format_report(callback.from_user.id, period)
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard())
    await callback.answer()


async def run_bot():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set in environment")
    init_db()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
