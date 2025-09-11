import asyncio
from typing import Dict

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from config import BOT_TOKEN, PREFIX
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
            InlineKeyboardButton(text="🗓️ Месяц", callback_data="report_month"),
        ],
        [
            InlineKeyboardButton(text="📅 Неделя", callback_data="report_week"),
            InlineKeyboardButton(text="📆 День", callback_data="report_day"),
        ],
        [InlineKeyboardButton(text="↻ Обновить", callback_data="menu_refresh")],
    ])


def make_links_text(user_id: int) -> str:
    return (
        "Ссылка для регистрации:\n"
        f"<code>{PREFIX}/{user_id}/registration?player_id=${{btag}}</code>\n\n"
        "Ссылка для первого депозита:\n"
        f"<code>{PREFIX}/{user_id}/firstdep?player_id=${{btag}}</code>"
    )


def format_report(user_id: int, period: str) -> str:
    mapping = {"all": "Все время", "month": "Месяц", "week": "Неделя", "day": "День"}
    title = mapping.get(period, "Все время")
    stats = aggregate_by_btag(user_id, period)
    if not stats:
        return f"📊 Отчет ({title})\n\nНет данных."
    lines = [f"📊 Отчет ({title})", "", "btag | Реги | Первых депов | Сумма вознаграждений"]
    total_regs = total_deps = 0
    total_reward = 0.0
    for btag, (regs, deps, reward_sum) in sorted(stats.items()):
        lines.append(f"{btag or '-'} | {regs} | {deps} | {reward_sum:.2f}")
        total_regs += regs
        total_deps += deps
        total_reward += reward_sum
    lines += ["", f"Итого: регистрации {total_regs}, первые депозиты {total_deps}, вознаграждение {total_reward:.2f}"]
    return "\n".join(lines)


@dp.message(Command("start"))
async def cmd_start(message: Message):
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
    text = make_links_text(message.from_user.id)
    await message.answer(text, parse_mode="HTML")


@dp.callback_query(F.data == "menu_generate")
async def on_menu_generate(callback: CallbackQuery):
    await callback.message.edit_text(make_links_text(callback.from_user.id), reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "menu_set_reward")
async def on_set_reward(callback: CallbackQuery):
    awaiting_reward_input[callback.from_user.id] = True
    await callback.message.edit_text(
        "Введите новое значение вознаграждения (число, например 10 или 12.5)",
        reply_markup=main_menu_keyboard(),
    )
    await callback.answer()


@dp.message()
async def on_any_message(message: Message):
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


@dp.callback_query(F.data.in_({"report_all", "report_month", "report_week", "report_day", "menu_refresh"}))
async def on_reports(callback: CallbackQuery):
    data = callback.data
    period_map = {
        "report_all": "all",
        "report_month": "month",
        "report_week": "week",
        "report_day": "day",
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


