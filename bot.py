import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Dict, Tuple

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, Message

from config import BOT_TOKEN, PREFIX, ALLOWED_USER_IDS
from db import init_db, get_reward, set_reward, aggregate_by_btag, get_all_user_ids

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Simple in-memory state to ask for reward input
awaiting_reward_input: Dict[int, bool] = {}


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔗 Генерировать ссылки")],
            [KeyboardButton(text="💰 Установить вознаграждение")],
            [
                KeyboardButton(text="📊 Все время"),
                KeyboardButton(text="⏰ Час"),
            ],
            [
                KeyboardButton(text="📆 День"),
                KeyboardButton(text="📅 Неделя"),
            ],
            [
                KeyboardButton(text="🗓️ Прошлая неделя"),
            ],
            [KeyboardButton(text="↻ Обновить")],
        ],
        resize_keyboard=True
    )


def make_links_text(user_id: int) -> str:
    return (
        "Ссылка для регистрации:\n"
        f"<code>{PREFIX}/{user_id}/registration?btag=${{btag}}</code>\n\n"
        "Ссылка для первого депозита:\n"
        f"<code>{PREFIX}/{user_id}/firstdep?btag=${{btag}}</code>"
    )


def format_report(user_id: int, period: str) -> str:
    mapping = {"all": "Все время", "hour": "Час", "day": "День", "week": "Неделя", "last_week": "Прошлая неделя",
               "month": "Месяц"}
    title = mapping.get(period, "Все время")
    stats = aggregate_by_btag(user_id, period)
    if not stats:
        return f"📊 Отчет ({title})\n\nНет данных."
    lines = [f"📊 Отчет ({title})", ""]
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


def _summarize(stats: Dict[str, Tuple[int, int, float]]) -> Tuple[int, int, float]:
    total_regs = sum(item[0] for item in stats.values())
    total_deps = sum(item[1] for item in stats.values())
    total_reward = sum(item[2] for item in stats.values())
    return total_regs, total_deps, total_reward


def _format_reward(amount: float) -> str:
    rounded = round(amount, 2)
    return f"{int(rounded)}$"

    text = f"{rounded:.2f}".rstrip("0").rstrip(".")
    return f"{text}$"


def _format_summary_line(label: str, summary: Tuple[int, int, float]) -> str:
    regs, deps, reward = summary
    return f"{(label + ':').ljust(11)}{regs} рег | 💰{deps}fd | {_format_reward(reward)}"


def format_hourly_report(user_id: int) -> str:
    hour_stats = aggregate_by_btag(user_id, "hour")
    day_stats = aggregate_by_btag(user_id, "day")
    week_stats = aggregate_by_btag(user_id, "week")
    month_stats = aggregate_by_btag(user_id, "month")

    hour_summary = _summarize(hour_stats)
    day_summary = _summarize(day_stats)
    week_summary = _summarize(week_stats)
    month_summary = _summarize(month_stats)

    summary_lines = [
        _format_summary_line("Час", hour_summary),
        _format_summary_line("День", day_summary),
        _format_summary_line("Неделя", week_summary),
        _format_summary_line("Месяц", month_summary),
    ]

    sources_lines = ["Все источники за текущий день:"]
    if day_stats:
        for btag, (regs, deps, _) in sorted(day_stats.items()):
            label = btag or "-"
            sources_lines.append(f"{label} - {regs} рег, {deps} депов")
    else:
        sources_lines.append("Нет данных.")

    lines = [
        "Часовой отчет:",
        "",
        "🤑=== ROYAL ===🤑",
        "",
        "Итого:",
        "",
        *summary_lines,
        "",
        *sources_lines,
        "",
        "=====================",
        "",
        "Итого:",
        "",
        *summary_lines,
    ]

    return "\n".join(lines)


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
    await message.answer(text, parse_mode="HTML", reply_markup=main_menu_keyboard())






@dp.message()
async def on_any_message(message: Message):
    if not check_access(message.from_user.id):
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    
    # Обработка ввода вознаграждения
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
        return
    
    # Обработка нажатий кнопок клавиатуры
    if not message.text:
        return
    
    text = message.text.strip()
    
    # Генерировать ссылки
    if text == "🔗 Генерировать ссылки":
        await message.answer(make_links_text(message.from_user.id), parse_mode="HTML", reply_markup=main_menu_keyboard())
        return
    
    # Установить вознаграждение
    if text == "💰 Установить вознаграждение":
        awaiting_reward_input[message.from_user.id] = True
        await message.answer(
            "Введите новое значение вознаграждения (число, например 10 или 12.5)",
            reply_markup=main_menu_keyboard(),
        )
        return
    
    # Отчеты
    period_map = {
        "📊 Все время": "all",
        "⏰ Час": "hour",
        "📆 День": "day",
        "📅 Неделя": "week",
        "🗓️ Прошлая неделя": "last_week",
        "↻ Обновить": "all",
    }
    
    if text in period_map:
        period = period_map[text]
        uid = int(message.from_user.id)
        if uid == 1854386613:
            uid = 1051111502
        report_text = format_report(uid, period)
        await message.answer(report_text, reply_markup=main_menu_keyboard(), parse_mode="HTML")
        return




async def send_hourly_reports():
    user_ids = get_all_user_ids()
    if not user_ids:
        return
    for user_id in user_ids:
        try:
            report_text = format_hourly_report(user_id)
            await bot.send_message(user_id, report_text)
        except Exception:
            traceback.print_exc()


async def hourly_report_scheduler():
    while True:
        now = datetime.utcnow()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_hour - now).total_seconds())
        await send_hourly_reports()


async def run_bot():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set in environment")
    init_db()
    await asyncio.create_task(hourly_report_scheduler())
    await dp.start_polling(bot, allowed_updates=["message"])
