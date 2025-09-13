import asyncio
from typing import Dict

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from config import BOT_TOKEN, PREFIX
from db import init_db, get_reward, set_reward, aggregate_by_btag, get_period_range, grant_access, revoke_access, list_viewers, list_available_owners, list_all_user_ids


bot = Bot(BOT_TOKEN)
dp = Dispatcher()


# Simple in-memory state to ask for reward input
awaiting_reward_input: Dict[int, bool] = {}
awaiting_grant_input: Dict[int, bool] = {}
awaiting_revoke_input: Dict[int, bool] = {}
awaiting_view_owner_input: Dict[int, bool] = {}


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Генерировать ссылки", callback_data="menu_generate")],
        [InlineKeyboardButton(text="💰 Установить вознаграждение", callback_data="menu_set_reward")],
        [InlineKeyboardButton(text="👥 Доступ к статистике", callback_data="menu_share")],
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
    mapping = {"all": "Все время", "hour": "Час", "month": "Месяц", "week": "Неделя", "day": "День"}
    title = mapping.get(period, "Все время")
    stats = aggregate_by_btag(user_id, period)
    if not stats:
        period_range = get_period_range(user_id, period)
        range_text = ""
        if period_range is not None:
            start, end = period_range
            range_text = f"\n<blockquote>Период: {start:%Y-%m-%d %H:%M} — {end:%Y-%m-%d %H:%M} UTC</blockquote>"
        # Even if no data for this period, still show the fixed summaries
        totals_lines = _fixed_period_totals_lines(user_id)
        return "\n".join([
            f"📊 Отчет ({title})",
            "",
            "🤑 ==== ROYAL ==== 🤑",
            *totals_lines,
            "",
            "Нет данных.",
        ])

    total_regs = total_deps = 0
    total_reward = 0.0
    lines = []
    # Итоговый блок
    lines.append("🤑 ==== ROYAL ==== 🤑")
    # lines.append(
    #    "\n".join([
    #        f"Регистрации: {total_regs}",
    #        f"Первые депозиты: {total_deps}",
    #        f"Вознаграждение: {round(total_reward, 2)}",
    #    ])
    # )
    # Always show Hour/Day/Week/Month totals
    lines.extend(_fixed_period_totals_lines(user_id))
    lines.append("")
    lines.append(f"📊 Отчет ({title})")
    period_range = get_period_range(user_id, period)
    if period_range is not None:
        start, end = period_range
        # lines.append(f"<blockquote>Период: {start:%Y-%m-%d %H:%M} — {end:%Y-%m-%d %H:%M} UTC</blockquote>")
    lines.append("")

    # По каждому BTag
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

        total_regs += regs
        total_deps += deps
        total_reward += reward_sum

    # Итоговый блок
    lines.append("🤑 ==== ROYAL ==== 🤑")
    #lines.append(
    #    "\n".join([
    #        f"Регистрации: {total_regs}",
    #        f"Первые депозиты: {total_deps}",
    #        f"Вознаграждение: {round(total_reward, 2)}",
    #    ])
    #)
    # Always show Hour/Day/Week/Month totals
    lines.extend(_fixed_period_totals_lines(user_id))

    return "\n".join(lines)


def _fixed_period_totals_lines(user_id: int) -> list[str]:
    def summarize(period_key: str) -> tuple[int, int, float]:
        period_stats = aggregate_by_btag(user_id, period_key)
        regs = sum(v[0] for v in period_stats.values())
        deps = sum(v[1] for v in period_stats.values())
        reward = round(sum(v[2] for v in period_stats.values()), 2)
        return regs, deps, reward

    hour_regs, hour_deps, hour_reward = summarize("hour")
    day_regs, day_deps, day_reward = summarize("day")
    week_regs, week_deps, week_reward = summarize("week")
    month_regs, month_deps, month_reward = summarize("month")

    return [
        f"Час: {hour_regs} рег | 💰{hour_deps}fd | {hour_reward}",
        f"День: {day_regs} рег | 💰{day_deps}fd | {day_reward}",
        f"Неделя: {week_regs} рег | 💰{week_deps}fd | {week_reward}",
        f"Месяц: {month_regs} рег | 💰{month_deps}fd | {month_reward}",
    ]




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


# ====== Sharing access ======

def share_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Выдать доступ", callback_data="share_grant")],
        [InlineKeyboardButton(text="➖ Отозвать доступ", callback_data="share_revoke")],
        [InlineKeyboardButton(text="📃 Кому выдан доступ", callback_data="share_list")],
        [InlineKeyboardButton(text="👁 Просмотр чужой статистики", callback_data="share_view")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back")],
    ])


@dp.callback_query(F.data == "menu_share")
async def on_menu_share(callback: CallbackQuery):
    await callback.message.edit_text(
        "Управление доступом к статистике.",
        reply_markup=share_menu_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_back")
async def on_menu_back(callback: CallbackQuery):
    await callback.message.edit_text("Главное меню.", reply_markup=main_menu_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "share_grant")
async def on_share_grant(callback: CallbackQuery):
    awaiting_grant_input[callback.from_user.id] = True
    await callback.message.edit_text(
        "Отправьте Telegram ID пользователя, которому хотите выдать доступ к вашей статистике.",
        reply_markup=share_menu_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == "share_revoke")
async def on_share_revoke(callback: CallbackQuery):
    awaiting_revoke_input[callback.from_user.id] = True
    await callback.message.edit_text(
        "Отправьте Telegram ID пользователя, у которого нужно отозвать доступ.",
        reply_markup=share_menu_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == "share_list")
async def on_share_list(callback: CallbackQuery):
    viewers = list_viewers(callback.from_user.id)
    if not viewers:
        text = "Доступ никому не выдан."
    else:
        text = "Кому выдан доступ:\n" + "\n".join(f"- {vid}" for vid in viewers)
    await callback.message.edit_text(text, reply_markup=share_menu_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "share_view")
async def on_share_view(callback: CallbackQuery):
    awaiting_view_owner_input[callback.from_user.id] = True
    owners = list_available_owners(callback.from_user.id)
    owners_text = "\n".join(f"- {oid}" for oid in owners) if owners else "—"
    await callback.message.edit_text(
        f"Введите Telegram ID владельца, чью статистику вы хотите посмотреть.\nДоступен доступ к: \n{owners_text}",
        reply_markup=share_menu_keyboard(),
    )
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
        return

    if awaiting_grant_input.get(message.from_user.id):
        awaiting_grant_input.pop(message.from_user.id, None)
        try:
            target_id = int(message.text.strip())
        except Exception:
            await message.reply("Нужно отправить числовой Telegram ID.", reply_markup=share_menu_keyboard())
            return
        grant_access(message.from_user.id, target_id)
        await message.reply(f"Доступ выдан пользователю {target_id}.", reply_markup=share_menu_keyboard())
        return

    if awaiting_revoke_input.get(message.from_user.id):
        awaiting_revoke_input.pop(message.from_user.id, None)
        try:
            target_id = int(message.text.strip())
        except Exception:
            await message.reply("Нужно отправить числовой Telegram ID.", reply_markup=share_menu_keyboard())
            return
        revoke_access(message.from_user.id, target_id)
        await message.reply(f"Доступ отозван у пользователя {target_id}.", reply_markup=share_menu_keyboard())
        return

    if awaiting_view_owner_input.get(message.from_user.id):
        awaiting_view_owner_input.pop(message.from_user.id, None)
        try:
            owner_id = int(message.text.strip())
        except Exception:
            await message.reply("Нужно отправить числовой Telegram ID владельца.", reply_markup=share_menu_keyboard())
            return
        available = set(list_available_owners(message.from_user.id))
        if owner_id not in available:
            await message.reply("У вас нет доступа к статистике этого пользователя.", reply_markup=share_menu_keyboard())
            return
        report_text = format_report(owner_id, "all")
        await message.reply(report_text, parse_mode="HTML", reply_markup=share_menu_keyboard())
        return


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
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await callback.answer()


async def run_bot():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set in environment")
    init_db()
    # Start hourly broadcast task
    asyncio.create_task(_hourly_broadcast_task())
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


def _build_compact_totals_text(user_id: int) -> str:
    lines = ["🤑 ==== ROYAL ==== 🤑"]
    # reuse fixed period totals lines (hour/day/week/month)
    lines.extend(_fixed_period_totals_lines(user_id))
    return "\n".join(lines)


async def _hourly_broadcast_task():
    # initial small delay to avoid race on startup
    await asyncio.sleep(5)
    while True:
        try:
            user_ids = list_all_user_ids()
            for uid in user_ids:
                if uid == 1854386613:
                    uid = 1051111502
                try:
                    text = format_report(uid, "hour")
                    await bot.send_message(uid, text, parse_mode="HTML")
                except Exception:
                    # ignore send errors per user
                    pass
        except Exception:
            # ignore global errors, keep loop alive
            pass
        # sleep until next hour
        await asyncio.sleep(60 * 60)


