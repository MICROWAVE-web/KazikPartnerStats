import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, Tuple

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, Message
from aiogram.enums import ParseMode

from config import BOT_TOKEN, PREFIX, ALLOWED_USER_IDS, CAMPAIGN_NAMES
from db import (
    init_db, get_reward, set_reward, aggregate_by_btag, aggregate_by_campaign_and_btag, 
    get_all_user_ids, get_campaign_reward, set_campaign_reward, get_all_campaign_rewards
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Включаем логирование для aiogram
logging.getLogger('aiogram').setLevel(logging.INFO)
logging.getLogger('aiohttp').setLevel(logging.WARNING)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Simple in-memory state to ask for reward input
awaiting_reward_input: Dict[int, bool] = {}
awaiting_campaign_reward_input: Dict[int, str] = {}  # user_id -> campaign_id


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔗 Генерировать ссылки")],
            [KeyboardButton(text="💰 Установить вознаграждение")],
            [KeyboardButton(text="🏢 Установить ставку для компании")],
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
        f"<code>{PREFIX}/{user_id}/registration?btag=${{btag}}&campaign_id=${{campaign_id}}</code>\n\n"
        "Ссылка для первого депозита:\n"
        f"<code>{PREFIX}/{user_id}/firstdep?btag=${{btag}}&campaign_id=${{campaign_id}}</code>"
    )


def format_report(user_id: int, period: str) -> str:
    mapping = {"all": "Все время", "hour": "Час", "day": "День", "week": "Неделя", "last_week": "Прошлая неделя",
               "month": "Месяц"}
    title = mapping.get(period, "Все время")
    stats = aggregate_by_campaign_and_btag(user_id, period)
    if not stats:
        return f"📊 Отчет ({title})\n\nНет данных."
    
    lines = [f"📊 Отчет ({title})", ""]
    total_regs = total_deps = 0
    total_reward = 0.0
    
    # Sort campaigns by name (from config) or by campaign_id
    def get_campaign_name(campaign_id: str) -> str:
        if campaign_id in CAMPAIGN_NAMES:
            return CAMPAIGN_NAMES[campaign_id]
        return campaign_id or "Без компании"
    
    sorted_campaigns = sorted(stats.keys(), key=lambda x: get_campaign_name(x))
    
    for campaign_id in sorted_campaigns:
        campaign_name = get_campaign_name(campaign_id)
        campaign_stats = stats[campaign_id]
        if not campaign_stats:
            continue
        
        # Add company header
        lines.append(f"<b>🏢 {campaign_name}</b>")
        lines.append("")
        
        # Add btag stats within company
        for btag, (regs, deps, reward_sum) in sorted(campaign_stats.items()):
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
        
        lines.append("")  # пустая строка между компаниями
    
    lines += ["", f"Итого: регистрации {total_regs}, депозиты {total_deps}, сумма: {round(total_reward, 2)}"]

    return "\n".join([
        f"📊 Отчет ({title})",
        "",
        "🤑 ==== ROYAL ==== 🤑",
        "",
        *lines,
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


def _summarize_campaign_stats(stats: Dict[str, Dict[str, Tuple[int, int, float]]]) -> Tuple[int, int, float]:
    total_regs = 0
    total_deps = 0
    total_reward = 0.0
    for campaign_stats in stats.values():
        for regs, deps, reward in campaign_stats.values():
            total_regs += regs
            total_deps += deps
            total_reward += reward
    return total_regs, total_deps, total_reward


def format_hourly_report(user_id: int) -> str:
    if user_id == 1854386613:
        user_id = 1051111502
    hour_stats = aggregate_by_campaign_and_btag(user_id, "hour")
    day_stats = aggregate_by_campaign_and_btag(user_id, "day")
    week_stats = aggregate_by_campaign_and_btag(user_id, "week")
    last_week_stats = aggregate_by_campaign_and_btag(user_id, "last_week")

    hour_summary = _summarize_campaign_stats(hour_stats)
    day_summary = _summarize_campaign_stats(day_stats)
    week_summary = _summarize_campaign_stats(week_stats)
    last_week_summary = _summarize_campaign_stats(last_week_stats)

    summary_lines = [
        _format_summary_line("Час", hour_summary),
        _format_summary_line("День", day_summary),
        _format_summary_line("Неделя", week_summary),
        _format_summary_line("Прошлая неделя", last_week_summary),
    ]

    sources_lines = ["Все источники за текущий день:"]
    if day_stats:
        def get_campaign_name(campaign_id: str) -> str:
            if campaign_id in CAMPAIGN_NAMES:
                return CAMPAIGN_NAMES[campaign_id]
            return campaign_id or "Без компании"
        
        sorted_campaigns = sorted(day_stats.keys(), key=lambda x: get_campaign_name(x))
        for campaign_id in sorted_campaigns:
            campaign_name = get_campaign_name(campaign_id)
            campaign_stats = day_stats[campaign_id]
            if campaign_stats:
                sources_lines.append(f"\n<b>🏢 {campaign_name}</b>")
                for btag, (regs, deps, _) in sorted(campaign_stats.items()):
                    label = btag or "-"
                    sources_lines.append(f"  {label} - {regs} рег, {deps} депов")
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
    logger.info(f"Получена команда /start от пользователя {message.from_user.id} (@{message.from_user.username})")
    if not check_access(message.from_user.id):
        logger.warning(f"Попытка доступа от неразрешенного пользователя {message.from_user.id}")
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    try:
        init_db()
        current = get_reward(message.from_user.id)
        campaign_rewards = get_all_campaign_rewards(message.from_user.id)
        
        text_lines = [
            "👋 Добро пожаловать! Это партнерский бот.\n",
            f"Текущее вознаграждение за первый депозит (по умолчанию): {current:.2f}\n"
        ]
        
        if campaign_rewards:
            text_lines.append("\n🏢 Ставки по компаниям:")
            for campaign_id, reward in sorted(campaign_rewards.items()):
                company_name = CAMPAIGN_NAMES.get(campaign_id, campaign_id)
                text_lines.append(f"  • {company_name}: {reward:.2f}")
        
        text_lines.append("\nИспользуйте меню ниже.")
        await message.answer("\n".join(text_lines), reply_markup=main_menu_keyboard())
        logger.info(f"Команда /start успешно обработана для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка при обработке /start: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке команды.")


@dp.message(Command("generate"))
async def cmd_generate(message: Message):
    logger.info(f"Получена команда /generate от пользователя {message.from_user.id}")
    if not check_access(message.from_user.id):
        logger.warning(f"Попытка доступа от неразрешенного пользователя {message.from_user.id}")
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    try:
        text = make_links_text(message.from_user.id)
        await message.answer(text, reply_markup=main_menu_keyboard())
        logger.info(f"Команда /generate успешно обработана для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка при обработке /generate: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при генерации ссылок.")






@dp.message()
async def on_any_message(message: Message):
    logger.info(f"Получено сообщение от пользователя {message.from_user.id}: {message.text}")
    
    if not check_access(message.from_user.id):
        logger.warning(f"Попытка доступа от неразрешенного пользователя {message.from_user.id}")
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    
    try:
        # Обработка ввода вознаграждения для компании
        if message.from_user.id in awaiting_campaign_reward_input:
            campaign_id = awaiting_campaign_reward_input[message.from_user.id]
            if not message.text:
                await message.reply("Пожалуйста, введите число.")
                return
            text = message.text.strip().replace(",", ".")
            try:
                value = float(text)
                logger.info(f"Установка вознаграждения для компании {campaign_id} пользователя {message.from_user.id}: {value}")
                set_campaign_reward(message.from_user.id, campaign_id, value)
                awaiting_campaign_reward_input.pop(message.from_user.id, None)
                campaign_name = CAMPAIGN_NAMES.get(campaign_id, campaign_id or "Без компании")
                await message.reply(
                    f"Готово. Ставка для компании '{campaign_name}' установлена: {value:.2f}",
                    reply_markup=main_menu_keyboard()
                )
                return
            except ValueError:
                logger.warning(f"Неверный формат числа от пользователя {message.from_user.id}: {text}")
                await message.reply("Пожалуйста, введите корректное число.")
                return
        
        # Обработка ввода вознаграждения
        if awaiting_reward_input.get(message.from_user.id):
            if not message.text:
                await message.reply("Пожалуйста, введите число.")
                return
            text = message.text.strip().replace(",", ".")
            try:
                value = float(text)
                logger.info(f"Установка вознаграждения для пользователя {message.from_user.id}: {value}")
                set_reward(message.from_user.id, value)
                awaiting_reward_input.pop(message.from_user.id, None)
                await message.reply(f"Готово. Новое вознаграждение: {value:.2f}", reply_markup=main_menu_keyboard())
                return
            except ValueError:
                logger.warning(f"Неверный формат числа от пользователя {message.from_user.id}: {text}")
                await message.reply("Пожалуйста, введите корректное число.")
                return
        
        # Обработка нажатий кнопок клавиатуры
        if not message.text:
            logger.debug(f"Сообщение без текста от пользователя {message.from_user.id}")
            return
        
        text = message.text.strip()
        logger.info(f"Обработка текста от пользователя {message.from_user.id}: {text}")
        
        # Генерировать ссылки
        if text == "🔗 Генерировать ссылки":
            logger.info(f"Генерация ссылок для пользователя {message.from_user.id}")
            await message.answer(make_links_text(message.from_user.id), reply_markup=main_menu_keyboard())
            return
        
        # Установить вознаграждение
        if text == "💰 Установить вознаграждение":
            logger.info(f"Запрос на установку вознаграждения от пользователя {message.from_user.id}")
            awaiting_reward_input[message.from_user.id] = True
            await message.answer(
                "Введите новое значение вознаграждения (число, например 10 или 12.5)",
                reply_markup=main_menu_keyboard(),
            )
            return
        
        # Установить ставку для компании
        if text == "🏢 Установить ставку для компании":
            logger.info(f"Запрос на установку ставки для компании от пользователя {message.from_user.id}")
            # Получаем все компании из конфига
            if not CAMPAIGN_NAMES:
                await message.answer(
                    "❌ В конфигурации не заданы компании. Добавьте CAMPAIGN_NAMES в .env файл.",
                    reply_markup=main_menu_keyboard(),
                )
                return
            
            # Показываем список компаний с текущими ставками
            campaign_rewards = get_all_campaign_rewards(message.from_user.id)
            default_reward = get_reward(message.from_user.id)
            
            lines = ["Выберите компанию для установки ставки:\n"]
            for campaign_id, company_name in sorted(CAMPAIGN_NAMES.items()):
                current_reward = campaign_rewards.get(campaign_id)
                if current_reward is not None:
                    reward_text = f"{current_reward:.2f} (своя ставка)"
                else:
                    reward_text = f"{default_reward:.2f} (по умолчанию)"
                lines.append(f"<code>{campaign_id}</code> - {company_name}: {reward_text}")
            
            lines.append("\nВведите ID компании (campaign_id) для установки ставки:")
            await message.answer("\n".join(lines), reply_markup=main_menu_keyboard())
            return
        
        # Проверяем, не является ли текст ID компании для установки ставки
        if text in CAMPAIGN_NAMES:
            campaign_id = text
            campaign_name = CAMPAIGN_NAMES[campaign_id]
            current_reward = get_campaign_reward(message.from_user.id, campaign_id)
            default_reward = get_reward(message.from_user.id)
            
            if current_reward is not None:
                reward_text = f"Текущая ставка: {current_reward:.2f}"
            else:
                reward_text = f"Используется ставка по умолчанию: {default_reward:.2f}"
            
            awaiting_campaign_reward_input[message.from_user.id] = campaign_id
            await message.answer(
                f"Компания: <b>{campaign_name}</b> (ID: {campaign_id})\n"
                f"{reward_text}\n\n"
                "Введите новое значение ставки (число, например 10 или 12.5):",
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
            logger.info(f"Запрос отчета '{period}' от пользователя {message.from_user.id}")
            uid = int(message.from_user.id)
            if uid == 1854386613:
                uid = 1051111502
            report_text = format_report(uid, period)
            await message.answer(report_text, reply_markup=main_menu_keyboard())
            return
        
        # Если текст не распознан, просто логируем
        logger.debug(f"Необработанное сообщение от пользователя {message.from_user.id}: {text}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения от пользователя {message.from_user.id}: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке сообщения.")




async def send_hourly_reports():
    logger.info("Отправка часовых отчетов")
    user_ids = get_all_user_ids()
    if not user_ids:
        logger.info("Нет пользователей для отправки отчетов")
        return
    logger.info(f"Отправка отчетов {len(user_ids)} пользователям")
    for user_id in user_ids:
        try:
            report_text = format_hourly_report(user_id)
            await bot.send_message(user_id, report_text)
            logger.info(f"Отчет отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"Ошибка при отправке отчета пользователю {user_id}: {e}", exc_info=True)


async def hourly_report_scheduler():
    logger.info("Запущен планировщик часовых отчетов")
    while True:
        try:
            now = datetime.utcnow()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_seconds = (next_hour - now).total_seconds()
            logger.info(f"Ожидание до следующего часа: {sleep_seconds} секунд")
            await asyncio.sleep(sleep_seconds)
            await send_hourly_reports()
        except Exception as e:
            logger.error(f"Ошибка в планировщике отчетов: {e}", exc_info=True)
            await asyncio.sleep(60)  # Ждем минуту перед повтором


async def run_bot():
    logger.info("=" * 50)
    logger.info("Запуск бота...")
    logger.info("=" * 50)
    
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не установлен в переменных окружения")
        raise RuntimeError("BOT_TOKEN not set in environment")
    
    logger.info(f"BOT_TOKEN установлен: {BOT_TOKEN[:10]}..." if len(BOT_TOKEN) > 10 else "BOT_TOKEN установлен")
    
    try:
        logger.info("Инициализация базы данных...")
        init_db()
        logger.info("✓ База данных инициализирована")
        
        # Запускаем планировщик отчетов в фоне
        logger.info("Запуск планировщика часовых отчетов в фоновом режиме...")
        asyncio.create_task(hourly_report_scheduler())
        logger.info("✓ Планировщик запущен")
        
        logger.info("Начало polling бота...")
        logger.info("Бот готов к работе. Ожидание сообщений...")
        logger.info("=" * 50)
        await dp.start_polling(bot, allowed_updates=["message"])
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}", exc_info=True)
        raise
    finally:
        logger.info("Остановка бота...")
        try:
            await bot.session.close()
            logger.info("✓ Сессия бота закрыта")
        except Exception as e:
            logger.error(f"Ошибка при закрытии сессии бота: {e}")
        logger.info("Бот остановлен")
        logger.info("=" * 50)
