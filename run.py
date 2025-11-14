import threading
import asyncio
import logging

from server import run_flask
from bot import run_bot

# Настройка логирования для основного модуля
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("Запуск приложения KazikPartnerStats")
    logger.info("=" * 60)
    
    try:
        logger.info("Запуск Flask сервера в отдельном потоке...")
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        logger.info("✓ Flask сервер запущен")
        
        logger.info("Запуск Telegram бота...")
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки приложения")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске приложения: {e}", exc_info=True)
        raise
    finally:
        logger.info("Приложение остановлено")


if __name__ == "__main__":
    main()



