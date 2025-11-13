import threading
import asyncio

from server import run_flask
from bot import run_bot


def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    asyncio.run(run_bot())


if __name__ == "__main__":
    main()



