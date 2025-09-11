import os
from dotenv import load_dotenv


load_dotenv()


BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PREFIX = os.getenv("PREFIX", "http://localhost:5000")

FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
FLASK_PORT = int(os.getenv("FLASK_PORT", "5000"))

# Default reward per first deposit if user hasn't set it yet
DEFAULT_REWARD_PER_DEP = float(os.getenv("DEFAULT_REWARD_PER_DEP", "1"))


