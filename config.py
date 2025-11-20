import os
from dotenv import load_dotenv


load_dotenv()


BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PREFIX = os.getenv("PREFIX", "http://localhost:5000")

FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
FLASK_PORT = int(os.getenv("FLASK_PORT", "8000"))

# Default reward per first deposit if user hasn't set it yet
DEFAULT_REWARD_PER_DEP = float(os.getenv("DEFAULT_REWARD_PER_DEP", "1"))

# Allowed user IDs for bot access (comma-separated)
ALLOWED_USER_IDS = [int(uid.strip()) for uid in os.getenv("ALLOWED_USER_IDS", "").split(",") if uid.strip()]

# Campaign ID to Company Name mapping
# Format: "campaign_id1:Company Name 1,campaign_id2:Company Name 2"
CAMPAIGN_NAMES = {}
campaign_names_str = os.getenv("CAMPAIGN_NAMES", "")
if campaign_names_str:
    for pair in campaign_names_str.split(","):
        if ":" in pair:
            campaign_id, company_name = pair.split(":", 1)
            CAMPAIGN_NAMES[campaign_id.strip()] = company_name.strip()




