import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent

# Load environment variables from .env file and override any existing terminal variables
load_dotenv(BASE_DIR / ".env", override=True)

# API Keys — set whichever provider(s) you want to use
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip().strip('"\'')

# Default provider: "claude", "gemini", or "easyocr"
DEFAULT_PROVIDER = os.environ.get("DEFAULT_PROVIDER", "gemini")

# Secret key for session signing — MUST be set in .env for production
SECRET_KEY = os.environ.get("SECRET_KEY", "")

DATABASE_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "data" / "realestate.db"))
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", str(BASE_DIR / "uploads"))
MAX_CONCURRENT_EXTRACTIONS = int(os.environ.get("MAX_CONCURRENT", "3"))
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
# Gemini 2.5 Pro is more reliable than Flash for Arabic OCR/reasoning
# (Flash occasionally returns refusals / "absolute" style hedges). Override via env if needed.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro")

# Production mode — set to "production" on VPS
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
