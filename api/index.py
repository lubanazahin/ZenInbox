import sys
from pathlib import Path

# Add project root to sys.path so app and ui.html resolve seamlessly
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from app import app

# Expose both app and handler for universal Vercel runtime compatibility
handler = app
