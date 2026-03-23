import os
from pathlib import Path

from easy_logging import setup_logger
from dotenv import load_dotenv

load_dotenv(Path(".env"))

logger = setup_logger(
    name="app",
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    log_to_file=os.getenv("LOG_TO_FILE", "true").lower() == "true",
)
