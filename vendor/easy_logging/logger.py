import logging
import sys
from logging import Handler
from logging.handlers import RotatingFileHandler
from pathlib import Path

_EASY_LOGGING_HANDLER_MARKER = "_easy_logging_managed_handler"


def _resolve_log_level(log_level: str | int) -> int:
    if isinstance(log_level, int):
        return log_level

    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Unknown log level: {log_level!r}")
    return level


def _mark_handler(handler: Handler) -> Handler:
    setattr(handler, _EASY_LOGGING_HANDLER_MARKER, True)
    return handler


def _is_managed_handler(handler: Handler) -> bool:
    return bool(getattr(handler, _EASY_LOGGING_HANDLER_MARKER, False))


def setup_logger(
    name: str = "app",
    log_level: str | int = "INFO",
    log_to_file: bool = False,
    log_file: str = "logs/app.log",
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    replace_handlers: bool = False,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(_resolve_log_level(log_level))
    logger.propagate = False

    if replace_handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    managed_handlers = [handler for handler in logger.handlers if _is_managed_handler(handler)]
    has_console = any(isinstance(handler, logging.StreamHandler) for handler in managed_handlers)
    has_file = any(isinstance(handler, RotatingFileHandler) for handler in managed_handlers)

    if not has_console:
        console_handler = _mark_handler(logging.StreamHandler(sys.stdout))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_to_file and not has_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = _mark_handler(
            RotatingFileHandler(
                filename=log_file,
                maxBytes=max_file_size,
                backupCount=backup_count,
            )
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if not log_to_file:
        removable = [
            handler
            for handler in logger.handlers
            if _is_managed_handler(handler) and isinstance(handler, RotatingFileHandler)
        ]
        for handler in removable:
            logger.removeHandler(handler)
            handler.close()

    return logger
