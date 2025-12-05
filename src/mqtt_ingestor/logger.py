"""
celine.common.logger
--------------------
Centralized logging setup for the CELINE package.

Usage:
    from celine.common.logger import get_logger
    logger = get_logger(__name__)
"""

import logging
import os
import sys
from typing import Optional
from dotenv import load_dotenv

# Mapping of human-friendly log levels
_LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def _configure_root_logger() -> None:
    """Configure the root logger once per process."""

    if logging.getLogger().handlers:
        # Already configured, do nothing
        return

    load_dotenv()
    # load local override
    load_dotenv(".env.local", override=True)
    load_dotenv(".env.dev", override=True)

    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = _LOG_LEVELS.get(log_level_str, logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(handler)

    # Optional: reduce verbosity from noisy libs
    for noisy in ["asyncio", "botocore", "urllib3", "pymongo"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a logger configured for the given name.

    Args:
        name: Usually `__name__` from the caller module.

    Example:
        >>> from celine.common.logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Something happened")
    """
    _configure_root_logger()
    return logging.getLogger(name)


# Initialize root configuration immediately for early logs
_configure_root_logger()
