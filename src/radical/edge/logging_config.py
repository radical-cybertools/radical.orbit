"""
Logging configuration for radical.edge

This module sets up standard Python logging with:
- Uvicorn-style colored output
- Support for correlation IDs in request context
- Structured log format

Import this module early in your application to configure logging.
"""

import logging
import os
import sys
import copy
import contextvars
from typing import Optional


# Context variable for request correlation ID
correlation_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'correlation_id', default=None
)


def set_correlation_id(req_id: str) -> None:
    """Set the correlation ID for the current async context."""
    correlation_id.set(req_id)


def get_correlation_id() -> Optional[str]:
    """Get the correlation ID for the current async context."""
    return correlation_id.get()


def clear_correlation_id() -> None:
    """Clear the correlation ID for the current async context."""
    correlation_id.set(None)


class ColoredFormatter(logging.Formatter):
    """
    Log formatter with Uvicorn-style coloring and correlation ID support.
    """

    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None,
                 style: str = '%', use_colors: Optional[bool] = None):
        super().__init__(fmt, datefmt, style)

        if use_colors is None:
            use_colors = sys.stdout.isatty()
        self.use_colors = use_colors

        self.COLORS = {
            logging.DEBUG: "\033[36m",    # Cyan
            logging.INFO: "\033[32m",     # Green
            logging.WARNING: "\033[33m",  # Yellow
            logging.ERROR: "\033[31m",    # Red
            logging.CRITICAL: "\033[31;1m",  # Bold Red
        }
        self.RESET = "\033[0m"
        self.DIM = "\033[2m"

    def format(self, record: logging.LogRecord) -> str:
        record = copy.copy(record)

        # Add correlation ID if available
        req_id = correlation_id.get()
        if req_id:
            # Truncate for readability
            short_id = req_id[:8] if len(req_id) > 8 else req_id
            if self.use_colors:
                record.msg = f"{self.DIM}[{short_id}]{self.RESET} {record.msg}"
            else:
                record.msg = f"[{short_id}] {record.msg}"

        if not self.use_colors:
            return super().format(record)

        levelname = record.levelname

        if record.levelno in self.COLORS:
            # Match Uvicorn: "INFO:     " (Colored, with colon, padded to 9)
            levelname_with_sep = f"{levelname}:"
            padded_levelname = f"{levelname_with_sep:<9}"
            record.levelname = (f"{self.COLORS[record.levelno]}"
                                f"{padded_levelname}{self.RESET}")

        return super().format(record)


def configure_logging(level: int = logging.INFO,
                      format_string: Optional[str] = None,
                      log_file: Optional[str] = None) -> None:
    """
    Configure logging for radical.edge.

    Args:
        level:         Logging level (default: logging.INFO).
        format_string: Custom format string for the stdout handler
                       (optional; ignored by the file handler which
                       always uses a plain timestamped format).
        log_file:      If given, also write logs to this file
                       (truncated on open).  Parent directory is
                       created if missing.  Stdout output stays
                       colored; the file is plain text with
                       timestamps so it survives ``less`` / ``grep``
                       and Dragon's stdio capture.
    """
    if format_string is None:
        format_string = '%(levelname)s %(message)s'

    handlers: list = []

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(ColoredFormatter(fmt=format_string))
    handlers.append(stdout_handler)

    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(logging.Formatter(
            fmt='%(asctime)s %(levelname)-8s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'))
        handlers.append(file_handler)

    logging.basicConfig(force=True, level=level, handlers=handlers)

    logging.getLogger("radical.edge").setLevel(level)


# Auto-configure on import.  Honor ``RADICAL_EDGE_LOG_LEVEL`` so that
# client scripts (amsc.py, etc.) inherit the level via env without
# needing a code edit; entry-point scripts call ``configure_logging``
# again after argparse, so this has no effect on them.
_env_level = os.environ.get('RADICAL_EDGE_LOG_LEVEL', 'INFO').upper()
configure_logging(getattr(logging, _env_level, logging.INFO))

