
# pylint: disable=protected-access
"""
Unit tests for logging_config: ColoredFormatter, configure_logging.
"""

import logging
from radical.orbit import logging_config as lc


# ---------------------------------------------------------------------------
# ColoredFormatter
# ---------------------------------------------------------------------------

def _make_record(msg="hello", level=logging.INFO):
    record = logging.LogRecord(
        name="test", level=level, pathname="", lineno=0,
        msg=msg, args=(), exc_info=None
    )
    return record


def test_colored_formatter_no_colors_plain():
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=False)
    result = fmt.format(_make_record("world", logging.INFO))
    assert "world" in result
    assert "\033[" not in result


def test_colored_formatter_with_colors():
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=True)
    result = fmt.format(_make_record("world", logging.WARNING))
    # Color escape codes should be present
    assert "\033[" in result
    assert "world" in result


def test_colored_formatter_all_levels_with_colors():
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=True)
    for level in (logging.DEBUG, logging.INFO, logging.WARNING,
                  logging.ERROR, logging.CRITICAL):
        record = _make_record("test", level)
        result = fmt.format(record)
        assert "test" in result


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------

def test_configure_logging_custom_format():
    # Should not raise and should apply level
    lc.configure_logging(level=logging.DEBUG,
                         format_string="%(levelname)s | %(message)s")
    logger = logging.getLogger("radical.orbit")
    assert logger.level == logging.DEBUG
    # Restore
    lc.configure_logging(level=logging.INFO)


def test_configure_logging_default_format():
    lc.configure_logging(level=logging.WARNING)
    logger = logging.getLogger("radical.orbit")
    assert logger.level == logging.WARNING
    lc.configure_logging(level=logging.INFO)
