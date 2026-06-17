
# pylint: disable=protected-access
"""
Unit tests for logging_config: correlation ID, ColoredFormatter, configure_logging.
"""

import logging
from radical.orbit import logging_config as lc


# ---------------------------------------------------------------------------
# Correlation ID
# ---------------------------------------------------------------------------

def test_set_get_correlation_id():
    lc.set_correlation_id("abc-123")
    assert lc.get_correlation_id() == "abc-123"
    lc.clear_correlation_id()


def test_clear_correlation_id():
    lc.set_correlation_id("xyz")
    lc.clear_correlation_id()
    assert lc.get_correlation_id() is None


def test_correlation_id_default_is_none():
    lc.clear_correlation_id()
    assert lc.get_correlation_id() is None


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
    lc.clear_correlation_id()
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=False)
    result = fmt.format(_make_record("world", logging.INFO))
    assert "world" in result
    assert "\033[" not in result


def test_colored_formatter_with_colors():
    lc.clear_correlation_id()
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=True)
    result = fmt.format(_make_record("world", logging.WARNING))
    # Color escape codes should be present
    assert "\033[" in result
    assert "world" in result


def test_colored_formatter_correlation_id_no_colors():
    lc.set_correlation_id("req-id-12345")
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=False)
    result = fmt.format(_make_record("msg"))
    assert "[req-id-1]" in result   # truncated to 8 chars
    assert "msg" in result
    lc.clear_correlation_id()


def test_colored_formatter_correlation_id_with_colors():
    lc.set_correlation_id("short")
    fmt = lc.ColoredFormatter(fmt="%(levelname)s %(message)s", use_colors=True)
    result = fmt.format(_make_record("msg"))
    # Short ID not truncated (< 8 chars)
    assert "short" in result
    lc.clear_correlation_id()


def test_colored_formatter_all_levels_with_colors():
    lc.clear_correlation_id()
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
