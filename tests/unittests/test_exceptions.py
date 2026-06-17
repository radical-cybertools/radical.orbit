"""
Tests for standardized exception types.
"""
import pytest

from radical.orbit.exceptions import (
    EndpointError,
    SessionError, SessionNotFoundError, SessionClosedError, SessionExpiredError,
    PluginError, PluginNotFoundError, PluginInitializationError,
    ResourceNotFoundError, ConnectionError, BridgeConnectionError,
    EndpointDisconnectedError, ValidationError, TimeoutError,
    exception_to_http_status
)


class TestEndpointError:
    """Tests for base EndpointError."""

    def test_endpoint_error_basic(self):
        """Test basic EndpointError creation."""
        exc = EndpointError("Something failed")
        assert str(exc) == "Something failed"
        assert exc.message == "Something failed"
        assert exc.code == "ENDPOINT_ERROR"

    def test_endpoint_error_custom_code(self):
        """Test EndpointError with custom code."""
        exc = EndpointError("Custom error", code="CUSTOM_CODE")
        assert exc.code == "CUSTOM_CODE"


class TestSessionErrors:
    """Tests for session-related errors."""

    def test_session_not_found_error(self):
        """Test SessionNotFoundError."""
        exc = SessionNotFoundError("session.abc123")
        assert exc.sid == "session.abc123"
        assert exc.code == "SESSION_NOT_FOUND"
        assert "session.abc123" in str(exc)

    def test_session_closed_error(self):
        """Test SessionClosedError."""
        exc = SessionClosedError("session.xyz789")
        assert exc.sid == "session.xyz789"
        assert exc.code == "SESSION_CLOSED"

    def test_session_expired_error(self):
        """Test SessionExpiredError."""
        exc = SessionExpiredError("session.old")
        assert exc.sid == "session.old"
        assert exc.code == "SESSION_EXPIRED"


class TestPluginErrors:
    """Tests for plugin-related errors."""

    def test_plugin_not_found_error(self):
        """Test PluginNotFoundError."""
        exc = PluginNotFoundError("unknown_plugin")
        assert exc.plugin_name == "unknown_plugin"
        assert exc.code == "PLUGIN_NOT_FOUND"
        assert "unknown_plugin" in str(exc)

    def test_plugin_initialization_error(self):
        """Test PluginInitializationError."""
        exc = PluginInitializationError("rhapsody", "Missing dependency")
        assert exc.plugin_name == "rhapsody"
        assert exc.reason == "Missing dependency"
        assert exc.code == "PLUGIN_INIT_FAILED"
        assert "rhapsody" in str(exc)
        assert "Missing dependency" in str(exc)


class TestResourceErrors:
    """Tests for resource-related errors."""

    def test_resource_not_found_error(self):
        """Test ResourceNotFoundError."""
        exc = ResourceNotFoundError("job", "job-123")
        assert exc.resource_type == "job"
        assert exc.resource_id == "job-123"
        assert exc.code == "RESOURCE_NOT_FOUND"
        assert "job" in str(exc)
        assert "job-123" in str(exc)


class TestConnectionErrors:
    """Tests for connection-related errors."""

    def test_bridge_connection_error_basic(self):
        """Test BridgeConnectionError without reason."""
        exc = BridgeConnectionError("http://localhost:8000")
        assert exc.url == "http://localhost:8000"
        assert exc.reason is None
        assert exc.code == "BRIDGE_CONNECTION_FAILED"

    def test_bridge_connection_error_with_reason(self):
        """Test BridgeConnectionError with reason."""
        exc = BridgeConnectionError("http://localhost:8000", "Connection refused")
        assert exc.reason == "Connection refused"
        assert "Connection refused" in str(exc)

    def test_endpoint_disconnected_error(self):
        """Test EndpointDisconnectedError."""
        exc = EndpointDisconnectedError("compute-node-1")
        assert exc.endpoint_name == "compute-node-1"
        assert exc.code == "ENDPOINT_DISCONNECTED"


class TestValidationError:
    """Tests for validation errors."""

    def test_validation_error_basic(self):
        """Test ValidationError without field."""
        exc = ValidationError("Invalid input")
        assert exc.message == "Invalid input"
        assert exc.field is None
        assert exc.code == "VALIDATION_ERROR"

    def test_validation_error_with_field(self):
        """Test ValidationError with field."""
        exc = ValidationError("Must be positive", field="count")
        assert exc.field == "count"


class TestTimeoutError:
    """Tests for timeout errors."""

    def test_timeout_error(self):
        """Test TimeoutError."""
        exc = TimeoutError("job submission", 30.0)
        assert exc.operation == "job submission"
        assert exc.timeout_seconds == 30.0
        assert exc.code == "TIMEOUT"
        assert "30" in str(exc)


class TestExceptionToHttpStatus:
    """Tests for exception to HTTP status mapping."""

    def test_session_not_found_maps_to_404(self):
        """Test SessionNotFoundError maps to 404."""
        exc = SessionNotFoundError("sid")
        assert exception_to_http_status(exc) == 404

    def test_session_closed_maps_to_410(self):
        """Test SessionClosedError maps to 410 (Gone)."""
        exc = SessionClosedError("sid")
        assert exception_to_http_status(exc) == 410

    def test_session_expired_maps_to_410(self):
        """Test SessionExpiredError maps to 410 (Gone)."""
        exc = SessionExpiredError("sid")
        assert exception_to_http_status(exc) == 410

    def test_plugin_not_found_maps_to_404(self):
        """Test PluginNotFoundError maps to 404."""
        exc = PluginNotFoundError("plugin")
        assert exception_to_http_status(exc) == 404

    def test_resource_not_found_maps_to_404(self):
        """Test ResourceNotFoundError maps to 404."""
        exc = ResourceNotFoundError("job", "id")
        assert exception_to_http_status(exc) == 404

    def test_validation_error_maps_to_400(self):
        """Test ValidationError maps to 400."""
        exc = ValidationError("bad input")
        assert exception_to_http_status(exc) == 400

    def test_timeout_maps_to_504(self):
        """Test TimeoutError maps to 504."""
        exc = TimeoutError("op", 10.0)
        assert exception_to_http_status(exc) == 504

    def test_bridge_connection_maps_to_503(self):
        """Test BridgeConnectionError maps to 503."""
        exc = BridgeConnectionError("url")
        assert exception_to_http_status(exc) == 503

    def test_endpoint_disconnected_maps_to_503(self):
        """Test EndpointDisconnectedError maps to 503."""
        exc = EndpointDisconnectedError("endpoint")
        assert exception_to_http_status(exc) == 503

    def test_generic_endpoint_error_maps_to_500(self):
        """Test generic EndpointError maps to 500."""
        exc = EndpointError("error")
        assert exception_to_http_status(exc) == 500

    def test_unknown_exception_maps_to_500(self):
        """Test unknown exception maps to 500."""
        exc = RuntimeError("unknown")
        assert exception_to_http_status(exc) == 500
