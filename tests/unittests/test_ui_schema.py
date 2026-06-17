"""
Unit tests for the UI schema module.
"""

import pytest
from pydantic import ValidationError

from radical.orbit.ui_schema import (
    UIConfig, UIForm, UIField, UIMonitor, UINotifications,
    UIFieldOption, UIFormSubmit, ui_config_to_dict, validate_ui_config
)


class TestUIField:
    """Tests for UIField model."""

    def test_basic_text_field(self):
        field = UIField(name="exec", label="Executable")
        assert field.name == "exec"
        assert field.label == "Executable"
        assert field.type == "text"
        assert field.required is True

    def test_select_field_with_options(self):
        field = UIField(
            name="backend",
            label="Backend",
            type="select",
            options=["local", "slurm", "pbs"]
        )
        assert field.type == "select"
        assert len(field.options) == 3
        assert field.options[0] == "local"

    def test_optional_field(self):
        field = UIField(
            name="queue",
            label="Queue",
            required=False,
            placeholder="optional"
        )
        assert field.required is False
        assert field.placeholder == "optional"

    def test_field_with_default(self):
        field = UIField(
            name="exec",
            label="Executable",
            default="/bin/echo"
        )
        assert field.default == "/bin/echo"

    def test_field_with_css_class(self):
        field = UIField(
            name="exec",
            label="Executable",
            css_class="p-exec"
        )
        assert field.css_class == "p-exec"


class TestUIForm:
    """Tests for UIForm model."""

    def test_basic_form(self):
        form = UIForm(
            id="submit",
            title="Submit Job",
            fields=[
                UIField(name="exec", label="Executable"),
                UIField(name="args", label="Arguments")
            ]
        )
        assert form.id == "submit"
        assert form.title == "Submit Job"
        assert len(form.fields) == 2
        assert form.layout == "single"

    def test_form_with_grid_layout(self):
        form = UIForm(
            id="submit",
            title="Submit",
            layout="grid2",
            fields=[]
        )
        assert form.layout == "grid2"

    def test_form_with_submit_button(self):
        form = UIForm(
            id="submit",
            title="Submit",
            fields=[],
            submit=UIFormSubmit(label="Run", style="success")
        )
        assert form.submit.label == "Run"
        assert form.submit.style == "success"


class TestUIMonitor:
    """Tests for UIMonitor model."""

    def test_basic_monitor(self):
        monitor = UIMonitor(
            id="jobs",
            title="Job Monitor"
        )
        assert monitor.id == "jobs"
        assert monitor.title == "Job Monitor"
        assert monitor.type == "raw"

    def test_task_list_monitor(self):
        monitor = UIMonitor(
            id="tasks",
            title="Task Monitor",
            type="task_list",
            css_class="rh-output",
            empty_text="No tasks submitted yet."
        )
        assert monitor.type == "task_list"
        assert monitor.css_class == "rh-output"
        assert monitor.empty_text == "No tasks submitted yet."

    def test_monitor_with_auto_load(self):
        monitor = UIMonitor(
            id="metrics",
            title="Metrics",
            type="metrics",
            auto_load="metrics/{sid}"
        )
        assert monitor.auto_load == "metrics/{sid}"


class TestUINotifications:
    """Tests for UINotifications model."""

    def test_notifications(self):
        notifications = UINotifications(
            topic="job_status",
            id_field="job_id",
            state_field="state"
        )
        assert notifications.topic == "job_status"
        assert notifications.id_field == "job_id"
        assert notifications.state_field == "state"


class TestUIConfig:
    """Tests for UIConfig model."""

    def test_minimal_config(self):
        config = UIConfig(title="My Plugin")
        assert config.title == "My Plugin"
        assert config.icon == "🔌"
        assert config.refresh_button is False
        assert config.forms == []
        assert config.monitors == []

    def test_full_config(self):
        config = UIConfig(
            icon="🚀",
            title="PsiJ Jobs",
            description="Submit HPC jobs",
            refresh_button=False,
            forms=[
                UIForm(
                    id="submit",
                    title="Submit Job",
                    layout="grid2",
                    fields=[
                        UIField(name="exec", label="Executable", css_class="p-exec"),
                        UIField(name="executor", label="Executor", type="select",
                               options=["local", "slurm"])
                    ],
                    submit=UIFormSubmit(label="Submit", style="success")
                )
            ],
            monitors=[
                UIMonitor(id="jobs", title="Job Monitor", type="task_list",
                         css_class="psij-output", empty_text="No jobs.")
            ],
            notifications=UINotifications(
                topic="job_status",
                id_field="job_id",
                state_field="state"
            )
        )
        assert config.icon == "🚀"
        assert config.title == "PsiJ Jobs"
        assert len(config.forms) == 1
        assert len(config.forms[0].fields) == 2
        assert config.notifications.topic == "job_status"

    def test_stub_config(self):
        config = UIConfig(
            icon="🧠",
            title="Lucid",
            stub_message="Not yet available."
        )
        assert config.stub_message == "Not yet available."


class TestUIConfigToDict:
    """Tests for ui_config_to_dict function."""

    def test_none_returns_empty_dict(self):
        result = ui_config_to_dict(None)
        assert result == {}

    def test_dict_returns_same(self):
        d = {"icon": "🔌", "title": "Test"}
        result = ui_config_to_dict(d)
        assert result == d

    def test_ui_config_converts(self):
        config = UIConfig(title="Test", icon="🔌")
        result = ui_config_to_dict(config)
        assert isinstance(result, dict)
        assert result["title"] == "Test"
        assert result["icon"] == "🔌"


class TestValidateUIConfig:
    """Tests for validate_ui_config function."""

    def test_none_returns_default(self):
        result = validate_ui_config(None)
        assert isinstance(result, UIConfig)
        assert result.title == "Plugin"

    def test_dict_converts_to_config(self):
        d = {"title": "My Plugin", "icon": "🔧"}
        result = validate_ui_config(d)
        assert isinstance(result, UIConfig)
        assert result.title == "My Plugin"
        assert result.icon == "🔧"

    def test_config_returns_same(self):
        config = UIConfig(title="Test")
        result = validate_ui_config(config)
        assert result is config

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError):
            validate_ui_config("invalid")


class TestPluginUIConfigs:
    """Test that existing plugins have valid ui_configs."""

    def test_sysinfo_ui_config(self):
        from radical.orbit.plugin_sysinfo import PluginSysInfo
        ui = PluginSysInfo.ui_config
        assert ui["icon"] == "🖥️"
        assert ui["title"] == "System Info"
        assert ui["refresh_button"] is True
        assert len(ui["monitors"]) == 1

    def test_queue_info_ui_config(self):
        from radical.orbit.plugin_queue_info import PluginQueueInfo
        ui = PluginQueueInfo.ui_config
        assert ui["icon"] == "📋"
        assert ui["title"] == "Queue Info"

    def test_psij_ui_config(self):
        from radical.orbit.plugin_psij import PluginPSIJ
        ui = PluginPSIJ.ui_config
        assert ui["icon"] == "🚀"
        assert ui["title"] == "PsiJ Jobs"
        assert len(ui["forms"]) == 1
        assert ui["forms"][0]["id"] == "submit"
        assert len(ui["forms"][0]["fields"]) == 8
        assert ui["notifications"]["topic"] == "job_status"

    def test_rhapsody_ui_config(self):
        try:
            from radical.orbit.plugin_rhapsody import PluginRhapsody
        except ImportError:
            pytest.skip("rhapsody not installed")

        ui = PluginRhapsody.ui_config
        assert ui["icon"] == "🎼"
        assert ui["title"] == "Rhapsody Tasks"
        assert len(ui["forms"]) == 1
        assert ui["notifications"]["topic"] == "task_status"

    def test_lucid_ui_config(self):
        pytest.importorskip('radical.pilot')
        from radical.orbit.plugin_lucid import PluginLucid
        ui = PluginLucid.ui_config
        assert ui["icon"] == "🧠"
        assert ui["stub_message"] is not None

    def test_xgfabric_ui_config(self):
        from radical.orbit.plugin_xgfabric import PluginXGFabric
        ui = PluginXGFabric.ui_config
        assert ui["icon"] == "🌊"
        assert ui["title"] == "XGFabric Workflow"
        assert ui["custom_template"] is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
