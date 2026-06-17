"""
UI Schema definitions for ORBIT plugins.

This module defines Pydantic models for validating plugin UI configurations.
Plugins can define a `ui_config` class attribute to describe their portal UI,
enabling dynamic rendering and seamless integration of external plugins.
"""

from typing import List, Optional, Union, Dict, Any
from pydantic import BaseModel, Field


class UIFieldOption(BaseModel):
    """Option for select/radio fields."""
    value: str
    label: Optional[str] = None


class UIField(BaseModel):
    """Form field definition."""
    name: str = Field(..., description="Field name for data collection")
    type: str = Field(
        default="text",
        description="Field type: text, select, number, textarea, checkbox, hidden"
    )
    label: str = Field(..., description="Display label")
    css_class: Optional[str] = Field(
        default=None,
        description="CSS class for element (used by JS to collect values)"
    )
    default: Optional[str] = Field(default=None, description="Default value")
    placeholder: Optional[str] = Field(default=None, description="Placeholder text")
    required: bool = Field(default=True, description="Whether field is required")
    options: Optional[List[Union[str, UIFieldOption]]] = Field(
        default=None,
        description="Options for select fields (strings or {value, label} objects)"
    )
    options_endpoint: Optional[str] = Field(
        default=None,
        description="Endpoint to fetch options dynamically (e.g., 'configs/{sid}')"
    )
    options_value_field: Optional[str] = Field(
        default="name",
        description="Field in response to use as option value"
    )
    options_label_field: Optional[str] = Field(
        default="name",
        description="Field in response to use as option label"
    )
    column: Optional[int] = Field(
        default=None,
        description="Column index for grid layouts (0-based)"
    )


class UIFormSubmit(BaseModel):
    """Submit button configuration."""
    label: str = Field(default="Submit", description="Button text")
    style: str = Field(
        default="success",
        description="Button style: success, primary, secondary, danger"
    )
    endpoint: Optional[str] = Field(
        default=None,
        description="Override endpoint (default: submit/{sid})"
    )


class UIForm(BaseModel):
    """Form definition."""
    id: str = Field(..., description="Unique form identifier")
    title: str = Field(..., description="Card title")
    layout: str = Field(
        default="single",
        description="Layout: single, grid2, grid3"
    )
    fields: List[UIField] = Field(default_factory=list)
    submit: Optional[UIFormSubmit] = Field(default=None)


class UIMonitor(BaseModel):
    """Output/monitor area definition."""
    id: str = Field(..., description="Unique monitor identifier")
    title: str = Field(..., description="Card title")
    type: str = Field(
        default="raw",
        description="Monitor type: task_list, metrics, table, raw"
    )
    css_class: Optional[str] = Field(
        default=None,
        description="CSS class for the output element"
    )
    empty_text: str = Field(
        default="",
        description="Text shown when empty"
    )
    auto_load: Optional[str] = Field(
        default=None,
        description="Endpoint to call on page load (e.g., 'metrics/{sid}')"
    )


class UINotifications(BaseModel):
    """SSE notification configuration."""
    topic: str = Field(..., description="Topic to listen for")
    id_field: str = Field(..., description="Field containing task/job ID")
    state_field: str = Field(default="state", description="Field containing state")


class UIConfig(BaseModel):
    """
    Complete UI configuration for a plugin.

    Example:
        ui_config = UIConfig(
            icon="🚀",
            title="My Plugin",
            forms=[UIForm(id="submit", title="Submit", fields=[...])],
            monitors=[UIMonitor(id="output", title="Output", type="raw")]
        )
    """
    icon: str = Field(default="🔌", description="Emoji or icon for the plugin")
    title: str = Field(..., description="Page title (endpoint name appended automatically)")
    refresh_button: bool = Field(
        default=False,
        description="Show refresh button in header"
    )
    auto_load: Optional[str] = Field(
        default=None,
        description="Endpoint to call on page load"
    )
    forms: List[UIForm] = Field(default_factory=list)
    monitors: List[UIMonitor] = Field(default_factory=list)
    notifications: Optional[UINotifications] = Field(default=None)

    # Additional metadata
    description: Optional[str] = Field(
        default=None,
        description="Short description of the plugin"
    )
    stub_message: Optional[str] = Field(
        default=None,
        description="Message to show for stub/placeholder plugins"
    )


def ui_config_to_dict(config: Union[Dict, UIConfig, None]) -> Dict[str, Any]:
    """
    Convert a UIConfig to a JSON-serializable dict.

    Handles None values and normalizes the structure.
    """
    if config is None:
        return {}

    if isinstance(config, dict):
        return config

    return config.model_dump(exclude_none=True)


def validate_ui_config(config: Union[Dict, UIConfig, None]) -> UIConfig:
    """
    Validate and normalize a UI config.

    Accepts dict (for backward compat) or UIConfig instance.
    Returns a validated UIConfig.
    """
    if config is None:
        return UIConfig(title="Plugin")

    if isinstance(config, UIConfig):
        return config

    if isinstance(config, dict):
        return UIConfig(**config)

    raise ValueError(f"Invalid ui_config type: {type(config)}")
