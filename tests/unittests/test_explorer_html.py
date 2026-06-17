"""
Unit tests for orbit_explorer.html consistency.

Catches accidental deletions of CSS classes and JS functions
that are still referenced in the HTML.
"""

import re
from pathlib import Path

import pytest

HTML_PATH = Path(__file__).resolve().parents[2] / "src/radical/orbit/data/orbit_explorer.html"


@pytest.fixture(scope="module")
def html():
    return HTML_PATH.read_text()


# ------------------------------------------------------------------
# JS: every function called via onclick="foo(...)" must be defined
# ------------------------------------------------------------------

def _find_onclick_calls(html):
    """Return set of function names invoked via onclick attributes."""
    # Matches onclick="funcName(..." — skip 'if' (used in ternary)
    names = set(re.findall(r'onclick="(\w+)\(', html))
    names.discard('if')
    return names


def _find_js_functions(html):
    """Return set of function names defined via 'function name(' or 'name = function'."""
    explicit  = set(re.findall(r'\bfunction\s+(\w+)\s*\(', html))
    assigned  = set(re.findall(r'\b(\w+)\s*=\s*(?:async\s+)?function\s*\(', html))
    arrow     = set(re.findall(r'\b(\w+)\s*=\s*(?:async\s+)?\(', html))
    return explicit | assigned | arrow


def test_onclick_functions_defined(html):
    """Every onclick handler must have a corresponding function definition."""
    called  = _find_onclick_calls(html)
    defined = _find_js_functions(html)
    missing = called - defined
    assert not missing, f"onclick references undefined functions: {sorted(missing)}"


# ------------------------------------------------------------------
# CSS: classes used in class="..." should have a style definition
# ------------------------------------------------------------------

# Classes used only as DOM selectors (querySelector) or in templates,
# not expected to have CSS definitions.
_SKIP_CLASSES = {
    'layoutClass',
    # form input selectors (queried by JS, styled inline or via parent)
    'bridge-account', 'bridge-duration', 'bridge-endpoint-name',
    'bridge-executor', 'bridge-jobs-output', 'bridge-nodes',
    'bridge-queue', 'bridge-target',
    # JS DOM hooks
    'endpoint-label', 'psij-attribute-rows', 'psij-attributes-container',
    'p-attr-key', 'p-attr-val',
}


def _find_used_classes(html):
    """Return set of CSS class names used in class='...' attributes."""
    classes = set()
    for attr in re.findall(r'class="([^"]*)"', html):
        for token in attr.split():
            # skip JS template expressions like ${foo}
            if '${' in token or token.startswith("'"):
                continue
            if not re.match(r'^[a-zA-Z][\w-]*$', token):
                continue
            classes.add(token)
    return classes - _SKIP_CLASSES


def _find_defined_classes(html):
    """Return set of CSS class names defined in <style> blocks."""
    # Extract all <style>...</style> content
    style_blocks = re.findall(r'<style[^>]*>(.*?)</style>', html, re.DOTALL)
    style_text   = '\n'.join(style_blocks)

    # Match selectors like .foo, .foo:hover, .foo>bar, .foo.bar etc.
    return set(re.findall(r'\.([a-zA-Z][\w-]*)', style_text))


def test_css_classes_defined(html):
    """CSS classes used in the HTML should have style definitions."""
    used    = _find_used_classes(html)
    defined = _find_defined_classes(html)
    missing = used - defined
    assert not missing, f"CSS classes used but not defined: {sorted(missing)}"
