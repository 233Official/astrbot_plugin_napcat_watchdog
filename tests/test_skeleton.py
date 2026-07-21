"""AST-level structural tests for plugin skeleton integrity.

These tests parse main.py without importing AstrBot,
verifying that auto-discovery, lifecycle, and command
structure remain intact.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"


def _parse_main() -> ast.Module:
    return ast.parse(MAIN_PATH.read_text(encoding="utf-8"))


def _plugin_class(tree: ast.Module) -> ast.ClassDef:
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id == "Star" for base in node.bases)
    )


def test_plugin_uses_star_auto_discovery_without_register() -> None:
    """Plugin inherits Star and does NOT use @register decorator."""
    tree = _parse_main()
    plugin = _plugin_class(tree)

    assert not plugin.decorator_list
    assert "register" not in MAIN_PATH.read_text(encoding="utf-8")


def test_plugin_exposes_required_lifecycle_and_status_command() -> None:
    """Plugin has __init__, initialize, terminate, and napcat_watchdog_status."""
    plugin = _plugin_class(_parse_main())
    methods = {
        node.name: node
        for node in plugin.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    constructor = methods["__init__"]
    assert [argument.arg for argument in constructor.args.args] == [
        "self",
        "context",
        "config",
    ]
    assert isinstance(methods["initialize"], ast.AsyncFunctionDef)
    assert isinstance(methods["terminate"], ast.AsyncFunctionDef)

    status = methods["status"]
    assert isinstance(status, ast.AsyncFunctionDef)
    assert any(
        isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr == "command"
        and decorator.args
        and isinstance(decorator.args[0], ast.Constant)
        and decorator.args[0].value == "napcat_watchdog_status"
        for decorator in status.decorator_list
    )
    # Status text should mention the WS phase (not the old skeleton message)
    status_unparse = ast.unparse(status)
    assert "WS 服务端" in status_unparse


def test_configuration_schema_has_all_required_fields() -> None:
    """_conf_schema.json contains all six expected fields with correct defaults."""
    schema = json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))

    assert schema["listen_host"]["default"] == "0.0.0.0"
    assert schema["listen_port"]["default"] == 19090
    assert schema["ws_path"]["default"] == "/napcat-watchdog/ws"
    assert schema["public_ws_url"]["default"] == ""
    assert schema["access_token"]["default"] == ""
    assert schema["offline_timeout_seconds"]["default"] == 90

    # Verify exactly six top-level keys (no extras, no missing)
    assert set(schema) == {
        "listen_host",
        "listen_port",
        "ws_path",
        "public_ws_url",
        "access_token",
        "offline_timeout_seconds",
    }
