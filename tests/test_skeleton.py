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
    tree = _parse_main()
    plugin = _plugin_class(tree)

    assert not plugin.decorator_list
    assert "register" not in MAIN_PATH.read_text(encoding="utf-8")


def test_plugin_exposes_required_lifecycle_and_status_command() -> None:
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
    assert "监控功能将在 PRD 确认后实现" in ast.unparse(status)


def test_configuration_schema_is_intentionally_empty() -> None:
    schema = json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))

    assert schema == {}
