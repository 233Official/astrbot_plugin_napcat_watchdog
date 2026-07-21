"""Runtime import tests — load main.py with minimal AstrBot stubs.

Verifies that the package-relative imports (``from .core import ...``)
resolve correctly and that ``NapCatWatchdogPlugin`` is a proper ``Star``
subclass when the plugin is loaded as a real package.

These tests create a temporary ``astrbot`` stub package and a temporary
plugin package directory, symlink the actual source files, and import
``astrbot_plugin_napcat_watchdog.main`` — the same path AstrBot would
use at runtime.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Generator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

ASTRBOT_API_INIT = """
from __future__ import annotations

class AstrBotConfig:
    def __init__(self):
        self._store: dict[str, object] = {}
    def get(self, key: str, default: object = None) -> object:
        return self._store.get(key, default)
    def __setitem__(self, key: str, value: object) -> None:
        self._store[key] = value
    def __getitem__(self, key: str) -> object:
        return self.get(key)
    def save_config(self) -> None:
        pass

class _Logger:
    def info(self, *args: object, **kwargs: object) -> None: pass
    def error(self, *args: object, **kwargs: object) -> None: pass
    def warning(self, *args: object, **kwargs: object) -> None: pass
    def exception(self, *args: object, **kwargs: object) -> None: pass

logger = _Logger()
"""

ASTRBOT_EVENT_INIT = """
from __future__ import annotations

class AstrMessageEvent:
    def plain_result(self, text: str) -> str:
        return text

class _Filter:
    def command(self, name: str):
        def deco(f):
            f._cmd = name
            return f
        return deco

filter = _Filter()
"""

ASTRBOT_STAR_INIT = """
from __future__ import annotations

class Context:
    pass

class Star:
    def __init__(self, context: Context, config: object) -> None:
        pass
"""


@pytest.fixture
def plugin_package(tmp_path: Path) -> Generator[str, None, None]:
    """Set up a temporary plugin package with astrbot stubs and symlinked sources.

    Yields the dotted module path ``"astrbot_plugin_napcat_watchdog.main"``
    that can be imported after this fixture is set up.
    """
    # --- 1. Create astrbot stub package (mirrors AstrBot's module structure) ---
    astrbot_pkg = tmp_path / "astrbot"
    astrbot_pkg.mkdir()
    (astrbot_pkg / "__init__.py").write_text("")

    api_pkg = astrbot_pkg / "api"
    api_pkg.mkdir()
    (api_pkg / "__init__.py").write_text(ASTRBOT_API_INIT)

    event_pkg = api_pkg / "event"
    event_pkg.mkdir()
    (event_pkg / "__init__.py").write_text(ASTRBOT_EVENT_INIT)

    star_pkg = api_pkg / "star"
    star_pkg.mkdir()
    (star_pkg / "__init__.py").write_text(ASTRBOT_STAR_INIT)

    # --- 2. Create plugin package with __init__.py and symlinks ---
    plugin_pkg = tmp_path / "astrbot_plugin_napcat_watchdog"
    plugin_pkg.mkdir()
    (plugin_pkg / "__init__.py").write_text("")

    # Symlink main.py
    (plugin_pkg / "main.py").symlink_to(ROOT / "main.py")

    # Symlink the entire core/ subpackage
    core_symlink = plugin_pkg / "core"
    core_symlink.symlink_to(ROOT / "core", target_is_directory=True)

    # --- 3. Add to sys.path and clear any stale cache ---
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()

    yield "astrbot_plugin_napcat_watchdog.main"

    # --- 4. Cleanup ---
    sys.path.remove(str(tmp_path))
    for mod in list(sys.modules):
        if mod.startswith("astrbot_plugin_napcat_watchdog") or mod == "astrbot":
            del sys.modules[mod]
        if mod.startswith("astrbot."):
            del sys.modules[mod]
    importlib.invalidate_caches()


class TestRuntimeImport:
    """Load main.py as a package module and verify Star subclass."""

    def test_import_resolves_relative_imports(self, plugin_package: str) -> None:
        """Package-relative imports resolve and NapCatWatchdogPlugin is a Star."""
        main_mod = importlib.import_module(plugin_package)

        assert hasattr(main_mod, "NapCatWatchdogPlugin")
        assert hasattr(main_mod, "Star")
        assert hasattr(main_mod, "WatchdogWSServer")
        assert hasattr(main_mod, "ensure_access_token")

        # Verify class hierarchy
        assert issubclass(main_mod.NapCatWatchdogPlugin, main_mod.Star)

    def test_plugin_can_be_instantiated(self, plugin_package: str) -> None:
        """NapCatWatchdogPlugin can be constructed with stub Context/Config."""
        main_mod = importlib.import_module(plugin_package)

        context = main_mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()

        plugin = main_mod.NapCatWatchdogPlugin(context, config)
        assert isinstance(plugin, main_mod.NapCatWatchdogPlugin)
        assert isinstance(plugin, main_mod.Star)

    def test_plugin_has_lifecycle_methods(self, plugin_package: str) -> None:
        """Plugin exposes async initialize, terminate, and status command."""
        main_mod = importlib.import_module(plugin_package)

        assert hasattr(main_mod.NapCatWatchdogPlugin, "initialize")
        assert hasattr(main_mod.NapCatWatchdogPlugin, "terminate")
        assert hasattr(main_mod.NapCatWatchdogPlugin, "status")
