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

import asyncio
import base64
import importlib
import json
import sys
import time
import types
from collections.abc import Generator
from pathlib import Path

import aiohttp
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

from enum import Enum


class PermissionType(Enum):
    ADMIN = "admin"


class AstrMessageEvent:
    def __init__(self):
        self.unified_msg_origin: str = ""
        self._sender_id: str = ""

    def plain_result(self, text: str) -> str:
        return text

    def is_private_chat(self) -> bool:
        return False

    def get_sender_id(self) -> str:
        return self._sender_id


class _CommandGroup:
    \"\"\"Stub for AstrBot's command group decorator.\"\"\"

    def __init__(self, name: str) -> None:
        self._name = name
        self._func = None

    def __call__(self, func):  # type: ignore[override]
        \"\"\"Called when the object is used as a decorator (group method).\"\"\"
        self._func = func
        func._cmd_group = self._name
        return self

    def command(self, sub_name: str):
        \"\"\"Return a decorator for subcommand methods.\"\"\"
        def deco(f):
            f._sub_cmd = f"{self._name} {sub_name}"
            return f
        return deco


class _Filter:
    PermissionType = PermissionType  # make filter.PermissionType accessible

    def command(self, name: str):
        def deco(f):
            f._cmd = name
            return f
        return deco

    def command_group(self, name: str) -> _CommandGroup:
        return _CommandGroup(name)

    def permission_type(self, perm: PermissionType):
        def deco(f):
            f._perm = perm
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
        self.context = context

class StarTools:
    @staticmethod
    def get_data_dir(plugin_name: str) -> str:
        import tempfile
        return tempfile.mkdtemp(prefix=f"{plugin_name}_")
"""


@pytest.fixture
def plugin_package(tmp_path: Path) -> Generator[str, None, None]:
    """Set up a temporary plugin package with astrbot stubs and symlinked sources.

    Yields the dotted module path ``"astrbot_plugin_napcat_watchdog.main"``
    that can be imported after this fixture is set up.
    """
    # --- 1. Create astrbot stub package ---
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

    (plugin_pkg / "main.py").symlink_to(ROOT / "main.py")

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
        assert hasattr(main_mod, "StateMachine")

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


class TestInitializeFailClosed:
    """initialize() must fail closed on various errors."""

    async def test_initialize_raises_on_get_data_dir_error(
        self,
        plugin_package: str,
    ) -> None:
        """When get_data_dir raises, initialize() must raise and WS stays None."""
        main_mod = importlib.import_module(plugin_package)
        astrbot_api = importlib.import_module("astrbot.api")

        original = main_mod.StarTools.get_data_dir

        def _failing(plugin_name: str | None = None) -> str:
            raise PermissionError("模拟无权限")

        try:
            main_mod.StarTools.get_data_dir = _failing

            context = main_mod.Context()
            config = astrbot_api.AstrBotConfig()
            config["access_token"] = "pre-set-test-token"
            config["offline_timeout_seconds"] = 90
            config["listen_host"] = "127.0.0.1"
            config["listen_port"] = 0
            config["ws_path"] = "/ws"

            plugin = main_mod.NapCatWatchdogPlugin(context, config)
            with pytest.raises(Exception):
                await plugin.initialize()

            # WS server should NOT have been created or started
            assert plugin._ws_server is None
        finally:
            main_mod.StarTools.get_data_dir = original

    async def test_initialize_does_not_write_source_data_dir(
        self,
        plugin_package: str,
    ) -> None:
        """After failed initialize, no data/ directory in plugin source root."""
        main_mod = importlib.import_module(plugin_package)
        astrbot_api = importlib.import_module("astrbot.api")

        source_data = Path(__file__).resolve().parents[1] / "data"
        assert not source_data.exists(), "Precondition: no source data/ dir"

        original = main_mod.StarTools.get_data_dir

        def _failing(plugin_name: str | None = None) -> str:
            raise PermissionError("模拟无权限")

        try:
            main_mod.StarTools.get_data_dir = _failing

            context = main_mod.Context()
            config = astrbot_api.AstrBotConfig()
            config["access_token"] = "pre-set-test-token"
            config["offline_timeout_seconds"] = 90
            config["listen_host"] = "127.0.0.1"
            config["listen_port"] = 0
            config["ws_path"] = "/ws"

            plugin = main_mod.NapCatWatchdogPlugin(context, config)
            with pytest.raises(Exception):
                await plugin.initialize()

            # Source data/ directory must not be created
            assert not source_data.exists()
        finally:
            main_mod.StarTools.get_data_dir = original

    async def test_initialize_raises_on_corrupt_subscriptions(
        self,
        plugin_package: str,
    ) -> None:
        """Corrupt subscription file causes initialize to raise; WS stays None."""
        import json
        import shutil
        import tempfile

        main_mod = importlib.import_module(plugin_package)
        astrbot_api = importlib.import_module("astrbot.api")

        # Point data_dir to a known temporary directory
        fixed_dir = Path(tempfile.mkdtemp(prefix="napcat_sub_test_"))
        original = main_mod.StarTools.get_data_dir

        def _fixed_dir(plugin_name: str | None = None) -> str:
            return str(fixed_dir)

        try:
            main_mod.StarTools.get_data_dir = _fixed_dir
            # Create a valid state snapshot (empty)
            state_path = fixed_dir / "napcat_watchdog_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "_schema_version": 1,
                        "qq_states": {},
                    }
                ),
                encoding="utf-8",
            )
            # Create a corrupt subscription file
            sub_path = fixed_dir / "subscriptions.json"
            sub_path.write_text("{corrupt json", encoding="utf-8")

            context = main_mod.Context()
            config = astrbot_api.AstrBotConfig()
            config["access_token"] = "pre-set-test-token"
            config["offline_timeout_seconds"] = 90
            config["listen_host"] = "127.0.0.1"
            config["listen_port"] = 0
            config["ws_path"] = "/ws"

            plugin = main_mod.NapCatWatchdogPlugin(context, config)
            with pytest.raises(Exception, match="Corrupt subscription"):
                await plugin.initialize()

            # WS server should NOT have been created or started
            assert plugin._ws_server is None
        finally:
            main_mod.StarTools.get_data_dir = original
            shutil.rmtree(fixed_dir, ignore_errors=True)


class TestAdmissionHeartbeatOrdering:
    """Admission persists registration but never emits online notifications."""

    async def test_admission_is_silent_and_first_heartbeat_emits(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        import asyncio

        main_mod = importlib.import_module(plugin_package)
        astrbot_api = importlib.import_module("astrbot.api")

        class _Manager:
            def __init__(self) -> None:
                self.calls: list[tuple[int, object, object]] = []

            async def handle_transition(
                self, self_id: int, status: object, kind: object, **kwargs: object
            ) -> None:
                self.calls.append((self_id, status, kind))

        plugin = main_mod.NapCatWatchdogPlugin(
            main_mod.Context(), astrbot_api.AstrBotConfig()
        )
        plugin._sm = main_mod.StateMachine(offline_timeout=10.0)
        plugin._sm.set_transition_callback(plugin._on_transition)
        plugin._snapshot_path = tmp_path / "state.json"
        manager = _Manager()
        plugin._notification_manager = manager

        generation = await plugin._admit_connection(123)
        await asyncio.sleep(0)
        assert generation == 1
        assert manager.calls == []
        assert plugin._sm._states[123].online_confirmed is False
        assert plugin._snapshot_path.exists()

        await plugin._on_event(
            123,
            generation,
            {
                "post_type": "meta_event",
                "meta_event_type": "heartbeat",
                "status": {"online": True},
            },
        )
        for _ in range(3):
            await asyncio.sleep(0)
        assert len(manager.calls) == 1
        assert manager.calls[0][2] == main_mod.TransitionKind.FIRST_ONLINE


@pytest.mark.asyncio
async def test_plugin_restart_restores_profiles_without_reconnect_lookup(
    plugin_package: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restored display data survives two plugin lifecycles without WS queries."""
    main_mod = importlib.import_module(plugin_package)
    astrbot_api = importlib.import_module("astrbot.api")
    monkeypatch.setattr(main_mod.StarTools, "get_data_dir", lambda name: str(tmp_path))
    server_factory = main_mod.WatchdogWSServer

    def ephemeral_server(**kwargs):
        kwargs["port"] = 0
        return server_factory(**kwargs)

    monkeypatch.setattr(main_mod, "WatchdogWSServer", ephemeral_server)
    avatar = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )

    async def fetch_avatar(self_id: int) -> bytes:
        return avatar

    cache = main_mod.ProfileCache(
        storage_dir=tmp_path / "profiles", avatar_fetcher=fetch_avatar
    )
    await cache.update(12345678, "合成监控账号")
    machine = main_mod.StateMachine()
    generation = machine.confirm_connection(12345678)
    machine.on_heartbeat(12345678, generation, True)
    main_mod.save_snapshot(
        tmp_path / "napcat_watchdog_state.json", machine.get_snapshot()
    )
    machine.cancel_all_timers()

    for _ in range(2):
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "local-profile-test"
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        plugin = main_mod.NapCatWatchdogPlugin(main_mod.Context(), config)
        try:
            await plugin.initialize()
            restored = plugin._get_profile(12345678)
            assert restored.nickname == "合成监控账号"
            assert restored.avatar_data_uri.startswith("data:image/png;base64,")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    f"http://127.0.0.1:{plugin._ws_server.bound_port}/napcat-watchdog/ws",
                    headers={
                        "Authorization": "Bearer local-profile-test",
                        "X-Self-ID": "12345678",
                    },
                ) as ws:
                    await ws.send_json(
                        {
                            "post_type": "meta_event",
                            "meta_event_type": "heartbeat",
                            "status": {"online": True},
                        }
                    )
                    with pytest.raises(asyncio.TimeoutError):
                        await ws.receive(timeout=0.05)
        finally:
            await plugin.terminate()


@pytest.mark.asyncio
async def test_initialize_migrates_backlog_without_rendering_or_spending_attempts(
    plugin_package: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real plugin startup is quiet even with fresh and exhausted v2 entries."""
    main_mod = importlib.import_module(plugin_package)
    astrbot_api = importlib.import_module("astrbot.api")
    monkeypatch.setattr(main_mod.StarTools, "get_data_dir", lambda name: str(tmp_path))
    server_factory = main_mod.WatchdogWSServer

    def ephemeral_server(**kwargs):
        kwargs["port"] = 0
        return server_factory(**kwargs)

    monkeypatch.setattr(main_mod, "WatchdogWSServer", ephemeral_server)
    subscriptions = main_mod.SubscriptionStore()
    subscriptions.subscribe("qq:GroupMessage:local-test", "owner")
    subscriptions.save(tmp_path / "subscriptions.json")
    machine = main_mod.StateMachine()
    entries = {}
    for self_id, attempts in ((12345678, 3), (23456789, 0)):
        generation = machine.confirm_connection(self_id)
        machine.on_heartbeat(self_id, generation, True)
        machine.force_offline(self_id)
        event_id = f"local-offline:{self_id}"
        entries[event_id] = {
            "event_id": event_id,
            "self_id": self_id,
            "event_type": "offline",
            "event_time": time.time(),
            "duration_seconds": None,
            "umo": "qq:GroupMessage:local-test",
            "owner_id": "owner",
            "attempts": attempts,
            "episode_id": 1,
        }
    main_mod.save_snapshot(
        tmp_path / "napcat_watchdog_state.json", machine.get_snapshot()
    )
    machine.cancel_all_timers()
    pending_path = tmp_path / "pending_notifications.json"
    pending_path.write_text(
        json.dumps(
            {"_schema_version": 2, "pending": entries, "seen": [], "completed": []}
        ),
        encoding="utf-8",
    )
    calls = []
    rendered = []

    async def send_message(umo, chain):
        calls.append(umo)
        return True

    async def render(*args, **kwargs):
        rendered.append(args)
        return base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
        )

    context = main_mod.Context()
    context.send_message = send_message
    config = astrbot_api.AstrBotConfig()
    config["access_token"] = "local-quiet-start-test"
    config["listen_host"] = "127.0.0.1"
    plugin = main_mod.NapCatWatchdogPlugin(context, config)
    plugin.html_render = render
    try:
        await plugin.initialize()
        assert calls == []
        assert rendered == []
        stored = json.loads(pending_path.read_text(encoding="utf-8"))
        assert stored["_schema_version"] == 3
        assert {
            item["self_id"]: item["attempts"] for item in stored["pending"].values()
        } == {12345678: 3, 23456789: 0}
    finally:
        await plugin.terminate()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mute_until", "role", "all_muted", "expected"),
    [
        (0, "member", False, True),
        (4_102_444_800, "member", False, False),
        (None, "member", False, False),
        (0, "member", True, False),
        (0, "member", None, False),
        (0, "admin", None, True),
    ],
)
async def test_notification_preflight_checks_current_sender_membership_and_mute(
    plugin_package: str, mute_until, role, all_muted, expected: bool
) -> None:
    main_mod = importlib.import_module(plugin_package)
    astrbot_api = importlib.import_module("astrbot.api")
    calls = []
    requested_platforms = []

    async def action(name, **params):
        calls.append((name, params))
        if name == "get_login_info":
            return {"user_id": 123}
        if name == "get_group_member_info":
            return {
                "user_id": 123,
                "group_id": 456,
                "role": role,
                "shut_up_timestamp": mute_until,
            }
        if name == "get_group_info":
            return {"group_id": 456, "group_all_shut": all_muted}
        raise AssertionError("Unexpected preflight action")

    client = types.SimpleNamespace(api=types.SimpleNamespace(call_action=action))
    platform = types.SimpleNamespace(
        meta=lambda: types.SimpleNamespace(name="aiocqhttp"),
        get_client=lambda: client,
    )

    def get_platform(platform_id):
        requested_platforms.append(platform_id)
        return platform if platform_id == "bot-a" else None

    context = main_mod.Context()
    context.get_platform_inst = get_platform
    plugin = main_mod.NapCatWatchdogPlugin(context, astrbot_api.AstrBotConfig())
    assert await plugin._notification_target_ready("bot-a:GroupMessage:456") is expected
    assert requested_platforms == ["bot-a"]
    assert all(params.get("group_id", 456) == 456 for _, params in calls)
    assert all(params.get("user_id", 123) == 123 for _, params in calls)


@pytest.mark.asyncio
async def test_notification_preflight_never_borrows_another_platform(
    plugin_package: str,
) -> None:
    main_mod = importlib.import_module(plugin_package)
    astrbot_api = importlib.import_module("astrbot.api")
    requested = []
    context = main_mod.Context()
    context.get_platform_inst = lambda platform_id: requested.append(platform_id)
    plugin = main_mod.NapCatWatchdogPlugin(context, astrbot_api.AstrBotConfig())
    assert await plugin._notification_target_ready("missing:GroupMessage:456") is False
    assert requested == ["missing"]
    assert await plugin._notification_target_ready("bot-a:FriendMessage:456") is False
    assert requested == ["missing"]
