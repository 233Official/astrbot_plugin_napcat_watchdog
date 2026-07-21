"""Command layer tests for NapCatWatchdogPlugin (Issue #4).

These tests verify the command responses, permission gating,
private-chat rejection, subscription persistence, status output,
and the interaction between commands and persistent state.

They import main.py via the same fixture used by
:mod:`test_runtime_import`, so AstrBot stubs are in place.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from core.state_machine import StateMachine
from core.subscriptions import SubscriptionStore

ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Fixture — same approach as test_runtime_import
# ---------------------------------------------------------------------------


@pytest.fixture
def plugin_package(tmp_path: Path) -> Generator[str, None, None]:
    """Set up a temporary plugin package with AstrBot stubs.

    Yields the module path for ``importlib.import_module()``.
    """
    # --- 1. AstrBot stub package ---
    astrbot_pkg = tmp_path / "astrbot"
    astrbot_pkg.mkdir()
    (astrbot_pkg / "__init__.py").write_text("")

    api_pkg = astrbot_pkg / "api"
    api_pkg.mkdir()
    (api_pkg / "__init__.py").write_text("""
from __future__ import annotations

class AstrBotConfig:
    def __init__(self):
        self._store: dict[str, object] = {}
        self._save_called = False
    def get(self, key: str, default: object = None) -> object:
        return self._store.get(key, default)
    def __setitem__(self, key: str, value: object) -> None:
        self._store[key] = value
    def __getitem__(self, key: str) -> object:
        return self.get(key)
    def save_config(self) -> None:
        self._save_called = True

class _Logger:
    def info(self, *args: object, **kwargs: object) -> None: pass
    def error(self, *args: object, **kwargs: object) -> None: pass
    def warning(self, *args: object, **kwargs: object) -> None: pass
    def exception(self, *args: object, **kwargs: object) -> None: pass

logger = _Logger()
""")

    event_pkg = api_pkg / "event"
    event_pkg.mkdir()
    (event_pkg / "__init__.py").write_text("""
from __future__ import annotations

from enum import Enum

class PermissionType(Enum):
    ADMIN = "admin"

class AstrMessageEvent:
    def __init__(self):
        self.unified_msg_origin: str = ""
        self._sender_id: str = ""
        self._private = False

    def plain_result(self, text: str) -> str:
        return text

    def is_private_chat(self) -> bool:
        return self._private

    def get_sender_id(self) -> str:
        return self._sender_id


class _CommandGroup:
    def __init__(self, name: str) -> None:
        self._name = name
        self._func = None

    def __call__(self, func):  # type: ignore[override]
        self._func = func
        func._cmd_group = self._name
        return self

    def command(self, sub_name: str):
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
""")

    star_pkg = api_pkg / "star"
    star_pkg.mkdir()
    (star_pkg / "__init__.py").write_text("""
from __future__ import annotations

class Context:
    pass

class Star:
    def __init__(self, context: Context, config: object) -> None:
        pass

class StarTools:
    @staticmethod
    def get_data_dir(plugin_name: str) -> str:
        import tempfile
        return tempfile.mkdtemp(prefix=f"{plugin_name}_")
""")

    # --- 2. Plugin package with symlinks ---
    plugin_pkg_dir = tmp_path / "astrbot_plugin_napcat_watchdog"
    plugin_pkg_dir.mkdir()
    (plugin_pkg_dir / "__init__.py").write_text("")
    (plugin_pkg_dir / "main.py").symlink_to(ROOT / "main.py")
    core_symlink = plugin_pkg_dir / "core"
    core_symlink.symlink_to(ROOT / "core", target_is_directory=True)

    # --- 3. sys.path ---
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


# ---------------------------------------------------------------------------
# Helpers (mirror main._mask_sensitive to test it)
# ---------------------------------------------------------------------------


def _mask_sensitive(text: str, visible_tail: int = 4) -> str:
    if not text:
        return text
    if len(text) <= visible_tail:
        return "*" * len(text)
    return "*" * (len(text) - visible_tail) + text[-visible_tail:]


# ---------------------------------------------------------------------------
# Private-chat rejection (subscribe / unsubscribe)
# ---------------------------------------------------------------------------


class TestPrivateChatRejection:
    """Private chat for subscribe/unsubscribe returns clear rejection."""

    async def _make_plugin(self, plugin_pkg: str) -> Any:
        mod = importlib.import_module(plugin_pkg)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "test-token"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        # Manually init internal components (skip full initialize)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        return plugin

    async def _make_private_event(self, plugin_pkg: str) -> Any:
        mod = importlib.import_module(plugin_pkg)
        event = mod.AstrMessageEvent()
        event._private = True
        event.unified_msg_origin = "group_test|user_123"
        event._sender_id = "admin_qq"
        return event

    async def _make_group_event(self, plugin_pkg: str) -> Any:
        mod = importlib.import_module(plugin_pkg)
        event = mod.AstrMessageEvent()
        event._private = False
        event.unified_msg_origin = "group_test|user_123"
        event._sender_id = "admin_qq"
        return event

    async def test_subscribe_private_rejected(self, plugin_package: str) -> None:
        plugin = await self._make_plugin(plugin_package)
        event = await self._make_private_event(plugin_package)
        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        assert len(results) == 1
        assert "不支持在私聊中操作" in results[0]
        # Verify no subscription was saved
        assert plugin._sub_store.count == 0

    async def test_unsubscribe_private_rejected(self, plugin_package: str) -> None:
        plugin = await self._make_plugin(plugin_package)
        event = await self._make_private_event(plugin_package)
        results: list[str] = []
        async for r in plugin.unsubscribe(event):
            results.append(r)
        assert len(results) == 1
        assert "不支持在私聊中操作" in results[0]

    async def test_subscribe_works_in_group(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        plugin = await self._make_plugin(plugin_package)
        plugin._subscription_path = tmp_path / "subscriptions.json"
        event = await self._make_group_event(plugin_package)
        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        assert len(results) >= 1
        assert "已订阅" in results[0] or "已更新" in results[0]
        assert plugin._sub_store.count == 1

    async def test_unsubscribe_works_in_group(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        plugin = await self._make_plugin(plugin_package)
        plugin._subscription_path = tmp_path / "subscriptions.json"
        event = await self._make_group_event(plugin_package)
        # Subscribe first
        async for _ in plugin.subscribe(event):
            pass
        assert plugin._sub_store.count == 1
        # Unsubscribe
        results: list[str] = []
        async for r in plugin.unsubscribe(event):
            results.append(r)
        assert len(results) == 1
        assert "已取消" in results[0]
        assert plugin._sub_store.count == 0


# ---------------------------------------------------------------------------
# Subscribe — idempotent & instance summary
# ---------------------------------------------------------------------------


class TestSubscribeBehavior:
    """Test subscribe idempotence and instance summary in response."""

    async def test_repeat_subscribe_idempotent(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """Repeated subscribe for same UMO keeps count=1."""
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        plugin._subscription_path = tmp_path / "subscriptions.json"

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_repeat|user_1"
        event._sender_id = "admin_1"

        # First subscribe
        async for _ in plugin.subscribe(event):
            pass
        assert plugin._sub_store.count == 1
        rec1 = plugin._sub_store.get("group_repeat|user_1")
        assert rec1 is not None
        orig_created = rec1.created_at

        # Second subscribe — different owner
        event._sender_id = "admin_2"
        async for _ in plugin.subscribe(event):
            pass
        assert plugin._sub_store.count == 1
        rec2 = plugin._sub_store.get("group_repeat|user_1")
        assert rec2 is not None
        assert rec2.owner_id == "admin_2"
        assert rec2.created_at == orig_created  # must preserve
        assert rec2.updated_at >= orig_created

    async def test_subscribe_response_includes_summary_with_instances(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """subscribe response includes current instance summary."""
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        plugin._subscription_path = tmp_path / "subscriptions.json"

        # Add a registered QQ
        await plugin._sm.try_reserve(12345)
        plugin._sm.confirm_connection(12345)

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_test|user_1"
        event._sender_id = "admin_1"

        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        combined = "\n".join(results)
        assert "已订阅" in combined
        assert "已登记实例" in combined or "暂无已登记实例" in combined

    async def test_subscribe_response_no_instances(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """Without any registered QQ, response says "暂无已登记实例"."""
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        plugin._subscription_path = tmp_path / "subscriptions.json"

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_test|u"
        event._sender_id = "admin"

        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        combined = "\n".join(results)
        assert "暂无已登记实例" in combined


# ---------------------------------------------------------------------------
# Unsubscribe — idempotent
# ---------------------------------------------------------------------------


class TestUnsubscribeBehavior:
    """Unsubscribe handles missing entry idempotently."""

    async def test_unsubscribe_non_existent(self, plugin_package: str) -> None:
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "never_subscribed|user_x"
        event._sender_id = "admin"

        results: list[str] = []
        async for r in plugin.unsubscribe(event):
            results.append(r)
        assert len(results) == 1
        assert "未订阅" in results[0]


# ---------------------------------------------------------------------------
# Status command — structure & sensitivity
# ---------------------------------------------------------------------------


class TestStatusCommand:
    """Status output structure and sensitivity."""

    async def _make_plugin(self, plugin_pkg: str) -> Any:
        mod = importlib.import_module(plugin_pkg)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "test-token"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        return plugin

    async def test_status_basic_fields(self, plugin_package: str) -> None:
        """Status shows the expected section headers."""
        plugin = await self._make_plugin(plugin_package)
        mod = importlib.import_module(plugin_package)
        event = mod.AstrMessageEvent()

        results: list[str] = []
        async for r in plugin.status(event):
            results.append(r)
        combined = "\n".join(results)
        assert "WebSocket 服务端" in combined
        assert "已登记实例数" in combined
        assert "订阅群数量" in combined
        assert "待补发数量: 0" in combined

    async def test_status_no_sensitive_data(self, plugin_package: str) -> None:
        """Status output must NOT contain sensitive fields."""
        plugin = await self._make_plugin(plugin_package)
        mod = importlib.import_module(plugin_package)
        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_secret|user_999"
        event._sender_id = "admin_qq_123456789"

        # Add subscription and state
        plugin._sub_store.subscribe(event.unified_msg_origin, event._sender_id)
        await plugin._sm.try_reserve(22222)
        plugin._sm.confirm_connection(22222)

        results: list[str] = []
        async for r in plugin.status(event):
            results.append(r)
        combined = "\n".join(results)

        # Must not contain raw UMO, raw owner QQ, token, or paths
        assert "group_secret" not in combined
        assert "user_999" not in combined
        assert "admin_qq_123456789" not in combined
        assert "test-token" not in combined
        assert "subscriptions.json" not in combined
        assert "napcat_watchdog_state" not in combined
        # QQ IDs masked — raw 22222 may appear in masking as "22222" since
        # it's a short number. Let's check it doesn't appear unmasked if
        # longer. For 5-digit 22222, last 4 visible: *2222
        assert "22222" not in combined or "*2222" in combined

    async def test_status_no_side_effects(self, plugin_package: str) -> None:
        """Status must not modify any state or trigger persistence."""
        plugin = await self._make_plugin(plugin_package)
        mod = importlib.import_module(plugin_package)
        event = mod.AstrMessageEvent()

        # Capture state before
        sub_snap_before = plugin._sub_store.get_snapshot()

        # Capture file write count — mock save to verify no call
        original_save = plugin._sub_store.save
        save_call_count = 0

        def tracking_save(*args: Any, **kwargs: Any) -> None:
            nonlocal save_call_count
            save_call_count += 1
            original_save(*args, **kwargs)

        plugin._sub_store.save = tracking_save  # type: ignore[assignment]

        async for _ in plugin.status(event):
            pass

        sub_snap_after = plugin._sub_store.get_snapshot()
        assert sub_snap_before == sub_snap_after
        assert save_call_count == 0


# ---------------------------------------------------------------------------
# Reject blank UMO / empty owner_id (gap 2, 7)
# ---------------------------------------------------------------------------


class TestSubscribeBlankInput:
    """subscribe must reject blank UMO and empty owner_id."""

    async def test_subscribe_blank_umo_rejected(self, plugin_package: str) -> None:
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "  "  # blank after strip
        event._sender_id = "admin_1"

        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        combined = "\n".join(results)
        assert "消息来源为空" in combined or "订阅失败" in combined
        assert plugin._sub_store.count == 0

    async def test_subscribe_blank_owner_rejected(self, plugin_package: str) -> None:
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_test|user"
        event._sender_id = "  "  # blank owner

        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        combined = "\n".join(results)
        assert "发送者ID为空" in combined or "订阅失败" in combined
        assert plugin._sub_store.count == 0


# ---------------------------------------------------------------------------
# Real file persistence round-trip (gap 4, 7)
# ---------------------------------------------------------------------------


class TestRealFilePersistence:
    """Commands with real subscriptions.json file path."""

    async def _make_plugin(self, plugin_pkg: str, sub_path: Path) -> Any:
        mod = importlib.import_module(plugin_pkg)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        plugin._subscription_path = sub_path
        return plugin

    async def test_subscribe_writes_file(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """First subscribe creates the subscriptions.json file."""
        sub_path = tmp_path / "subscriptions.json"
        plugin = await self._make_plugin(plugin_package, sub_path)
        mod = importlib.import_module(plugin_package)

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_test|user_123"
        event._sender_id = "admin_qq"

        async for _ in plugin.subscribe(event):
            pass

        assert sub_path.exists(), "File must be created after subscribe"
        assert plugin._sub_store.count == 1

    async def test_subscribe_file_content(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """File content matches the subscription record."""
        import json

        sub_path = tmp_path / "subscriptions.json"
        plugin = await self._make_plugin(plugin_package, sub_path)
        mod = importlib.import_module(plugin_package)

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_filetest|user_456"
        event._sender_id = "admin_qq_1"

        async for _ in plugin.subscribe(event):
            pass

        raw = json.loads(sub_path.read_text(encoding="utf-8"))
        assert raw["_schema_version"] == 1
        subs = raw["subscriptions"]
        assert "group_filetest|user_456" in subs
        entry = subs["group_filetest|user_456"]
        assert entry["umo"] == "group_filetest|user_456"
        assert entry["owner_id"] == "admin_qq_1"
        assert isinstance(entry["created_at"], float)
        assert isinstance(entry["updated_at"], float)

    async def test_unsubscribe_removes_from_file(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """Unsubscribe removes the entry from the file."""
        import json

        sub_path = tmp_path / "subscriptions.json"
        plugin = await self._make_plugin(plugin_package, sub_path)
        mod = importlib.import_module(plugin_package)

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_unsub|user_789"
        event._sender_id = "admin"

        # Subscribe
        async for _ in plugin.subscribe(event):
            pass

        raw = json.loads(sub_path.read_text(encoding="utf-8"))
        assert "group_unsub|user_789" in raw["subscriptions"]

        # Unsubscribe
        results: list[str] = []
        async for r in plugin.unsubscribe(event):
            results.append(r)
        assert "已取消" in results[0]

        raw2 = json.loads(sub_path.read_text(encoding="utf-8"))
        assert "group_unsub|user_789" not in raw2["subscriptions"]

    async def test_repeat_subscribe_writes_once(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """Repeated subscribe for same UMO does not create duplicate entries."""
        sub_path = tmp_path / "subscriptions.json"
        plugin = await self._make_plugin(plugin_package, sub_path)
        mod = importlib.import_module(plugin_package)

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_repeat|user_1"
        event._sender_id = "admin"

        async for _ in plugin.subscribe(event):
            pass
        # Subscribe again
        async for _ in plugin.subscribe(event):
            pass

        import json

        raw = json.loads(sub_path.read_text(encoding="utf-8"))
        assert len(raw["subscriptions"]) == 1

    async def test_save_failure_rollback_file_unchanged(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """When save fails, the original file is untouched."""
        sub_path = tmp_path / "subscriptions.json"
        plugin = await self._make_plugin(plugin_package, sub_path)
        mod = importlib.import_module(plugin_package)

        event = mod.AstrMessageEvent()
        event.unified_msg_origin = "group_rollback|user_1"
        event._sender_id = "admin"

        # First subscribe creates the file
        async for _ in plugin.subscribe(event):
            pass
        original_content = sub_path.read_text(encoding="utf-8")

        # Break _save_subscriptions to trigger proper rollback.
        # Use the plugin module's own SubscriptionError class so the
        # except clause in main.py catches it (module identity matters
        # across symlinked packages).
        SubErr = mod.SubscriptionError
        original_subs_save = plugin._save_subscriptions

        def broken_subs_save() -> None:
            raise SubErr("Simulated disk full")

        plugin._save_subscriptions = broken_subs_save  # type: ignore[assignment]

        # Try a second subscribe (different owner)
        event._sender_id = "admin_2"
        results: list[str] = []
        async for r in plugin.subscribe(event):
            results.append(r)
        combined = "\n".join(results)
        assert "保存失败" in combined

        # Verify rollback: owner_id reverted to original
        rec = plugin._sub_store.get("group_rollback|user_1")
        assert rec is not None
        assert rec.owner_id == "admin", "Owner must be rolled back"

        # File must be unchanged
        current_content = sub_path.read_text(encoding="utf-8")
        assert current_content == original_content

        # Restore
        plugin._save_subscriptions = original_subs_save


# ---------------------------------------------------------------------------
# Status — WS not running shows connection count 0 (gap 6, 7)
# ---------------------------------------------------------------------------


class TestStatusConnectionCount:
    """Status must always output 当前连接数, even when WS is not running."""

    async def test_status_ws_not_running_shows_zero(self, plugin_package: str) -> None:
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        # WS not started
        plugin._ws_server = None

        event = mod.AstrMessageEvent()
        results: list[str] = []
        async for r in plugin.status(event):
            results.append(r)
        combined = "\n".join(results)
        assert "WebSocket 服务端: 未运行" in combined
        assert "当前连接数: 0" in combined

    async def test_status_read_only_no_write(
        self, plugin_package: str, tmp_path: Path
    ) -> None:
        """Status must not write any files."""
        sub_path = tmp_path / "subscriptions.json"
        mod = importlib.import_module(plugin_package)
        context = mod.Context()
        astrbot_api = importlib.import_module("astrbot.api")
        config = astrbot_api.AstrBotConfig()
        config["access_token"] = "t"
        config["offline_timeout_seconds"] = 90
        config["listen_host"] = "127.0.0.1"
        config["listen_port"] = 0
        config["ws_path"] = "/ws"
        plugin = mod.NapCatWatchdogPlugin(context, config)
        plugin._sub_store = SubscriptionStore()
        plugin._sm = StateMachine()
        plugin._subscription_path = sub_path

        event = mod.AstrMessageEvent()
        async for _ in plugin.status(event):
            pass

        # No file should have been created
        assert not sub_path.exists()


# ---------------------------------------------------------------------------
# Masking helper test
# ---------------------------------------------------------------------------


class TestMaskSensitive:
    def test_long_id(self) -> None:
        assert _mask_sensitive("12345678", 4) == "****5678"

    def test_short_id_fully_masked(self) -> None:
        """Short non-empty strings must be fully masked."""
        assert _mask_sensitive("12", 4) == "**"

    def test_exact_length_fully_masked(self) -> None:
        """Strings equal to visible_tail are also fully masked."""
        assert _mask_sensitive("abcd", 4) == "****"

    def test_umo_masking(self) -> None:
        masked = _mask_sensitive("group_123456|user_789012", 6)
        assert masked.endswith("789012")
        assert masked.startswith("*")
        assert len(masked) == len("group_123456|user_789012")

    def test_empty_string_not_masked(self) -> None:
        assert _mask_sensitive("", 4) == ""

    def test_single_char_masked(self) -> None:
        assert _mask_sensitive("a", 4) == "*"
