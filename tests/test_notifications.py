"""Deterministic notification pipeline tests without a live AstrBot runtime."""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from core.duration import format_natural_duration
from core.notifications import (
    _MAX_DEDUPE_IDS,
    NotificationManager,
    PendingNotification,
    PendingNotificationStore,
    _format_time,
)
from core.state_machine import QQStatus, TransitionKind
from core.subscriptions import SubscriptionStore

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)
_JPEG = b"\xff\xd8\xff" + b"test-jpeg-data"


class FakePlain:
    def __init__(self, text: str) -> None:
        self.text = text


class FakeImage:
    def __init__(self, file: Any = None) -> None:
        self.file = file


class FakeAt:
    def __init__(self, qq: str) -> None:
        self.qq = qq


class FakeMessageChain:
    def __init__(self) -> None:
        self.chain: list[Any] = []

    def use_t2i(self, value: bool) -> FakeMessageChain:
        self.use_t2i_value = value
        return self


@pytest.fixture
def astrbot_components(monkeypatch: pytest.MonkeyPatch) -> None:
    api = types.ModuleType("astrbot.api")
    event = types.ModuleType("astrbot.api.event")
    components = types.ModuleType("astrbot.api.message_components")
    event.MessageChain = FakeMessageChain  # type: ignore[attr-defined]
    components.Plain = FakePlain  # type: ignore[attr-defined]
    components.Image = FakeImage  # type: ignore[attr-defined]
    components.At = FakeAt  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "astrbot.api", api)
    monkeypatch.setitem(sys.modules, "astrbot.api.event", event)
    monkeypatch.setitem(sys.modules, "astrbot.api.message_components", components)


class FakeContext:
    def __init__(self, failures: set[str] | None = None) -> None:
        self.failures = failures or set()
        self.calls: list[tuple[str, FakeMessageChain]] = []

    async def send_message(self, umo: str, chain: FakeMessageChain) -> bool:
        self.calls.append((umo, chain))
        return umo not in self.failures


def make_manager(
    tmp_path: Path,
    context: FakeContext,
    *,
    html_render: Any = None,
    profile_provider: Any = None,
    profile_waiter: Any = None,
    profile_wait_timeout: float = 3.0,
) -> tuple[NotificationManager, SubscriptionStore, Path]:
    subscriptions = SubscriptionStore(wall_clock=lambda: 1000.0)
    pending_path = tmp_path / "pending_notifications.json"
    manager = NotificationManager(
        context,
        subscriptions,
        pending_path,
        html_render=html_render,
        profile_provider=profile_provider,
        profile_waiter=profile_waiter,
        profile_wait_timeout=profile_wait_timeout,
    )
    return manager, subscriptions, pending_path


@pytest.mark.asyncio
async def test_offline_sends_image_and_current_owner_to_each_subscriber(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    render_data: list[dict[str, Any]] = []

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        render_data.append(data)
        return _PNG

    manager, subscriptions, _ = make_manager(tmp_path, context, html_render=render)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    subscriptions.subscribe("qq:GroupMessage:2", "owner-b")

    await manager.handle_transition(
        123456,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=1000.0,
    )

    assert [call[0] for call in context.calls] == [
        "qq:GroupMessage:1",
        "qq:GroupMessage:2",
    ]
    assert all(type(call[1].chain[0]) is FakeImage for call in context.calls)
    assert [call[1].chain[1].qq for call in context.calls] == ["owner-a", "owner-b"]
    assert all("owner-" not in data for data in render_data)


@pytest.mark.asyncio
async def test_first_online_and_recovered_use_online_variant_without_owner(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    variants: list[str] = []

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        variants.append(data["event_type"])
        return _PNG

    manager, subscriptions, _ = make_manager(tmp_path, context, html_render=render)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        1, QQStatus.ONLINE, TransitionKind.FIRST_ONLINE, event_time=1000.0
    )
    await manager.handle_transition(
        1,
        QQStatus.ONLINE,
        TransitionKind.RECOVERED,
        event_time=1010.0,
        offline_since=1000.0,
    )

    assert variants == ["recovered", "recovered"]
    assert all(len(call[1].chain) == 1 for call in context.calls)


@pytest.mark.asyncio
async def test_no_subscribers_creates_no_delivery_or_pending(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    manager, _, pending_path = make_manager(tmp_path, context)

    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
    )

    assert context.calls == []
    assert manager.pending_count == 0
    # The durable seen marker prevents a subscription added later (or after
    # restart) from receiving this historical event.
    assert pending_path.exists()


@pytest.mark.asyncio
async def test_invalid_render_result_uses_text_fallback(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()

    async def invalid_render(*args: Any, **kwargs: Any) -> bytes:
        return b"not an image"

    manager, subscriptions, _ = make_manager(
        tmp_path, context, html_render=invalid_render
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
    )

    assert len(context.calls) == 1
    assert isinstance(context.calls[0][1].chain[0], FakePlain)
    assert context.calls[0][1].chain[1].qq == "owner-a"


@pytest.mark.asyncio
async def test_failure_is_isolated_retry_capped_and_pending_persisted(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext(failures={"qq:GroupMessage:bad"})
    manager, subscriptions, pending_path = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:bad", "owner-a")
    subscriptions.subscribe("qq:GroupMessage:good", "owner-b")

    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
    )

    assert [umo for umo, _ in context.calls].count("qq:GroupMessage:bad") == 3
    assert [umo for umo, _ in context.calls].count("qq:GroupMessage:good") == 1
    assert manager.pending_count == 1
    assert pending_path.exists()


@pytest.mark.asyncio
async def test_restart_catchup_and_unsubscribe_drop_pending(
    tmp_path: Path, astrbot_components: None
) -> None:
    failing = FakeContext(failures={"qq:GroupMessage:1"})
    manager, subscriptions, pending_path = make_manager(tmp_path, failing)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
    )
    assert manager.pending_count == 1

    recovered = FakeContext()
    manager2 = NotificationManager(recovered, subscriptions, pending_path)
    manager2.load()
    await manager2.catch_up()
    assert [umo for umo, _ in recovered.calls] == ["qq:GroupMessage:1"]
    assert manager2.pending_count == 0

    failing2 = FakeContext(failures={"qq:GroupMessage:1"})
    manager3, _, _ = make_manager(tmp_path / "second", failing2)
    manager3.subscription_store.subscribe("qq:GroupMessage:1", "owner-a")
    await manager3.handle_transition(
        2, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1001.0
    )
    manager3.subscription_store.unsubscribe("qq:GroupMessage:1")
    manager3.drop_for_umo("qq:GroupMessage:1")
    assert manager3.pending_count == 0


@pytest.mark.asyncio
async def test_duplicate_and_non_notification_kinds_do_not_send(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    manager, subscriptions, _ = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        1, QQStatus.ONLINE, TransitionKind.FIRST_ONLINE, event_time=1000.0
    )
    await manager.handle_transition(
        1, QQStatus.ONLINE, TransitionKind.FIRST_ONLINE, event_time=1000.0
    )
    await manager.handle_transition(
        1, QQStatus.ONLINE, TransitionKind.SHUTDOWN, event_time=1001.0
    )

    assert len(context.calls) == 1


def test_dynamic_template_data_masks_qq_and_excludes_connection_values() -> None:
    from core.notifications import _template_data

    data = _template_data(
        self_id=123456789,
        event_type="offline",
        event_time=1000.0,
        duration_seconds=None,
    )
    joined = " ".join(str(value) for value in data.values())
    assert data["masked_qq"] == "12****89"
    assert "123456789" not in joined
    assert "token" not in joined.lower()
    assert "ws://" not in joined.lower()


@pytest.mark.asyncio
async def test_renderer_receives_geometry_and_profile_fields(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    captured: dict[str, Any] = {}
    avatar = "data:image/png;base64," + base64.b64encode(_PNG).decode("ascii")

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        captured["data"] = data
        captured["options"] = kwargs["options"]
        return _JPEG

    manager, subscriptions, _ = make_manager(tmp_path, context, html_render=render)
    manager.profile_provider = lambda self_id: {
        "nickname": "测试账号",
        "avatar_data_uri": avatar,
    }
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        123456, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
    )

    options = captured["options"]
    assert options == {
        "full_page": True,
        "type": "jpeg",
        "quality": 90,
        "viewport_width": 812,
        "viewport_height": 320,
        "device_scale_factor_level": "high",
        "wait_until": "domcontentloaded",
    }
    assert captured["data"]["nickname"] == "测试账号"
    assert captured["data"]["avatar_data_uri"] == avatar
    assert "1280" not in str(options)
    assert "720" not in str(options)


@pytest.mark.asyncio
async def test_remote_avatar_url_is_rejected_and_missing_profile_is_safe(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    captured: dict[str, Any] = {}

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        captured.update(data)
        return _PNG

    manager, subscriptions, _ = make_manager(tmp_path, context, html_render=render)
    manager.profile_provider = lambda self_id: {
        "nickname": "昵称\n不应换行",
        "avatar_data_uri": "https://qlogo.cn/remote.png",
    }
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        1, QQStatus.ONLINE, TransitionKind.FIRST_ONLINE, event_time=1000.0
    )

    assert captured["nickname"] == "昵称不应换行"
    assert captured["avatar_data_uri"] is None
    assert "qlogo.cn" not in str(captured)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", [TransitionKind.FIRST_ONLINE, TransitionKind.RECOVERED]
)
async def test_cold_recovery_waits_for_profile_and_rereads_cache_before_one_render(
    tmp_path: Path, astrbot_components: None, kind: TransitionKind
) -> None:
    context = FakeContext()
    profile: dict[str, Any] = {
        "nickname": " \n ",
        "avatar_data_uri": "https://example.invalid/avatar.png",
    }
    waiter_started = asyncio.Event()
    release_refresh = asyncio.Event()
    captured: list[dict[str, Any]] = []
    waiter_calls: list[int] = []
    avatar = "data:image/png;base64," + base64.b64encode(_PNG).decode("ascii")

    async def refresh() -> dict[str, str]:
        await release_refresh.wait()
        profile.update(nickname="恢复后的昵称", avatar_data_uri=avatar)
        return {"nickname": "不能直接作为渲染数据的返回值"}

    refresh_task = asyncio.create_task(refresh())

    def wait_for_profile(self_id: int) -> asyncio.Task[dict[str, str]]:
        waiter_calls.append(self_id)
        waiter_started.set()
        return refresh_task

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        captured.append(data)
        waiter_started.set()
        return _PNG

    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=render,
        profile_provider=lambda self_id: profile,
        profile_waiter=wait_for_profile,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    subscriptions.subscribe("qq:GroupMessage:2", "owner-b")
    offline_since = 880.0 if kind == TransitionKind.RECOVERED else None
    notification = asyncio.create_task(
        manager.handle_transition(
            123456,
            QQStatus.ONLINE,
            kind,
            event_time=1000.0,
            offline_since=offline_since,
            episode_id=7,
        )
    )
    try:
        await waiter_started.wait()
        assert waiter_calls == [123456]
        assert captured == []
        assert context.calls == []
        await manager.handle_transition(
            123456, QQStatus.ONLINE, kind, event_time=2000.0, episode_id=7
        )
        assert waiter_calls == [123456]
        manager.wall_clock = lambda: 9000.0
        release_refresh.set()
        await notification
    finally:
        release_refresh.set()
        await asyncio.gather(notification, refresh_task, return_exceptions=True)

    assert len(captured) == 1
    assert captured[0]["nickname"] == "恢复后的昵称"
    assert captured[0]["avatar_data_uri"] == avatar
    assert captured[0]["event_time"] == _format_time(1000.0)
    if offline_since is None:
        assert "duration_text" not in captured[0]
    else:
        assert captured[0]["duration_text"] == format_natural_duration(120.0)
    assert [umo for umo, _ in context.calls] == [
        "qq:GroupMessage:1",
        "qq:GroupMessage:2",
    ]
    assert all(len(chain.chain) == 1 for _, chain in context.calls)
    assert all(isinstance(chain.chain[0], FakeImage) for _, chain in context.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("partial_nickname", [None, "已取得的昵称"])
async def test_cold_recovery_timeout_rereads_partial_cache_and_keeps_refresh_alive(
    tmp_path: Path, astrbot_components: None, partial_nickname: str | None
) -> None:
    context = FakeContext()
    profile: dict[str, Any] = {}
    start_refresh = asyncio.Event()
    release_refresh = asyncio.Event()
    captured: list[dict[str, Any]] = []
    waiter_calls: list[int] = []
    avatar = "data:image/png;base64," + base64.b64encode(_PNG).decode("ascii")

    async def refresh() -> None:
        await start_refresh.wait()
        if partial_nickname:
            profile["nickname"] = partial_nickname
        await release_refresh.wait()
        profile.update(nickname="完整资料", avatar_data_uri=avatar)

    refresh_task = asyncio.create_task(refresh())

    def wait_for_profile(self_id: int) -> asyncio.Task[None]:
        waiter_calls.append(self_id)
        start_refresh.set()
        return refresh_task

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        captured.append(data)
        return _PNG

    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=render,
        profile_provider=lambda self_id: profile,
        profile_waiter=wait_for_profile,
        profile_wait_timeout=0.01,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    try:
        await manager.handle_transition(
            123456,
            QQStatus.ONLINE,
            TransitionKind.RECOVERED,
            event_time=1000.0,
            episode_id=7,
        )
        assert waiter_calls == [123456]
        assert not refresh_task.done()
        assert len(context.calls) == 1
        assert len(context.calls[0][1].chain) == 1
        assert isinstance(context.calls[0][1].chain[0], FakeImage)
        assert captured[0]["nickname"] == partial_nickname
        assert captured[0]["avatar_data_uri"] is None
    finally:
        start_refresh.set()
        release_refresh.set()
        await refresh_task

    await manager.handle_transition(
        123456,
        QQStatus.ONLINE,
        TransitionKind.RECOVERED,
        event_time=2000.0,
        episode_id=7,
    )
    assert len(context.calls) == 1
    await manager.handle_transition(
        123456,
        QQStatus.ONLINE,
        TransitionKind.RECOVERED,
        event_time=3000.0,
        episode_id=8,
    )
    assert len(context.calls) == 2
    assert waiter_calls == [123456]
    assert captured[1]["nickname"] == "完整资料"
    assert captured[1]["avatar_data_uri"] == avatar


@pytest.mark.asyncio
async def test_cold_recovery_cancellation_does_not_cancel_background_refresh(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    profile: dict[str, Any] = {}
    waiter_started = asyncio.Event()
    release_refresh = asyncio.Event()
    waiter_calls: list[int] = []

    async def refresh() -> None:
        await release_refresh.wait()
        profile["nickname"] = "后台取得的昵称"

    refresh_task = asyncio.create_task(refresh())

    def wait_for_profile(self_id: int) -> asyncio.Task[None]:
        waiter_calls.append(self_id)
        waiter_started.set()
        return refresh_task

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        waiter_started.set()
        return _PNG

    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=render,
        profile_provider=lambda self_id: profile,
        profile_waiter=wait_for_profile,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    notification = asyncio.create_task(
        manager.handle_transition(
            123456,
            QQStatus.ONLINE,
            TransitionKind.FIRST_ONLINE,
            event_time=1000.0,
            episode_id=7,
        )
    )
    try:
        await waiter_started.wait()
        assert waiter_calls == [123456]
        notification.cancel()
        with pytest.raises(asyncio.CancelledError):
            await notification
        assert not refresh_task.done()
        assert context.calls == []
    finally:
        release_refresh.set()
        await asyncio.gather(notification, refresh_task, return_exceptions=True)

    assert profile["nickname"] == "后台取得的昵称"
    await manager.handle_transition(
        123456,
        QQStatus.ONLINE,
        TransitionKind.RECOVERED,
        event_time=2000.0,
        episode_id=8,
    )
    assert waiter_calls == [123456]
    assert len(context.calls) == 1
    assert len(context.calls[0][1].chain) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("partial_nickname", [None, "部分成功的昵称"])
async def test_cold_recovery_wait_failure_uses_latest_cache_for_text_fallback(
    tmp_path: Path, astrbot_components: None, partial_nickname: str | None
) -> None:
    context = FakeContext()
    profile: dict[str, Any] = {}
    captured: dict[str, Any] = {}

    async def wait_for_profile(self_id: int) -> None:
        if partial_nickname:
            profile["nickname"] = partial_nickname
        raise RuntimeError("profile refresh failed")

    async def invalid_render(
        template: str, data: dict[str, Any], **kwargs: Any
    ) -> bytes:
        captured.update(data)
        return b"not an image"

    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=invalid_render,
        profile_provider=lambda self_id: profile,
        profile_waiter=wait_for_profile,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    await manager.handle_transition(
        123456,
        QQStatus.ONLINE,
        TransitionKind.RECOVERED,
        event_time=1000.0,
    )

    assert captured["nickname"] == partial_nickname
    assert captured["avatar_data_uri"] is None
    assert len(context.calls) == 1
    assert len(context.calls[0][1].chain) == 1
    assert isinstance(context.calls[0][1].chain[0], FakePlain)
    if partial_nickname:
        assert partial_nickname in context.calls[0][1].chain[0].text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", [TransitionKind.FIRST_ONLINE, TransitionKind.RECOVERED]
)
@pytest.mark.parametrize("cached_fields", ["nickname", "avatar", "both"])
async def test_recovery_uses_stale_cached_profile_without_waiting(
    tmp_path: Path,
    astrbot_components: None,
    kind: TransitionKind,
    cached_fields: str,
) -> None:
    context = FakeContext()
    avatar = "data:image/png;base64," + base64.b64encode(_PNG).decode("ascii")
    # Successful cached data remains usable regardless of its refresh age.
    profile = {
        "nickname": "历史昵称" if cached_fields in {"nickname", "both"} else None,
        "avatar_data_uri": avatar if cached_fields in {"avatar", "both"} else None,
    }
    waiter_calls: list[int] = []
    captured: dict[str, Any] = {}

    async def wait_for_profile(self_id: int) -> None:
        waiter_calls.append(self_id)
        raise RuntimeError("cached profile must not wait")

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        captured.update(data)
        return _PNG

    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=render,
        profile_provider=lambda self_id: profile,
        profile_waiter=wait_for_profile,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    await manager.handle_transition(123456, QQStatus.ONLINE, kind, event_time=1000.0)

    assert waiter_calls == []
    assert captured["nickname"] == profile["nickname"]
    assert captured["avatar_data_uri"] == profile["avatar_data_uri"]
    assert len(context.calls) == 1
    assert len(context.calls[0][1].chain) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("has_cached_profile", [False, True])
async def test_offline_never_waits_for_profile_and_keeps_owner_mention(
    tmp_path: Path, astrbot_components: None, has_cached_profile: bool
) -> None:
    context = FakeContext()
    profile = {"nickname": "历史昵称"} if has_cached_profile else None
    waiter_calls: list[int] = []
    captured: dict[str, Any] = {}

    async def wait_for_profile(self_id: int) -> None:
        waiter_calls.append(self_id)
        raise RuntimeError("offline notifications must not wait")

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        captured.update(data)
        return _PNG

    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=render,
        profile_provider=lambda self_id: profile,
        profile_waiter=wait_for_profile,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    await manager.handle_transition(
        123456,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=1000.0,
        online_since=880.0,
    )

    assert waiter_calls == []
    assert captured["nickname"] == ("历史昵称" if has_cached_profile else None)
    assert captured["duration_text"] == format_natural_duration(120.0)
    assert len(context.calls) == 1
    assert isinstance(context.calls[0][1].chain[0], FakeImage)
    assert context.calls[0][1].chain[1].qq == "owner-a"


@pytest.mark.asyncio
async def test_episode_id_is_the_notification_deduplication_identity(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    manager, subscriptions, _ = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        1,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=1000.0,
        episode_id=7,
    )
    await manager.handle_transition(
        1,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=2000.0,
        episode_id=7,
    )
    await manager.handle_transition(
        1,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=2000.0,
        episode_id=8,
    )

    assert len(context.calls) == 2


@pytest.mark.asyncio
async def test_seen_episode_is_not_sent_to_a_later_subscription_after_restart(
    tmp_path: Path, astrbot_components: None
) -> None:
    first_context = FakeContext()
    manager, _, pending_path = make_manager(tmp_path, first_context)
    await manager.handle_transition(
        1,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=1000.0,
        episode_id=3,
    )

    second_context = FakeContext()
    manager2, subscriptions2, _ = make_manager(tmp_path, second_context)
    manager2.load()
    subscriptions2.subscribe("qq:GroupMessage:1", "owner-a")
    await manager2.handle_transition(
        1,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=9999.0,
        episode_id=3,
    )

    assert pending_path.exists()
    assert second_context.calls == []


@pytest.mark.asyncio
async def test_v1_pending_file_migrates_without_losing_delivery(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    manager, subscriptions, pending_path = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    pending_path.write_text(
        json.dumps(
            {
                "_schema_version": 1,
                "pending": {
                    "1:offline:1000.000000:legacy": {
                        "event_id": "1:offline:1000.000000:legacy",
                        "self_id": 1,
                        "event_type": "offline",
                        "event_time": 1000.0,
                        "duration_seconds": None,
                        "umo": "qq:GroupMessage:1",
                        "owner_id": "owner-a",
                        "attempts": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    manager.load()
    raw = json.loads(pending_path.read_text(encoding="utf-8"))
    assert raw["_schema_version"] == 2
    assert manager.pending_count == 1
    await manager.catch_up()
    assert [umo for umo, _ in context.calls] == ["qq:GroupMessage:1"]
    assert manager.pending_count == 0


def test_durable_dedupe_retention_is_bounded_and_protects_pending_ids(
    tmp_path: Path,
) -> None:
    store = PendingNotificationStore(tmp_path / "pending.json")
    pending = PendingNotification(
        event_id="1:offline:pending:target",
        self_id=1,
        event_type="offline",
        event_time=1000.0,
        duration_seconds=None,
        umo="qq:GroupMessage:1",
        owner_id="owner-a",
        episode_id=7,
    )
    store.upsert(pending)
    store.mark_seen("1:offline:pending")
    store.mark_completed(pending.event_id)

    for index in range(_MAX_DEDUPE_IDS + 64):
        store.mark_seen(f"seen:{index}")
        store.mark_completed(f"completed:{index}")

    assert len(store._seen) == _MAX_DEDUPE_IDS
    assert len(store._completed) == _MAX_DEDUPE_IDS
    assert "1:offline:pending" in store._seen
    assert pending.event_id in store._completed

    store.save()
    restored = PendingNotificationStore(store.path)
    restored.load()
    assert len(restored._seen) == _MAX_DEDUPE_IDS
    assert len(restored._completed) == _MAX_DEDUPE_IDS
    assert restored.get(pending.event_id) is not None
    assert restored.is_seen("1:offline:pending")
    assert restored.is_completed(pending.event_id)
