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
    NotificationPersistenceError,
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


class FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_manager(
    tmp_path: Path,
    context: FakeContext,
    *,
    html_render: Any = None,
    profile_provider: Any = None,
    profile_waiter: Any = None,
    profile_wait_timeout: float = 3.0,
    clock: FakeClock | None = None,
    state_provider: Any = None,
    target_guard: Any = None,
) -> tuple[NotificationManager, SubscriptionStore, Path]:
    clock = clock if clock is not None else FakeClock()
    subscriptions = SubscriptionStore(wall_clock=clock)
    pending_path = tmp_path / "pending_notifications.json"
    manager = NotificationManager(
        context,
        subscriptions,
        pending_path,
        html_render=html_render,
        profile_provider=profile_provider,
        profile_waiter=profile_waiter,
        profile_wait_timeout=profile_wait_timeout,
        wall_clock=clock,
        state_provider=state_provider,
        target_guard=target_guard,
        min_send_interval=30.0,
        failure_cooldown=300.0,
    )
    return manager, subscriptions, pending_path


def queue_notification(
    manager: NotificationManager,
    *,
    self_id: int = 1,
    umo: str = "qq:GroupMessage:1",
    event_type: str = "offline",
    event_time: float = 1000.0,
    episode_id: int = 1,
    attempts: int = 0,
) -> PendingNotification:
    item = PendingNotification(
        event_id=f"{self_id}:{event_type}:episode:{episode_id}:{umo}",
        self_id=self_id,
        event_type=event_type,
        event_time=event_time,
        duration_seconds=None,
        umo=umo,
        owner_id="owner-a",
        attempts=attempts,
        episode_id=episode_id,
    )
    manager.pending.upsert(item)
    return item


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

    clock = FakeClock()
    manager, subscriptions, _ = make_manager(
        tmp_path, context, html_render=render, clock=clock
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")

    await manager.handle_transition(
        1, QQStatus.ONLINE, TransitionKind.FIRST_ONLINE, event_time=1000.0
    )
    clock.advance(30.0)
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

    assert [umo for umo, _ in context.calls].count("qq:GroupMessage:bad") == 1
    assert [umo for umo, _ in context.calls].count("qq:GroupMessage:good") == 1
    assert manager.pending_count == 1
    assert pending_path.exists()
    raw = json.loads(pending_path.read_text(encoding="utf-8"))
    assert next(iter(raw["pending"].values()))["attempts"] == 1
    assert raw["delivery_targets"]["qq:GroupMessage:bad"] == 1300.0


@pytest.mark.asyncio
async def test_restart_catchup_is_quiet_and_unsubscribe_drops_target_state(
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
    clock = FakeClock()
    manager2 = NotificationManager(
        recovered, subscriptions, pending_path, wall_clock=clock
    )
    manager2.load()
    await manager2.catch_up()
    assert recovered.calls == []
    assert manager2.pending_count == 1
    assert manager2.dispatch_pending() == ()
    clock.advance(300.0)
    workers = manager2.dispatch_pending()
    assert len(workers) == 1
    await asyncio.gather(*workers)
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
    raw = json.loads(manager3.pending.path.read_text(encoding="utf-8"))
    assert "qq:GroupMessage:1" not in raw["delivery_targets"]


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
        manager.wall_clock = lambda: 1500.0
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
    manager.wall_clock = FakeClock(3000.0)
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
    manager.wall_clock = FakeClock(2000.0)
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
@pytest.mark.parametrize("schema_version", [1, 2])
async def test_legacy_pending_file_migrates_without_replaying_or_resetting_attempts(
    tmp_path: Path, astrbot_components: None, schema_version: int
) -> None:
    context = FakeContext()
    manager, subscriptions, pending_path = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    pending_path.write_text(
        json.dumps(
            {
                "_schema_version": schema_version,
                "pending": {
                    "1:offline:1000.000000:legacy": {
                        "event_id": "1:offline:1000.000000:legacy",
                        "self_id": 1,
                        "event_type": "offline",
                        "event_time": 1000.0,
                        "duration_seconds": None,
                        "umo": "qq:GroupMessage:1",
                        "owner_id": "owner-a",
                        "attempts": 2,
                    }
                },
                **(
                    {
                        "seen": ["historical-seen"],
                        "completed": ["historical-completed"],
                    }
                    if schema_version == 2
                    else {}
                ),
            }
        ),
        encoding="utf-8",
    )

    manager.load()
    raw = json.loads(pending_path.read_text(encoding="utf-8"))
    assert raw["_schema_version"] == 3
    assert raw["delivery_targets"] == {}
    assert next(iter(raw["pending"].values()))["attempts"] == 2
    if schema_version == 2:
        assert manager.pending.is_seen("historical-seen")
        assert manager.pending.is_completed("historical-completed")
    assert manager.pending_count == 1
    await manager.catch_up()
    assert context.calls == []
    assert manager.pending_count == 1
    await asyncio.gather(*manager.dispatch_pending())
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


@pytest.mark.asyncio
async def test_large_startup_backlog_is_quiet_and_keeps_only_latest_live_summaries(
    tmp_path: Path, astrbot_components: None
) -> None:
    clock = FakeClock(5000.0)
    context = FakeContext()
    render_calls: list[dict[str, Any]] = []

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        render_calls.append(data)
        return _PNG

    manager, subscriptions, _ = make_manager(
        tmp_path, context, clock=clock, html_render=render
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    for self_id in range(1, 101):
        queue_notification(manager, self_id=self_id, event_time=1000.0)
        queue_notification(manager, self_id=self_id, event_time=4800.0, episode_id=2)
        queue_notification(
            manager,
            self_id=self_id,
            event_type="recovered",
            event_time=4900.0,
            episode_id=2,
        )
    queue_notification(manager, self_id=101, event_time=1000.0)
    queue_notification(manager, umo="qq:GroupMessage:removed", event_time=4900.0)
    manager.pending.mark_seen("preserved-seen")
    manager.pending.mark_completed("preserved-completed")
    manager.persist()

    restored, restored_subscriptions, path = make_manager(
        tmp_path, context, clock=clock, html_render=render
    )
    restored_subscriptions.subscribe("qq:GroupMessage:1", "current-owner")
    restored.load()
    tasks_before = asyncio.all_tasks()
    await restored.catch_up()
    await restored.catch_up()
    assert asyncio.all_tasks() == tasks_before

    assert context.calls == []
    assert render_calls == []
    assert restored.pending_count == 100
    assert all(
        item.event_type == "recovered" and item.event_time == 4900.0
        for item in restored.pending.all()
    )
    assert restored.pending.is_seen("preserved-seen")
    assert restored.pending.is_completed("preserved-completed")
    assert len(json.loads(path.read_text(encoding="utf-8"))["pending"]) == 100
    workers = restored.dispatch_pending()
    assert isinstance(workers, tuple)
    assert len(workers) == 1
    await asyncio.gather(*workers)
    assert len(context.calls) == 1
    assert len(render_calls) == 1
    assert restored.pending_count == 99
    assert restored.dispatch_pending() == ()


@pytest.mark.asyncio
async def test_equal_time_recovery_wins_and_exhausted_latest_never_revives_old_state(
    tmp_path: Path, astrbot_components: None
) -> None:
    context = FakeContext()
    manager, subscriptions, _ = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    latest = queue_notification(
        manager, event_type="recovered", episode_id=3, attempts=3
    )
    queue_notification(manager, event_time=999.0, episode_id=1)
    queue_notification(manager, episode_id=2)
    manager.persist()

    await manager.catch_up()
    assert manager.pending.all() == [latest]
    assert manager.dispatch_pending() == ()
    assert latest.attempts == 3
    assert context.calls == []


@pytest.mark.asyncio
async def test_state_filter_drops_opposite_and_unknown_but_defers_pending_offline(
    tmp_path: Path, astrbot_components: None
) -> None:
    states = {1: "online", 2: "offline", 3: "pending_offline", 4: None}
    context = FakeContext()
    manager, subscriptions, _ = make_manager(
        tmp_path, context, state_provider=states.get
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    queue_notification(manager, self_id=1)
    queue_notification(manager, self_id=2, event_type="recovered")
    deferred = queue_notification(manager, self_id=3)
    queue_notification(manager, self_id=4)

    await manager.catch_up()
    assert manager.pending.all() == [deferred]
    assert manager.dispatch_pending() == ()
    assert context.calls == []
    states[3] = "offline"
    await asyncio.gather(*manager.dispatch_pending())
    assert len(context.calls) == 1
    assert manager.pending_count == 0


@pytest.mark.asyncio
async def test_three_total_attempts_stay_exhausted_across_repeated_reloads(
    tmp_path: Path, astrbot_components: None
) -> None:
    clock = FakeClock()
    target = "qq:GroupMessage:1"
    context = FakeContext(failures={target})
    manager, subscriptions, path = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe(target, "owner-a")
    await manager.handle_transition(
        1,
        QQStatus.OFFLINE,
        TransitionKind.OFFLINE_TIMEOUT,
        event_time=1000.0,
        episode_id=1,
    )

    for reload_index in range(1, 6):
        clock.advance(300.0)
        manager, subscriptions, _ = make_manager(tmp_path, context, clock=clock)
        subscriptions.subscribe(target, "owner-a")
        manager.load()
        await manager.catch_up()
        assert len(context.calls) == min(reload_index, 3)
        assert manager.pending.all()[0].attempts == min(reload_index, 3)
        workers = manager.dispatch_pending()
        assert len(workers) == (1 if reload_index < 3 else 0)
        await asyncio.gather(*workers)
        assert len(context.calls) == min(reload_index + 1, 3)
        raw = json.loads(path.read_text(encoding="utf-8"))
        assert next(iter(raw["pending"].values()))["attempts"] == min(
            reload_index + 1, 3
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("exceptional_failure", [False, True])
async def test_cooldown_survives_reload_and_new_events_without_blocking_healthy_group(
    tmp_path: Path, astrbot_components: None, exceptional_failure: bool
) -> None:
    clock = FakeClock()
    bad = "qq:GroupMessage:bad"
    good = "qq:GroupMessage:good"

    class MutedContext(FakeContext):
        async def send_message(self, umo: str, chain: FakeMessageChain) -> bool:
            result = await super().send_message(umo, chain)
            if exceptional_failure and umo == bad:
                raise PermissionError("bot muted")
            return result

    context = MutedContext(failures={bad})
    manager, subscriptions, path = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe(bad, "owner-a")
    subscriptions.subscribe(good, "owner-b")
    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=clock()
    )
    clock.advance(30.0)
    restored, subscriptions, _ = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe(bad, "owner-a")
    subscriptions.subscribe(good, "owner-b")
    restored.load()
    await restored.catch_up()
    await restored.handle_transition(
        2, QQStatus.ONLINE, TransitionKind.RECOVERED, event_time=clock()
    )
    assert [umo for umo, _ in context.calls].count(bad) == 1
    assert [umo for umo, _ in context.calls].count(good) == 2
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["delivery_targets"][bad] == 1300.0
    clock.advance(269.0)
    assert restored.dispatch_pending() == ()
    context.failures.clear()
    exceptional_failure = False
    clock.advance(1.0)
    workers = restored.dispatch_pending()
    assert len(workers) == 1
    await asyncio.gather(*workers)
    assert [umo for umo, _ in context.calls].count(bad) == 2
    assert len(context.calls[-1][1].chain) == 1
    assert (
        json.loads(path.read_text(encoding="utf-8"))["delivery_targets"][bad] == 1330.0
    )


@pytest.mark.asyncio
async def test_success_pacing_survives_reload_across_accounts_and_event_types(
    tmp_path: Path, astrbot_components: None
) -> None:
    clock = FakeClock()
    context = FakeContext()
    manager, subscriptions, _ = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=clock()
    )
    restored, subscriptions, _ = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-b")
    restored.load()
    await restored.handle_transition(
        2, QQStatus.ONLINE, TransitionKind.RECOVERED, event_time=1001.0
    )
    assert len(context.calls) == 1
    clock.advance(29.0)
    assert restored.dispatch_pending() == ()
    clock.advance(1.0)
    workers = restored.dispatch_pending()
    assert len(workers) == 1
    await asyncio.gather(*workers)
    assert len(context.calls) == 2
    assert len(context.calls[-1][1].chain) == 1


@pytest.mark.asyncio
async def test_concurrent_transitions_keep_one_sender_per_target(
    tmp_path: Path, astrbot_components: None
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    clock = FakeClock()
    active = 0
    peak = 0

    class BlockingContext(FakeContext):
        async def send_message(self, umo: str, chain: FakeMessageChain) -> bool:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            self.calls.append((umo, chain))
            started.set()
            try:
                await release.wait()
                return True
            finally:
                active -= 1

    context = BlockingContext()
    manager, subscriptions, _ = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    first = asyncio.create_task(
        manager.handle_transition(
            1,
            QQStatus.OFFLINE,
            TransitionKind.OFFLINE_TIMEOUT,
            event_time=1000.0,
            episode_id=1,
        )
    )
    try:
        await started.wait()
        await manager.handle_transition(
            1,
            QQStatus.OFFLINE,
            TransitionKind.OFFLINE_TIMEOUT,
            event_time=1000.0,
            episode_id=1,
        )
        await manager.handle_transition(
            2,
            QQStatus.ONLINE,
            TransitionKind.RECOVERED,
            event_time=1001.0,
            episode_id=1,
        )
        assert manager.dispatch_pending() == ()
        assert len(context.calls) == 1
        assert peak == 1
    finally:
        release.set()
        await first
    assert manager.pending_count == 1
    clock.advance(30.0)
    await asyncio.gather(*manager.dispatch_pending())
    assert len(context.calls) == 2
    assert peak == 1
    assert manager.pending_count == 0


@pytest.mark.asyncio
async def test_render_supersession_never_sends_obsolete_offline_card(
    tmp_path: Path, astrbot_components: None
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    rendered: list[str] = []

    async def render(template: str, data: dict[str, Any], **kwargs: Any) -> bytes:
        rendered.append(data["event_type"])
        if data["event_type"] == "offline":
            started.set()
            await release.wait()
        return _PNG

    context = FakeContext()
    manager, subscriptions, _ = make_manager(tmp_path, context, html_render=render)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    first = asyncio.create_task(
        manager.handle_transition(
            1,
            QQStatus.OFFLINE,
            TransitionKind.OFFLINE_TIMEOUT,
            event_time=1000.0,
            episode_id=1,
        )
    )
    try:
        await started.wait()
        await manager.handle_transition(
            1,
            QQStatus.ONLINE,
            TransitionKind.RECOVERED,
            event_time=1001.0,
            episode_id=1,
        )
        assert context.calls == []
    finally:
        release.set()
        await first
    assert context.calls == []
    await asyncio.gather(*manager.dispatch_pending())
    assert rendered == ["offline", "recovered"]
    assert len(context.calls) == 1
    assert len(context.calls[0][1].chain) == 1
    assert manager.pending_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["opposite", "unknown", "pending", "expired", "unsubscribed"]
)
async def test_render_rechecks_current_state_expiry_and_subscription_before_send(
    tmp_path: Path, astrbot_components: None, change: str
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    clock = FakeClock()
    states: dict[int, str | None] = {1: "offline"}

    async def render(*args: Any, **kwargs: Any) -> bytes:
        started.set()
        await release.wait()
        return _PNG

    context = FakeContext()
    manager, subscriptions, _ = make_manager(
        tmp_path,
        context,
        html_render=render,
        clock=clock,
        state_provider=states.get,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    notification = asyncio.create_task(
        manager.handle_transition(
            1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
        )
    )
    try:
        await started.wait()
        if change == "opposite":
            states[1] = "online"
        elif change == "unknown":
            states[1] = None
        elif change == "pending":
            states[1] = "pending_offline"
        elif change == "expired":
            clock.advance(3601.0)
        else:
            subscriptions.unsubscribe("qq:GroupMessage:1")
    finally:
        release.set()
        await notification
    assert context.calls == []
    assert manager.pending_count == (1 if change == "pending" else 0)
    if change == "pending":
        states[1] = "offline"
        await asyncio.gather(*manager.dispatch_pending())
        assert len(context.calls) == 1


@pytest.mark.asyncio
async def test_failed_pre_send_persistence_is_fail_closed(
    tmp_path: Path, astrbot_components: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = FakeContext()
    manager, subscriptions, path = make_manager(tmp_path, context)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    save = manager.pending.save
    failed_reservations: list[int] = []

    def fail_reservation() -> None:
        if any(item.attempts for item in manager.pending.all()):
            failed_reservations.append(1)
            raise NotificationPersistenceError("disk unavailable")
        save()

    monkeypatch.setattr(manager.pending, "save", fail_reservation)
    await manager.handle_transition(
        1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
    )
    assert failed_reservations
    assert context.calls == []
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert next(iter(raw["pending"].values()))["attempts"] == 0


@pytest.mark.asyncio
async def test_cancelled_send_keeps_durable_attempt_and_provisional_cooldown(
    tmp_path: Path, astrbot_components: None
) -> None:
    started = asyncio.Event()
    never_release = asyncio.Event()
    clock = FakeClock()

    class BlockingContext(FakeContext):
        async def send_message(self, umo: str, chain: FakeMessageChain) -> bool:
            self.calls.append((umo, chain))
            started.set()
            await never_release.wait()
            return True

    context = BlockingContext()
    manager, subscriptions, path = make_manager(tmp_path, context, clock=clock)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    notification = asyncio.create_task(
        manager.handle_transition(
            1, QQStatus.OFFLINE, TransitionKind.OFFLINE_TIMEOUT, event_time=1000.0
        )
    )
    try:
        await started.wait()
        raw = json.loads(path.read_text(encoding="utf-8"))
        assert next(iter(raw["pending"].values()))["attempts"] == 1
        assert raw["delivery_targets"]["qq:GroupMessage:1"] == 1300.0
    finally:
        notification.cancel()
        await asyncio.gather(notification, return_exceptions=True)
        await manager.close()
    recovered = FakeContext()
    restored, subscriptions, _ = make_manager(tmp_path, recovered, clock=clock)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    restored.load()
    await restored.catch_up()
    assert restored.pending.all()[0].attempts == 1
    assert restored.dispatch_pending() == ()
    clock.advance(300.0)
    await asyncio.gather(*restored.dispatch_pending())
    assert len(recovered.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["render", "send"])
async def test_close_cancels_owned_work_and_prevents_further_sends(
    tmp_path: Path, astrbot_components: None, phase: str
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    never_release = asyncio.Event()

    async def block() -> None:
        started.set()
        try:
            await never_release.wait()
        finally:
            cancelled.set()

    async def render(*args: Any, **kwargs: Any) -> bytes:
        if phase == "render":
            await block()
        return _PNG

    class BlockingContext(FakeContext):
        async def send_message(self, umo: str, chain: FakeMessageChain) -> bool:
            self.calls.append((umo, chain))
            await block()
            return True

    context = BlockingContext()
    manager, subscriptions, _ = make_manager(tmp_path, context, html_render=render)
    subscriptions.subscribe("qq:GroupMessage:1", "owner-a")
    queue_notification(manager)
    workers = manager.dispatch_pending()
    assert len(workers) == 1
    await started.wait()
    await manager.close()
    assert cancelled.is_set()
    assert all(worker.done() for worker in workers)
    assert len(context.calls) == (1 if phase == "send" else 0)
    assert manager.dispatch_pending() == ()
    await manager.handle_transition(
        2, QQStatus.ONLINE, TransitionKind.RECOVERED, event_time=1001.0
    )
    await manager.close()
    assert len(context.calls) == (1 if phase == "send" else 0)


@pytest.mark.parametrize("allowance", [-1.0, float("nan"), float("inf"), True, "1300"])
def test_invalid_delivery_target_state_is_rejected_fail_closed(
    tmp_path: Path, allowance: Any
) -> None:
    path = tmp_path / "pending.json"
    path.write_text(
        json.dumps(
            {
                "_schema_version": 3,
                "pending": {},
                "seen": [],
                "completed": [],
                "delivery_targets": {"qq:GroupMessage:1": allowance},
            }
        ),
        encoding="utf-8",
    )
    store = PendingNotificationStore(path)
    with pytest.raises(NotificationPersistenceError):
        store.load()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["blocked_before_render", "changed_during_render", "unavailable"]
)
async def test_target_policy_skip_never_spends_attempts_or_bypasses_cooldown(
    tmp_path: Path, astrbot_components: None, mode: str
) -> None:
    clock = FakeClock()
    context = FakeContext()
    ready = [mode == "changed_during_render"]
    rendered = []

    async def guard(umo: str) -> bool:
        if mode == "unavailable":
            raise RuntimeError("read-only membership lookup unavailable")
        return ready[0]

    async def render(template, data, **kwargs):
        rendered.append(data)
        ready[0] = False
        return _PNG

    manager, subscriptions, pending_path = make_manager(
        tmp_path,
        context,
        clock=clock,
        html_render=render,
        target_guard=guard,
    )
    subscriptions.subscribe("qq:GroupMessage:1", "owner")
    try:
        await manager.handle_transition(
            1,
            QQStatus.OFFLINE,
            TransitionKind.OFFLINE_TIMEOUT,
            event_time=clock(),
            episode_id=1,
        )
        assert context.calls == []
        assert len(rendered) == (1 if mode == "changed_during_render" else 0)
        assert [item.attempts for item in manager.pending.all()] == [0]
        loaded = PendingNotificationStore(pending_path)
        loaded.load()
        assert loaded.delivery_targets["qq:GroupMessage:1"] >= (
            clock() + manager.failure_cooldown
        )
        await manager.handle_transition(
            2,
            QQStatus.OFFLINE,
            TransitionKind.OFFLINE_TIMEOUT,
            event_time=clock(),
            episode_id=1,
        )
        assert context.calls == []
        assert all(item.attempts == 0 for item in manager.pending.all())
        assert len(rendered) == (1 if mode == "changed_during_render" else 0)
    finally:
        await manager.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("backup_failure", [False, True])
async def test_legacy_migration_preserves_private_backup_before_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backup_failure: bool
) -> None:
    import core.notifications as notification_module

    manager, subscriptions, path = make_manager(tmp_path, FakeContext())
    subscriptions.subscribe("qq:GroupMessage:1", "owner")
    item = queue_notification(manager, attempts=2)
    original = json.dumps(
        {
            "_schema_version": 2,
            "pending": {item.event_id: item.to_dict()},
            "seen": [],
            "completed": [],
        },
        indent=2,
    ).encode("utf-8")
    path.write_bytes(original)
    restored = NotificationManager(
        FakeContext(), subscriptions, path, wall_clock=FakeClock()
    )
    if backup_failure:
        real_open = notification_module.os.open

        def fail_backup(filename, flags, mode=0o777, **kwargs):
            if str(filename).endswith(".bak"):
                raise OSError("backup storage unavailable")
            return real_open(filename, flags, mode, **kwargs)

        monkeypatch.setattr(notification_module.os, "open", fail_backup)
    try:
        if backup_failure:
            with pytest.raises(NotificationPersistenceError):
                restored.load()
            assert path.read_bytes() == original
        else:
            restored.load()
            backups = list(tmp_path.glob(path.name + ".v2.*.bak"))
            assert len(backups) == 1
            assert backups[0].read_bytes() == original
            assert backups[0].stat().st_mode & 0o777 == 0o600
            assert restored.pending.get(item.event_id).attempts == 2
            assert json.loads(path.read_text())["_schema_version"] == 3
            restored.load()
            assert list(tmp_path.glob(path.name + ".v2.*.bak")) == backups
    finally:
        await manager.close()
        await restored.close()
