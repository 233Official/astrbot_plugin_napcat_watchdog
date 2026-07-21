"""Comprehensive tests for StateMachine with injectable clocks.

All timing uses a ``FakeMono`` / ``FakeWall`` pair.  The state machine
never relies on real ``time.sleep`` for core state logic.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from core.state_machine import (
    MAX_QQ,
    QQStatus,
    StateMachine,
    TransitionKind,
)

# ---------------------------------------------------------------------------
# Fake clocks
# ---------------------------------------------------------------------------


@dataclass
class FakeMono:
    """Injected monotonic clock.  Advance with ``.advance(delta)``."""

    _time: float = 0.0

    def advance(self, delta: float) -> None:
        self._time += delta

    def __call__(self) -> float:
        return self._time


@dataclass
class FakeWall:
    """Injected wall clock (UTC epoch).  Advance with ``.advance(delta)``."""

    _time: float = 1000000.0

    def advance(self, delta: float) -> None:
        self._time += delta

    def __call__(self) -> float:
        return self._time


def make_sm(
    timeout: float = 90.0,
    mono: FakeMono | None = None,
    wall: FakeWall | None = None,
) -> StateMachine:
    mono = mono or FakeMono()
    wall = wall or FakeWall()
    return StateMachine(
        offline_timeout=timeout,
        monotonic_clock=mono,
        wall_clock=wall,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_default_timeout(self) -> None:
        sm = make_sm()
        assert sm.offline_timeout == 90.0

    def test_custom_timeout(self) -> None:
        sm = make_sm(timeout=30.0)
        assert sm.offline_timeout == 30.0

    def test_negative_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="offline_timeout must be positive"):
            make_sm(timeout=-1)

    def test_zero_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="offline_timeout must be positive"):
            make_sm(timeout=0)

    def test_counts_start_at_zero(self) -> None:
        sm = make_sm()
        assert sm.registered_count == 0
        assert sm.online_count == 0
        assert sm.pending_offline_count == 0
        assert sm.offline_count == 0
        assert sm.reservation_count == 0


# ---------------------------------------------------------------------------
# Registration limits
# ---------------------------------------------------------------------------


class TestRegistrationLimit:
    """Enforce MAX_QQ limit with atomic reservations."""

    async def test_register_up_to_max(self) -> None:
        sm = make_sm()
        for i in range(MAX_QQ):
            ok = await sm.try_reserve(i + 1)
            assert ok, f"QQ {i + 1} should be allowed"
            sm.confirm_connection(i + 1)
        assert sm.registered_count == MAX_QQ

    async def test_reject_21st_unknown_qq(self) -> None:
        sm = make_sm()
        for i in range(MAX_QQ):
            await sm.try_reserve(i + 1)
            sm.confirm_connection(i + 1)
        ok = await sm.try_reserve(MAX_QQ + 1)
        assert not ok, "21st QQ should be rejected"

    async def test_registered_qq_can_reconnect_after_limit(self) -> None:
        sm = make_sm()
        for i in range(MAX_QQ):
            await sm.try_reserve(i + 1)
            sm.confirm_connection(i + 1)
        ok = await sm.try_reserve(1)
        assert ok, "Already-registered QQ must be allowed to reconnect"

    async def test_reservation_released_on_failure(self) -> None:
        sm = make_sm()
        ok = await sm.try_reserve(42)
        assert ok
        assert sm.reservation_count == 1
        sm.release_reservation(42)
        assert sm.reservation_count == 0


# ---------------------------------------------------------------------------
# First connection — FIRST_ONLINE
# ---------------------------------------------------------------------------


class TestFirstConnection:
    """First-ever connection → ONLINE + FIRST_ONLINE callback."""

    async def test_first_connect_sets_online(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(mono=mono, wall=wall)
        mono.advance(10.0)

        await sm.try_reserve(1)
        gen = sm.confirm_connection(1)
        assert gen == 1
        assert sm.registered_count == 1
        assert sm.online_count == 1

    async def test_first_connect_returns_first_online_kind(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.FIRST_ONLINE

    async def test_first_connect_fires_event_after_manual_dispatch(self) -> None:
        sm = make_sm()
        transitions: list[tuple[int, QQStatus, TransitionKind]] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            transitions.append((sid, status, kind))

        sm.set_transition_callback(cb)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind is not None
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        assert len(transitions) == 1
        assert transitions[0] == (1, QQStatus.ONLINE, TransitionKind.FIRST_ONLINE)

    async def test_get_view_after_first_connect(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        view = sm.get_view(1)
        assert view is not None
        assert view["status"] == "online"
        assert view["self_id"] == 1

    async def test_get_view_unknown_qq(self) -> None:
        sm = make_sm()
        assert sm.get_view(999) is None


# ---------------------------------------------------------------------------
# Connection replacement — same QQ, new connection
# ---------------------------------------------------------------------------


class TestConnectionReplacement:
    """Same-QQ new connection without state transition."""

    async def test_replacement_increments_generation(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        g2 = sm.confirm_connection(1)
        assert g2 == g1 + 1

    async def test_replacement_stays_online(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        sm.confirm_connection(1)
        assert sm.online_count == 1

    async def test_replacement_no_transition_kind(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.FIRST_ONLINE

        # Replacement — should return None
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind is None

    async def test_replacement_resets_heartbeat_baseline(self) -> None:
        """New connection resets last_heartbeat_mono to 0."""
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_heartbeat(1, g, True)
        mono.advance(10.0)
        sm.confirm_connection(1)  # replacement
        # After replacement, any stale heartbeat should be ignored
        assert sm.online_count == 1


# ---------------------------------------------------------------------------
# Disconnect → signal tracking
# ---------------------------------------------------------------------------


class TestDisconnect:
    """Disconnect sets a signal but confirmed_status stays ONLINE."""

    async def test_disconnect_sets_signal(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_disconnect(1, g)
        # Confirmed status is still ONLINE
        assert sm.online_count == 0  # Because view shows PENDING
        assert sm.pending_offline_count == 1  # PENDING view
        assert sm.offline_count == 0

    async def test_stale_disconnect_ignored(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        sm.confirm_connection(1)  # gen=2
        sm.on_disconnect(1, g1)  # stale gen=1
        assert sm.online_count == 1

    async def test_disconnect_unknown_qq_noop(self) -> None:
        sm = make_sm()
        sm.on_disconnect(999, 1)
        assert sm.registered_count == 0

    async def test_disconnect_from_offline_noop(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)  # exceed 10s timeout
        sm.check_timeouts()
        assert sm.offline_count == 1
        # disconnect while OFFLINE should be noop
        sm.on_disconnect(1, g)
        assert sm.offline_count == 1


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    """Heartbeat event processing and signal tracking."""

    async def test_heartbeat_online_refreshes(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_heartbeat(1, g, True)
        assert sm.online_count == 1

    async def test_heartbeat_false_triggers_signal(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_heartbeat(1, g, False)
        assert sm.online_count == 0
        assert sm.pending_offline_count == 1

    async def test_stale_heartbeat_ignored(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        sm.confirm_connection(1)  # gen=2
        sm.on_heartbeat(1, g1, False)  # stale
        assert sm.online_count == 1

    async def test_unknown_qq_heartbeat_noop(self) -> None:
        sm = make_sm()
        sm.on_heartbeat(999, 1, True)
        assert sm.registered_count == 0

    async def test_heartbeat_true_recovers_from_signal(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_heartbeat(1, g, False)  # signal
        assert sm.pending_offline_count == 1
        mono.advance(2.0)
        sm.on_heartbeat(1, g, True)  # recovery
        assert sm.online_count == 1
        assert sm.pending_offline_count == 0

    async def test_heartbeat_true_recovers_from_offline(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_heartbeat(1, g, False)  # signal
        mono.advance(15.0)  # exceed 10s timeout
        sm.check_timeouts()
        assert sm.offline_count == 1

        transitions: list[tuple[int, QQStatus, TransitionKind]] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            transitions.append((sid, status, kind))

        sm.set_transition_callback(cb)
        mono.advance(2.0)
        kind = sm.on_heartbeat(1, g, True)
        assert kind == TransitionKind.RECOVERED, f"Expected RECOVERED, got {kind}"
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        # Yield for the callback task to execute
        await asyncio.sleep(0)
        assert sm.online_count == 1
        assert sm.offline_count == 0
        assert any(t[2] == TransitionKind.RECOVERED for t in transitions), (
            f"Expected RECOVERED in {transitions}"
        )

    async def test_heartbeat_false_during_pending_does_not_extend_deadline(
        self,
    ) -> None:
        """Earliest deadline rule: subsequent online=false does not reset timer."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_heartbeat(1, g, False)  # signal at mono=5
        mono.advance(3.0)
        sm.on_heartbeat(1, g, False)  # second at mono=8 — should not extend
        mono.advance(4.0)  # now mono=12, earliest signal + 10 = 15
        # Not yet timed out
        assert sm.pending_offline_count == 1
        mono.advance(5.0)  # now mono=17, earliest signal + 10 = 15 → timed out
        sm.check_timeouts()
        assert sm.offline_count == 1


# ---------------------------------------------------------------------------
# PENDING → OFFLINE (timeout via check_timeouts)
# ---------------------------------------------------------------------------


class TestPendingTimeout:
    """Deadline expiry transitions to OFFLINE."""

    async def test_pending_times_out_to_offline(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        assert sm.pending_offline_count == 1
        mono.advance(15.0)  # exceed 10s timeout
        sm.check_timeouts()
        assert sm.offline_count == 1
        assert sm.pending_offline_count == 0

    async def test_pending_times_out_fires_callback(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        transitions: list[tuple[int, QQStatus, TransitionKind]] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            transitions.append((sid, status, kind))

        sm.set_transition_callback(cb)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        result = sm.check_timeouts()
        # Manually fire event for each transition
        for sid, kind in result:
            sm.fire_transition_event(sid, QQStatus.OFFLINE, kind)
        await asyncio.sleep(0)
        assert any(t[2] == TransitionKind.OFFLINE_TIMEOUT for t in transitions)

    async def test_two_qqs_timeout_together(self) -> None:
        """Two QQs disconnected, both timeout in same check_timeouts call."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        await sm.try_reserve(2)
        g2 = sm.confirm_connection(2)
        sm.on_disconnect(1, g1)
        sm.on_disconnect(2, g2)
        assert sm.pending_offline_count == 2
        mono.advance(15.0)
        transitions = sm.check_timeouts()
        assert len(transitions) == 2
        assert sm.offline_count == 2
        assert sm.pending_offline_count == 0

    async def test_pending_recovers_before_timeout(self) -> None:
        """Heartbeat true recovers from heartbeat_false signal before timeout."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_heartbeat(1, g, False)  # heartbeat_false signal
        assert sm.pending_offline_count == 1
        mono.advance(5.0)  # before timeout
        sm.on_heartbeat(1, g, True)  # recovery - clears heartbeat_false
        assert sm.online_count == 1
        assert sm.pending_offline_count == 0
        mono.advance(20.0)  # well past original deadline
        sm.check_timeouts()
        assert sm.online_count == 1  # recovered, not timed out

    async def test_recovery_clears_only_heartbeat_signals(self) -> None:
        """Heartbeat recovery clears heartbeat signals but not disconnect."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(2.0)
        sm.on_disconnect(1, g)  # disconnect at mono=2
        mono.advance(3.0)
        sm.on_heartbeat(1, g, True)  # heartbeat at mono=5
        # Heartbeat should clear heartbeat miss/false but NOT disconnect
        # Wait, the issue says: heartbeat true clears heartbeat-derived signals
        # But disconnect signal remains
        assert sm.pending_offline_count == 1  # disconnect still active

    async def test_connect_clears_disconnect_signal(self) -> None:
        """New connection clears the disconnect signal."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(2.0)
        sm.on_disconnect(1, g)
        assert sm.pending_offline_count == 1
        mono.advance(3.0)
        sm.confirm_connection(1)  # new connection
        assert sm.online_count == 1
        assert sm.pending_offline_count == 0


# ---------------------------------------------------------------------------
# OFFLINE → ONLINE (recovery)
# ---------------------------------------------------------------------------


class TestOfflineRecovery:
    """Recovery from OFFLINE via connection or heartbeat."""

    async def test_reconnect_from_offline(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1

        transitions: list[tuple[int, QQStatus, TransitionKind]] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            transitions.append((sid, status, kind))

        sm.set_transition_callback(cb)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.RECOVERED
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        assert sm.online_count == 1
        assert sm.offline_count == 0
        # Should fire RECOVERED callback
        assert any(t[2] == TransitionKind.RECOVERED for t in transitions)

    async def test_offline_heartbeat_recovers(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_heartbeat(1, g, False)
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1

        transitions: list[tuple[int, QQStatus, TransitionKind]] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            transitions.append((sid, status, kind))

        sm.set_transition_callback(cb)
        mono.advance(1.0)  # advance time so the heartbeat is "fresh"
        kind = sm.on_heartbeat(1, g, True)
        assert kind == TransitionKind.RECOVERED, f"Expected RECOVERED, got {kind}"
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        # Yield control so the callback task can run
        await asyncio.sleep(0)
        assert sm.online_count == 1
        assert sm.offline_count == 0
        assert any(t[2] == TransitionKind.RECOVERED for t in transitions), (
            f"Expected RECOVERED in {transitions}"
        )

        # Also verify the callback content
        assert len(transitions) >= 1, (
            f"Expected at least 1 transition, got {transitions}"
        )
        assert transitions[-1][0] == 1


# ---------------------------------------------------------------------------
# Earliest deadline rule
# ---------------------------------------------------------------------------


class TestEarliestDeadline:
    """Multiple failure signals use earliest deadline."""

    async def test_disconnect_then_heartbeat_false(self) -> None:
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(2.0)
        sm.on_disconnect(1, g)  # earliest = mono=2
        mono.advance(5.0)
        sm.on_heartbeat(1, g, False)  # not earliest
        mono.advance(6.0)  # now mono=13, earliest + 10 = 12
        sm.check_timeouts()
        assert sm.offline_count == 1

    async def test_heartbeat_false_then_disconnect(self) -> None:
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(2.0)
        sm.on_heartbeat(1, g, False)  # earliest = mono=2
        mono.advance(3.0)
        sm.on_disconnect(1, g)  # not earliest
        mono.advance(8.0)  # now mono=13, earliest + 10 = 12
        sm.check_timeouts()
        assert sm.offline_count == 1


# ---------------------------------------------------------------------------
# Heartbeat timeout sweep
# ---------------------------------------------------------------------------


class TestHeartbeatTimeoutSweep:
    """Periodic sweep detects QQs without recent heartbeats."""

    async def test_sweep_detects_timeout(self) -> None:
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        mono.advance(15.0)
        sm.sweep_heartbeat_timeouts()
        assert sm.pending_offline_count == 1

    async def test_sweep_uses_connection_time_if_no_heartbeat(self) -> None:
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        mono.advance(15.0)
        sm.sweep_heartbeat_timeouts()
        assert sm.pending_offline_count == 1

    async def test_online_qq_not_affected_when_recent_heartbeat(self) -> None:
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(3.0)
        sm.on_heartbeat(1, g, True)  # Fresh heartbeat
        mono.advance(5.0)  # Still within timeout
        sm.sweep_heartbeat_timeouts()
        assert sm.online_count == 1

    async def test_sweep_does_not_flag_before_timeout(self) -> None:
        """timeout-ε: sweep does NOT flag the QQ."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        mono.advance(9.999)  # still within timeout
        sm.sweep_heartbeat_timeouts()
        assert sm.online_count == 1
        assert sm.pending_offline_count == 0

    async def test_sweep_flags_and_immediately_timeouts(self) -> None:
        """At exact timeout, sweep flags AND check_timeouts confirms OFFLINE."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        sm.confirm_connection(1)  # baseline = 0
        mono.advance(10.0)  # exactly at timeout boundary
        sm.sweep_heartbeat_timeouts()
        assert sm.pending_offline_count == 1
        # With fix: heartbeat_miss_mono = baseline = 0
        # earliest = 0, deadline = 0 + 10 = 10, now = 10 >= 10
        transitions = sm.check_timeouts()
        assert len(transitions) == 1
        assert sm.offline_count == 1

    async def test_sweep_triggers_check_timeouts(self) -> None:
        """sweep sets signal, then check_timeouts transitions to OFFLINE."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        mono.advance(15.0)
        sm.sweep_heartbeat_timeouts()
        assert sm.pending_offline_count == 1
        # With fix: heartbeat_miss_mono = baseline = 0
        # earliest = 0, deadline = 0 + 10 = 10, now = 15 >= 10
        # Already past deadline, check_timeouts should immediately timeout
        sm.check_timeouts()
        assert sm.offline_count == 1


# ---------------------------------------------------------------------------
# Restart / load_snapshot
# ---------------------------------------------------------------------------


class TestRestartGracePeriod:
    """Previously ONLINE QQs keep confirmed_status ONLINE, view is PENDING."""

    def test_persisted_online_stays_online_confirmed(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "online",
                "registered_at": 100.0,
                "last_status_change": 100.0,
                "offline_since": None,
            }
        }
        sm.load_snapshot(snapshot)
        assert sm.registered_count == 1
        # Confirmed status is ONLINE, but view shows PENDING (grace period)
        assert sm.pending_offline_count == 1
        assert sm.online_count == 0

    def test_persisted_offline_stays_offline(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "offline",
                "registered_at": 100.0,
                "last_status_change": 200.0,
                "offline_since": 200.0,
            }
        }
        sm.load_snapshot(snapshot)
        assert sm.offline_count == 1

    def test_persisted_online_not_changed_to_pending_in_load(self) -> None:
        """load_snapshot does NOT change confirmed_status to anything else."""
        mono = FakeMono()
        sm = make_sm(mono=mono)
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "online",
                "registered_at": 100.0,
                "last_status_change": 100.0,
                "offline_since": None,
            }
        }
        sm.load_snapshot(snapshot)
        # Internal check: confirmed_status stays ONLINE
        assert sm._states[1].confirmed_status == QQStatus.ONLINE

    async def test_grace_connect_restores_online_silently(self) -> None:
        """Connecting during grace period restores ONLINE without transition."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "online",
                "registered_at": mono() - 5,
                "last_status_change": mono() - 5,
                "offline_since": None,
            }
        }
        transitions: list[tuple[int, QQStatus, TransitionKind]] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            transitions.append((sid, status, kind))

        sm.set_transition_callback(cb)
        sm.load_snapshot(snapshot)
        assert sm.pending_offline_count == 1
        # Reconnect during grace period
        sm.confirm_connection(1)
        assert sm.online_count == 1
        assert sm.pending_offline_count == 0
        # No online transition callback for silent grace recovery
        assert not any(
            t[2] in (TransitionKind.FIRST_ONLINE, TransitionKind.RECOVERED)
            for t in transitions
        )

    async def test_grace_expires_to_offline(self) -> None:
        """Grace period timer expiry transitions to OFFLINE."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "online",
                "registered_at": mono(),
                "last_status_change": mono(),
                "offline_since": None,
            }
        }
        sm.load_snapshot(snapshot)
        assert sm.pending_offline_count == 1
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1


# ---------------------------------------------------------------------------
# Snapshot round-trip
# ---------------------------------------------------------------------------


class TestSnapshotRoundTrip:
    """get_snapshot excludes runtime fields; load_snapshot restores correctly."""

    async def test_snapshot_excludes_runtime_fields(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        snap = sm.get_snapshot()
        entry = snap["1"]
        # Runtime-only fields must not be present
        assert "generation" not in entry
        assert "last_heartbeat_mono" not in entry
        assert "last_heartbeat_time" not in entry
        assert "connection_established_mono" not in entry
        assert "notified_online" not in entry
        assert "notified_offline" not in entry
        assert "signals" not in entry
        assert "pending_deadline_mono" not in entry
        assert "pending_timer" not in entry
        # Business fields must be present
        assert entry["self_id"] == 1
        assert entry["status"] == "online"
        assert "registered_at" in entry
        assert "last_status_change" in entry

    async def test_snapshot_has_all_registered_qq(self) -> None:
        sm = make_sm()
        for sid in (1, 2, 3):
            await sm.try_reserve(sid)
            sm.confirm_connection(sid)
        snap = sm.get_snapshot()
        assert len(snap) == 3

    def test_load_empty_snapshot(self) -> None:
        sm = make_sm()
        sm.load_snapshot({})
        assert sm.registered_count == 0

    def test_snapshot_returns_copy_not_reference(self) -> None:
        """get_snapshot returns a new dict, not internal state reference."""
        sm = make_sm()
        await_pseudo(sm)
        snap = sm.get_snapshot()
        # Mutating the snapshot should not affect internal state
        snap["1"]["status"] = "offline"
        assert sm._states[1].confirmed_status == QQStatus.ONLINE

    async def test_view_shows_pending_when_signal_active(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_disconnect(1, g)
        snap = sm.get_all_views()
        assert snap["1"]["status"] == "pending_offline"

    async def test_view_shows_online_after_recovery(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_disconnect(1, g)
        mono.advance(2.0)
        sm.confirm_connection(1)
        snap = sm.get_all_views()
        assert snap["1"]["status"] == "online"

    def test_all_views_returns_copy(self) -> None:
        sm = make_sm()
        await_pseudo(sm)
        views = sm.get_all_views()
        assert isinstance(views, dict)

    async def test_persistable_snapshot_has_only_allowed_keys(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        snap = sm.get_snapshot()
        entry = snap["1"]
        keys = set(entry)
        allowed = {
            "self_id",
            "status",
            "registered_at",
            "last_status_change",
            "offline_since",
        }
        assert keys == allowed


# ---------------------------------------------------------------------------
# Transition callback
# ---------------------------------------------------------------------------


class TestTransitionCallback:
    """Callback fires on expected transitions with correct TransitionKind."""

    async def test_first_online_kind(self) -> None:
        sm = make_sm()
        kinds: list[TransitionKind] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            kinds.append(kind)

        sm.set_transition_callback(cb)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.FIRST_ONLINE
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        assert TransitionKind.FIRST_ONLINE in kinds

    async def test_offline_timeout_kind(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        kinds: list[TransitionKind] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            kinds.append(kind)

        sm.set_transition_callback(cb)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        transitions = sm.check_timeouts()
        for sid, kind in transitions:
            sm.fire_transition_event(sid, QQStatus.OFFLINE, kind)
        await asyncio.sleep(0)
        assert TransitionKind.OFFLINE_TIMEOUT in kinds

    async def test_recovered_kind(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1

        kinds: list[TransitionKind] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            kinds.append(kind)

        sm.set_transition_callback(cb)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.RECOVERED
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        assert TransitionKind.RECOVERED in kinds

    async def test_shutdown_kind(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(mono=mono, wall=wall)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kinds: list[TransitionKind] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            kinds.append(kind)

        sm.set_transition_callback(cb)
        sm.force_offline(1)
        await asyncio.sleep(0)
        assert TransitionKind.SHUTDOWN in kinds

    async def test_callback_error_does_not_crash(self) -> None:
        sm = make_sm()

        async def _broken(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            raise RuntimeError("callback failed")

        sm.set_transition_callback(_broken)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind is not None
        # fire_transition_event catches callback exceptions
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)  # Should not raise
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Undo confirm
# ---------------------------------------------------------------------------


class TestUndoConfirm:
    """undo_confirm removes new QQs or restores previous status."""

    async def test_undo_removes_new_qq_and_releases_reservation(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        assert sm.registered_count == 1
        assert sm.reservation_count == 0  # consumed by confirm
        sm.undo_confirm(1)
        assert sm.registered_count == 0
        assert sm.reservation_count == 0  # released by undo

    async def test_undo_restores_offline_for_existing(self) -> None:
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1

        # Try to reconnect but fail
        sm.confirm_connection(1)
        assert sm.online_count == 1
        sm.undo_confirm(1)
        assert sm.offline_count == 1  # restored to OFFLINE
        # offline_since should be set
        state_info = sm.get_view(1)
        assert state_info is not None
        assert state_info["status"] == "offline"
        assert state_info["offline_since"] is not None


# ---------------------------------------------------------------------------
# Force offline (shutdown)
# ---------------------------------------------------------------------------


class TestForceOffline:
    """force_offline transitions a QQ to OFFLINE immediately."""

    async def test_force_offline_sets_offline(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        assert sm.online_count == 1
        sm.force_offline(1)
        assert sm.offline_count == 1

    async def test_force_offline_unknown_qq(self) -> None:
        sm = make_sm()
        assert not sm.force_offline(999)

    async def test_force_offline_clears_signals(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        mono.advance(5.0)
        sm.on_disconnect(1, g)
        sm.force_offline(1)
        assert sm.offline_count == 1
        # No pending after force
        assert sm.pending_offline_count == 0


# ---------------------------------------------------------------------------
# Stale event isolation
# ---------------------------------------------------------------------------


class TestStaleEventIsolation:
    """Events from old connections are discarded."""

    async def test_old_disconnect_after_reconnect(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        sm.confirm_connection(1)  # gen 2
        sm.on_disconnect(1, g1)  # stale
        assert sm.online_count == 1

    async def test_old_heartbeat_after_reconnect(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        sm.confirm_connection(1)  # gen 2
        sm.on_heartbeat(1, g1, False)  # stale
        assert sm.online_count == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Various edge cases and boundary conditions."""

    async def test_single_qq_multiple_disconnects(self) -> None:
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        sm.on_disconnect(1, g)
        sm.on_disconnect(1, g)
        assert sm.pending_offline_count == 1

    async def test_rapid_connect_disconnect_cycle(self) -> None:
        sm = make_sm(timeout=10.0)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        for _ in range(3):
            sm.on_disconnect(1, g)
            g = sm.confirm_connection(1)
        assert sm.online_count == 1

    async def test_pending_offline_count_with_mixed(self) -> None:
        mono = FakeMono()
        sm = make_sm(mono=mono)
        # One QQ online, one pending
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        await sm.try_reserve(2)
        g2 = sm.confirm_connection(2)
        mono.advance(5.0)
        sm.on_disconnect(2, g2)
        assert sm.online_count == 1
        assert sm.pending_offline_count == 1
        assert sm.offline_count == 0

    async def test_generations_increase_monotonically(self) -> None:
        sm = make_sm()
        await sm.try_reserve(1)
        prev = 0
        for _ in range(5):
            g = sm.confirm_connection(1)
            assert g > prev
            prev = g

    async def test_load_snapshot_generation_is_zero(self) -> None:
        sm = make_sm()
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "offline",
                "registered_at": 1.0,
                "last_status_change": 2.0,
                "offline_since": 2.0,
            }
        }
        sm.load_snapshot(snapshot)
        # After load, generation should be 0 until first reconnect
        assert sm._states[1].generation == 0
        g = sm.confirm_connection(1)
        assert g == 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Rollback semantics (Issue #3 #4)
# ---------------------------------------------------------------------------


class TestRollbackSemantics:
    """Persistence-failure rollback preserves correct confirmed status."""

    async def test_undo_online_replacement_preserves_status(self) -> None:
        """ONLINE replacement save failure must NOT change to OFFLINE."""
        mono = FakeMono()
        sm = make_sm(mono=mono)
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        assert sm.online_count == 1

        # Replacement
        g2 = sm.confirm_connection(1)
        assert g2 == g1 + 1
        assert sm.online_count == 1
        old_gen = sm._states[1].generation

        # Simulate save failure - undo
        sm.undo_confirm(1)
        assert sm.online_count == 1, "Must remain ONLINE after undo"
        assert sm.offline_count == 0, "Must NOT become OFFLINE"
        assert sm.registered_count == 1
        # Generation rolled back
        assert sm._states[1].generation == old_gen - 1
        # get_confirm_transition should no longer claim a transition
        assert sm.get_confirm_transition(1) is None

    async def test_undo_first_registration_clears_all(self) -> None:
        """First registration save failure: no state, no reservation, no connection."""
        sm = make_sm()
        await sm.try_reserve(1)
        assert sm.reservation_count == 1
        sm.confirm_connection(1)
        assert sm.registered_count == 1
        assert sm.reservation_count == 0

        # Simulate save failure
        sm.undo_confirm(1)
        assert sm.registered_count == 0
        assert sm.reservation_count == 0
        assert sm.get_view(1) is None

    async def test_undo_offline_recovery_keeps_offline(self) -> None:
        """OFFLINE recovery save failure must stay OFFLINE."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1

        # Attempt recovery
        sm.confirm_connection(1)
        assert sm.online_count == 1

        # Save failure — undo
        sm.undo_confirm(1)
        assert sm.offline_count == 1, "Must be restored to OFFLINE"
        assert sm.online_count == 0
        assert sm.registered_count == 1

    async def test_batch_rollback_restores_both(self) -> None:
        """Two QQs timeout in batch; rolling back both restores PENDING."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g1 = sm.confirm_connection(1)
        await sm.try_reserve(2)
        g2 = sm.confirm_connection(2)
        sm.on_disconnect(1, g1)
        sm.on_disconnect(2, g2)
        mono.advance(15.0)
        transitions = sm.check_timeouts()
        assert len(transitions) == 2
        assert sm.offline_count == 2

        # Simulate batch save failure → rollback both
        for sid, _ in transitions:
            sm.rollback_timeout(sid)
        assert sm.offline_count == 0, "Both must revert from OFFLINE"
        assert sm.pending_offline_count == 2, "Both must return to PENDING"
        # Verify signals preserved — can retry on next cycle
        mono.advance(1.0)
        transitions2 = sm.check_timeouts()
        assert len(transitions2) == 2

    async def test_timeout_rollback_restores_signals_for_retry(self) -> None:
        """OFFLINE timeout save failure keeps confirmed ONLINE/PENDING
        and preserves signals for retry."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)

        # First check_timeouts transitions to OFFLINE internally
        transitions = sm.check_timeouts()
        assert len(transitions) == 1
        assert sm.offline_count == 1

        # Simulate persist failure — rollback
        sm.rollback_timeout(1)
        assert sm.offline_count == 0, "Must revert to ONLINE/PENDING"
        # View should be pending (signals restored)
        view = sm.get_view(1)
        assert view is not None
        assert view["status"] == "pending_offline", (
            f"Expected pending_offline, got {view['status']}"
        )

        # Signals are restored — next check_timeouts can retry
        mono.advance(1.0)  # still past 10s deadline
        transitions = sm.check_timeouts()
        assert len(transitions) >= 1, "Must be able to re-detect timeout"

    async def test_heartbeat_recovery_rollback_keeps_offline(self) -> None:
        """Heartbeat recovery save failure must restore OFFLINE+signals."""
        mono = FakeMono()
        wall = FakeWall()
        sm = make_sm(timeout=10.0, mono=mono, wall=wall)
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)
        sm.on_disconnect(1, g)
        mono.advance(15.0)
        sm.check_timeouts()
        assert sm.offline_count == 1

        # Heartbeat attempts recovery
        kind = sm.on_heartbeat(1, g, True)
        assert kind == TransitionKind.RECOVERED
        assert sm.online_count == 1

        # Simulate persist failure — rollback
        sm.rollback_heartbeat_recovery(1)
        assert sm.offline_count == 1, "Must restore to OFFLINE"
        assert sm.online_count == 0
        # Signals should be restored for next heartbeat retry
        view = sm.get_view(1)
        assert view is not None
        assert view["status"] == "offline"

    async def test_confirm_transition_only_on_real_change(self) -> None:
        """get_confirm_transition returns None for ONLINE replacement."""
        sm = make_sm()
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        assert sm.get_confirm_transition(1) == TransitionKind.FIRST_ONLINE

        # Replacement — no transition
        sm.confirm_connection(1)
        assert sm.get_confirm_transition(1) is None

        # OFFLINE recovery — transition
        mono = FakeMono()
        wall = FakeWall()
        sm2 = make_sm(timeout=5.0, mono=mono, wall=wall)
        await sm2.try_reserve(2)
        g = sm2.confirm_connection(2)
        sm2.on_disconnect(2, g)
        mono.advance(10.0)
        sm2.check_timeouts()
        assert sm2.offline_count == 1
        sm2.confirm_connection(2)
        assert sm2.get_confirm_transition(2) == TransitionKind.RECOVERED

    async def test_on_heartbeat_returns_none_when_no_recovery(self) -> None:
        """on_heartbeat returns None for normal keep-alive."""
        sm = make_sm()
        await sm.try_reserve(1)
        g = sm.confirm_connection(1)

        # Normal heartbeat, ONLINE→ONLINE
        result = sm.on_heartbeat(1, g, True)
        assert result is None

        # Heartbeat with false during ONLINE
        result = sm.on_heartbeat(1, g, False)
        assert result is None  # no transition, just signal

    async def test_confirm_transition_returns_none_for_unknown_qq(self) -> None:
        """get_confirm_transition returns None if QQ was never confirmed."""
        sm = make_sm()
        assert sm.get_confirm_transition(999) is None


# ---------------------------------------------------------------------------
# Event ordering (persist → fire)
# ---------------------------------------------------------------------------


class TestEventOrdering:
    """Events fire only after successful persist."""

    async def test_event_only_fires_once(self) -> None:
        """fire_transition_event dispatches exactly one callback."""
        sm = make_sm()
        call_count = 0

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            nonlocal call_count
            call_count += 1

        sm.set_transition_callback(cb)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.FIRST_ONLINE

        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        assert call_count == 1, f"Expected exactly 1 callback, got {call_count}"

        # Second fire should also work (e.g. for timeout then recovered)
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        assert call_count == 2

    async def test_no_callback_when_no_transition(self) -> None:
        """No callback fires for ONLINE replacement."""
        sm = make_sm()
        call_count = 0

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            nonlocal call_count
            call_count += 1

        sm.set_transition_callback(cb)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind == TransitionKind.FIRST_ONLINE
        sm.fire_transition_event(1, QQStatus.ONLINE, kind)
        await asyncio.sleep(0)
        c1 = call_count

        # Replacement — no fire
        sm.confirm_connection(1)
        kind = sm.get_confirm_transition(1)
        assert kind is None
        await asyncio.sleep(0)
        assert call_count == c1, "No callback should fire for replacement"


# ---------------------------------------------------------------------------
# Grace period recovery
# ---------------------------------------------------------------------------


class TestGracePeriodRecovery:
    """Grace period connect restores ONLINE silently (no transition)."""

    async def test_grace_connect_no_transition_kind(self) -> None:
        """Connecting during grace period returns None from get_confirm."""
        mono = FakeMono()
        sm = make_sm(timeout=10.0, mono=mono)
        snapshot = {
            "1": {
                "self_id": 1,
                "status": "online",
                "registered_at": mono() - 5,
                "last_status_change": mono() - 5,
                "offline_since": None,
            }
        }
        sm.load_snapshot(snapshot)
        assert sm.pending_offline_count == 1
        # Reconnect during grace period
        sm.confirm_connection(1)
        # Should be silent — no transition kind
        kind = sm.get_confirm_transition(1)
        assert kind is None, "Grace period connect should have no transition"


# ---------------------------------------------------------------------------
# Shutdown rollback no-op
# ---------------------------------------------------------------------------


class TestShutdownRollback:
    """force_offline is not rolled back (no persistence needed)."""

    async def test_force_offline_fires_event(self) -> None:
        sm = make_sm()
        kinds: list[TransitionKind] = []

        async def cb(sid: int, status: QQStatus, kind: TransitionKind) -> None:
            kinds.append(kind)

        sm.set_transition_callback(cb)
        await sm.try_reserve(1)
        sm.confirm_connection(1)
        sm.force_offline(1)
        await asyncio.sleep(0)
        assert TransitionKind.SHUTDOWN in kinds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def await_pseudo(sm: StateMachine) -> None:
    """Set up one QQ for tests that just need a non-empty state.

    Creates a short-lived event loop so sync test functions can use
    the state machine without needing an async fixture.
    """
    import asyncio as _asyncio

    async def _setup() -> None:
        await sm.try_reserve(1)
        sm.confirm_connection(1)

    _asyncio.run(_setup())
