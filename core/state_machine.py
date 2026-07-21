"""QQ registration, clock-injectable state machine with signal tracking.

This module implements the core business logic for Issue #3:

- Automatic QQ registration from ``X-Self-ID`` (max 20).
- Single authoritative connection generation per QQ.
- Atomic slot reservation for concurrent first connections.
- Configurable offline-timeout debounce state machine.
- Multiple failure-signal coalescing with earliest-deadline rule.
- Injectable monotonic/wall clocks for deterministic testing.
- Restart grace period for previously online QQs.
- Transition notification via callback — coordinator is responsible for
  persistence-and-fire ordering (Issue #3 #4).
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

MAX_QQ = 20
"""Hard limit on simultaneously registered QQ instances."""


class QQStatus(Enum):
    """Persisted confirmed status — only ONLINE and OFFLINE.

    ``PENDING_OFFLINE`` is **never** stored; it is a runtime-derived view
    that :meth:`get_snapshot` computes when an ONLINE QQ has active
    failure signals or is within the restart grace period.
    """

    ONLINE = "online"
    OFFLINE = "offline"


class TransitionKind(Enum):
    """Classifies every state-machine transition for the coordinator layer.

    Used by :class:`TransitionCallback` so the caller can distinguish
    first-time registration from recovery or timeout.
    """

    FIRST_ONLINE = "first_online"
    """Brand-new QQ first-ever connected — needs persistence."""

    RECOVERED = "recovered"
    """OFFLINE QQ reconnected or heartbeat restored — needs persistence."""

    OFFLINE_TIMEOUT = "offline_timeout"
    """Pending offline signals timed out — needs persistence."""

    SHUTDOWN = "shutdown"
    """Plugin termination — forced OFFLINE, no persistence."""


TransitionCallback = Callable[[int, QQStatus, TransitionKind], Awaitable[None]]
"""Async callback ``fn(self_id, new_confirmed_status, kind)``.

Fired after the coordinator has persisted the transition.  Raising from
this callback does **not** roll back persistence.
"""


# ---------------------------------------------------------------------------
# Internal signal tracking
# ---------------------------------------------------------------------------


@dataclass
class _Signals:
    """Independent failure-signal timestamps for earliest-deadline rule.

    Each signal tracks its own monotonic start time.  Recovery clears
    only the specific signal that was resolved.  The ``deadline_mono``
    is ``earliest_mono + offline_timeout`` and is recomputed on every
    signal change.
    """

    disconnect_mono: float | None = None
    """WS disconnect detected."""

    heartbeat_false_mono: float | None = None
    """Heartbeat with ``online=false`` received."""

    heartbeat_miss_mono: float | None = None
    """Heartbeat timeout sweep flagged this QQ."""

    grace_start_mono: float | None = None
    """Restart grace period for a previously ONLINE QQ."""

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def earliest_mono(self) -> float | None:
        candidates = [
            t
            for t in (
                self.disconnect_mono,
                self.heartbeat_false_mono,
                self.heartbeat_miss_mono,
                self.grace_start_mono,
            )
            if t is not None
        ]
        return min(candidates) if candidates else None

    @property
    def has_any(self) -> bool:
        return any(
            (
                self.disconnect_mono is not None,
                self.heartbeat_false_mono is not None,
                self.heartbeat_miss_mono is not None,
                self.grace_start_mono is not None,
            )
        )

    # ------------------------------------------------------------------
    # Selective clearing
    # ------------------------------------------------------------------

    def clear_disconnect(self) -> None:
        self.disconnect_mono = None

    def clear_heartbeat_false(self) -> None:
        self.heartbeat_false_mono = None

    def clear_heartbeat_miss(self) -> None:
        self.heartbeat_miss_mono = None

    def clear_grace(self) -> None:
        self.grace_start_mono = None

    def clear_all(self) -> None:
        self.disconnect_mono = None
        self.heartbeat_false_mono = None
        self.heartbeat_miss_mono = None
        self.grace_start_mono = None


# ---------------------------------------------------------------------------
# Per-QQ internal state
# ---------------------------------------------------------------------------


@dataclass
class _QQData:
    """Per-QQ runtime and persisted business state.

    Runtime-only fields (monotonic clocks, ``generation``, connection
    refs, timer references) are excluded from JSON persistence — see
    :meth:`StateMachine.get_snapshot`.
    """

    self_id: int
    confirmed_status: QQStatus  # only ONLINE or OFFLINE
    generation: int
    registered_at: float  # UTC epoch seconds
    last_status_change: float  # UTC epoch seconds
    offline_since: float | None = None  # UTC epoch seconds

    # Runtime-only — never persisted
    connection_established_mono: float | None = None
    last_heartbeat_mono: float = 0.0  # 0.0 = never received
    last_heartbeat_at: float | None = None  # wall-clock UTC epoch; None = never
    signals: _Signals = field(default_factory=_Signals)
    pending_timer: asyncio.Task[None] | None = None


# ---------------------------------------------------------------------------
# Pre-transition snapshot for rollback
# ---------------------------------------------------------------------------


@dataclass
class _PreTimeout:
    """Saved state before a timeout transition, used for rollback."""

    confirmed_status: QQStatus
    last_status_change: float
    offline_since: float | None
    signals: _Signals
    had_timer: bool


@dataclass
class _PreHbRecovery:
    """Saved state before a heartbeat-recovery transition, used for rollback."""

    confirmed_status: QQStatus
    last_status_change: float
    offline_since: float | None
    signals: _Signals


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


class StateMachine:
    """QQ state machine with registration limit, clock injection, debounce.

    Manages at most :data:`MAX_QQ` simultaneously registered QQs.  Each
    connection cycle gets a monotonic generation number; events carrying a
    stale generation are silently discarded.

    **Transition rules**::

        (nonexistent) → confirm_connection  → ONLINE  ✉ FIRST_ONLINE
        ONLINE        → on_disconnect       → ONLINE  (sets disconnect signal)
        ONLINE        → on_heartbeat(false))→ ONLINE  (sets heartbeat_false signal)
        ONLINE        → sweep timeout       → ONLINE  (sets heartbeat_miss signal)
        ONLINE+PENDING→ on_heartbeat(true)  → ONLINE  (clears hb signals)
        ONLINE+PENDING→ confirm_connection  → ONLINE  (silent, clears signals)
        OFFLINE+PENDING→ deadline reached   → OFFLINE ✉ OFFLINE_TIMEOUT
        OFFLINE       → confirm_connection  → ONLINE  ✉ RECOVERED
        OFFLINE       → on_heartbeat(true)  → ONLINE  ✉ RECOVERED

    ``✉`` marks transitions for which the coordinator **must** persist
    before calling :meth:`fire_transition_event`.  The state machine
    provides rollback methods for persistence failures.

    Parameters
    ----------
    offline_timeout :
        Seconds after which a QQ with active failure signals transitions
        to ``OFFLINE``.  Must be positive.
    monotonic_clock :
        Callable returning monotonic seconds.  Defaults to
        :func:`time.monotonic`.  Injected for test determinism.
    wall_clock :
        Callable returning UTC epoch seconds.  Defaults to
        :func:`time.time`.  Injected for test determinism.
    """

    def __init__(
        self,
        offline_timeout: float = 90.0,
        monotonic_clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if offline_timeout <= 0:
            raise ValueError("offline_timeout must be positive")
        self._offline_timeout = offline_timeout
        self._monotonic = monotonic_clock
        self._wall = wall_clock

        self._states: dict[int, _QQData] = {}
        self._reservations: set[int] = set()
        self._reservation_lock = asyncio.Lock()
        self._on_transition: TransitionCallback | None = None

        # --- Pre-transition state for rollback ---
        # confirm_connection: old status before potential transition
        self._confirm_old_status: dict[int, QQStatus] = {}
        # Track brand-new confirmations (distinct from loaded-from-snapshot)
        self._new_confirm_ids: set[int] = set()
        # check_timeouts: pre-transition snapshot
        self._pre_timeout: dict[int, _PreTimeout] = {}
        # on_heartbeat recovery: pre-transition snapshot
        self._pre_hb_recovery: dict[int, _PreHbRecovery] = {}

    # ---- Public properties ----

    @property
    def offline_timeout(self) -> float:
        return self._offline_timeout

    @property
    def registered_count(self) -> int:
        return len(self._states)

    @property
    def reservation_count(self) -> int:
        return len(self._reservations)

    @property
    def online_count(self) -> int:
        """Number of QQs whose confirmed status is ONLINE with no active signals."""
        return sum(
            1 for s in self._states.values() if self._view_status(s) == QQStatus.ONLINE
        )

    @property
    def pending_offline_count(self) -> int:
        """Number of QQs whose runtime view is PENDING_OFFLINE."""
        return sum(1 for s in self._states.values() if self._view_status(s) is None)

    @property
    def offline_count(self) -> int:
        """Number of QQs whose confirmed status is OFFLINE."""
        return sum(
            1 for s in self._states.values() if s.confirmed_status == QQStatus.OFFLINE
        )

    def set_transition_callback(self, callback: TransitionCallback | None) -> None:
        """Register an async callback invoked on committed transitions.

        The callback receives ``(self_id, new_confirmed_status, kind)``.
        The coordinator **must** have persisted before calling this
        (via :meth:`fire_transition_event`).
        """
        self._on_transition = callback

    # ------------------------------------------------------------------
    # Slot reservation (called before WS upgrade)
    # ------------------------------------------------------------------

    async def try_reserve(self, self_id: int) -> bool:
        """Atomically reserve a slot for *self_id*.

        Returns ``True`` if the QQ is already registered or a new slot was
        successfully reserved.  Returns ``False`` when the 20-QQ limit would
        be exceeded.

        Callers **must** call :meth:`release_reservation` if the connection
        attempt fails after a successful reservation.
        """
        async with self._reservation_lock:
            if self_id in self._states:
                return True
            current = len(self._states) + len(self._reservations)
            if current >= MAX_QQ:
                return False
            self._reservations.add(self_id)
            return True

    def release_reservation(self, self_id: int) -> None:
        """Release an unused reservation (e.g. WS upgrade failed)."""
        self._reservations.discard(self_id)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def confirm_connection(self, self_id: int) -> int:
        """Confirm a newly established WebSocket connection for *self_id*.

        Called **after** the WS upgrade succeeds.  Returns the connection
        generation number that the caller must store for stale-event
        filtering.

        Does **not** fire the transition callback — the coordinator is
        responsible for persisting and then calling
        :meth:`fire_transition_event`.

        If the QQ was previously ``OFFLINE`` or is brand-new, the caller
        should:

        1. Call :meth:`get_confirm_transition` to check if a transition
           occurred.
        2. Persist snapshot.
        3. On success, call :meth:`fire_transition_event`.
        4. On failure, call :meth:`undo_confirm`.
        """
        self._reservations.discard(self_id)
        now_utc = self._wall()
        now_mono = self._monotonic()
        state = self._states.get(self_id)

        if state is None:
            # ---- First-ever connection ----
            state = _QQData(
                self_id=self_id,
                confirmed_status=QQStatus.ONLINE,
                generation=1,
                registered_at=now_utc,
                last_status_change=now_utc,
                connection_established_mono=now_mono,
            )
            self._states[self_id] = state
            self._cancel_timer(state)
            self._confirm_old_status[self_id] = QQStatus.ONLINE  # brand-new
            self._new_confirm_ids.add(self_id)
            return 1

        # ---- Existing QQ ----
        self._new_confirm_ids.discard(self_id)
        self._confirm_old_status[self_id] = state.confirmed_status
        state.generation += 1
        state.connection_established_mono = now_mono
        state.last_heartbeat_mono = 0.0  # reset heartbeat baseline
        old_status = state.confirmed_status

        # Clear disconnect and grace signals (new connection resolves these)
        state.signals.clear_disconnect()
        state.signals.clear_grace()
        state.signals.clear_heartbeat_miss()
        # Keep heartbeat_false signal if present (will be cleared by next heartbeat)

        self._cancel_timer(state)

        if old_status == QQStatus.OFFLINE:
            # Transition: OFFLINE → ONLINE (recovery)
            state.confirmed_status = QQStatus.ONLINE
            state.last_status_change = now_utc
            state.offline_since = None
            state.signals.clear_all()
        elif self._has_pending_signals(state):
            # Grace-period/signal recovery — silent, no callback
            state.signals.clear_grace()
            state.signals.clear_heartbeat_miss()

        return state.generation

    def get_confirm_transition(self, self_id: int) -> TransitionKind | None:
        """Return the transition kind from the last :meth:`confirm_connection`.

        Returns ``None`` for ONLINE replacement (no callback needed).
        The coordinator should call this after a successful persist to
        decide whether to fire a transition event.
        """
        old = self._confirm_old_status.get(self_id)
        if old is None:
            return None
        state = self._states.get(self_id)
        if state is None:
            return None

        # Brand-new QQ (never persisted before)
        if self_id in self._new_confirm_ids:
            return TransitionKind.FIRST_ONLINE
        # OFFLINE → ONLINE recovery
        if old == QQStatus.OFFLINE and state.confirmed_status == QQStatus.ONLINE:
            return TransitionKind.RECOVERED
        # ONLINE replacement — no transition
        return None

    def _get_confirm_new_status(self, self_id: int) -> QQStatus | None:
        """Return the confirmed status that the confirm set, or None."""
        state = self._states.get(self_id)
        if state is None:
            return None
        return state.confirmed_status

    def undo_confirm(self, self_id: int) -> None:
        """Roll back a :meth:`confirm_connection` call.

        Removes the QQ from state entirely if it was brand-new (and
        releases the reservation), or restores the previous confirmed
        status and runtime flags for an existing QQ.  Called when
        persistence of the transition fails.
        """
        old_status = self._confirm_old_status.pop(self_id, None)
        self._new_confirm_ids.discard(self_id)
        state = self._states.get(self_id)
        if state is None:
            return
        if state.generation == 1:
            # Brand-new QQ that failed to persist — remove completely
            self._states.pop(self_id, None)
            self.release_reservation(self_id)
        else:
            # Existing QQ — restore pre-confirm state
            state.generation -= 1
            state.connection_established_mono = None
            state.last_heartbeat_mono = 0.0
            state.signals.clear_all()
            if old_status is not None:
                state.confirmed_status = old_status
                if old_status == QQStatus.OFFLINE and state.offline_since is None:
                    state.offline_since = self._wall()

    def on_disconnect(self, self_id: int, generation: int) -> None:
        """Handle WebSocket disconnection.

        Events with a *generation* that does not match the current
        connection generation are silently discarded (stale events).
        """
        state = self._states.get(self_id)
        if state is None or state.generation != generation:
            return
        if state.confirmed_status != QQStatus.ONLINE:
            return

        if state.signals.disconnect_mono is None:
            state.signals.disconnect_mono = self._monotonic()
            self._schedule_timer(state)

    # ------------------------------------------------------------------
    # Heartbeat processing
    # ------------------------------------------------------------------

    def on_heartbeat(
        self, self_id: int, generation: int, online: bool
    ) -> TransitionKind | None:
        """Process a validated OneBot 11 heartbeat event.

        Only heartbeats that pass strict validation (dict, ``post_type ==
        "meta_event"``, ``meta_event_type == "heartbeat"``, ``status``
        dict, ``status.online`` is exact ``bool``, optional ``self_id``
        matches) should reach this method.

        Returns a :class:`TransitionKind` if a transition occurred
        (``RECOVERED``), or ``None``.  The coordinator must persist and
        then call :meth:`fire_transition_event` if a transition happened.
        On persist failure, call :meth:`rollback_heartbeat_recovery`.
        """
        state = self._states.get(self_id)
        if state is None or state.generation != generation:
            return None

        now_mono = self._monotonic()
        state.last_heartbeat_mono = now_mono
        state.last_heartbeat_at = self._wall()

        if online:
            # ---- Heartbeat says online ----
            was_pending = self._has_pending_signals(state)

            # Clear heartbeat-derived signals
            state.signals.clear_heartbeat_false()
            state.signals.clear_heartbeat_miss()
            state.signals.clear_grace()

            if state.confirmed_status == QQStatus.OFFLINE:
                # OFFLINE → ONLINE recovery via heartbeat
                self._save_pre_hb_recovery(state)
                state.confirmed_status = QQStatus.ONLINE
                state.last_status_change = self._wall()
                state.offline_since = None
                state.signals.clear_all()
                self._cancel_timer(state)
                return TransitionKind.RECOVERED
            elif was_pending and not self._has_pending_signals(state):
                # Was pending, now fully recovered — cancel timer
                self._cancel_timer(state)
        else:
            # ---- Heartbeat says offline ----
            if state.signals.heartbeat_false_mono is None:
                state.signals.heartbeat_false_mono = now_mono
                self._schedule_timer(state)

        return None

    # ------------------------------------------------------------------
    # Heartbeat timeout sweep
    # ------------------------------------------------------------------

    def sweep_heartbeat_timeouts(self) -> None:
        """Check all ONLINE QQs for heartbeat timeout.

        Called periodically (e.g. every 30 s) by the coordinator layer.
        A QQ whose last heartbeat (or connection establishment if none
        received) is older than ``offline_timeout`` will be flagged with
        a heartbeat-miss signal.
        """
        now_mono = self._monotonic()
        for state in list(self._states.values()):
            if state.confirmed_status != QQStatus.ONLINE:
                continue
            if state.signals.heartbeat_false_mono is not None:
                # Already tracking a false heartbeat — don't add miss
                continue

            baseline = state.last_heartbeat_mono
            if baseline == 0.0:
                baseline = state.connection_established_mono
            if baseline is None:
                continue
            if now_mono - baseline >= self._offline_timeout:
                if state.signals.heartbeat_miss_mono is None:
                    state.signals.heartbeat_miss_mono = baseline
                    self._schedule_timer(state)

    # ------------------------------------------------------------------
    # Deadline check & transition to OFFLINE
    # ------------------------------------------------------------------

    def check_timeouts(self) -> list[tuple[int, TransitionKind]]:
        """Check all QQs with active signals for deadline expiry.

        Returns a list of ``(self_id, kind)`` for each transition that
        occurred.  The coordinator **must** persist for each transition
        and then call :meth:`fire_transition_event`.  On persist failure,
        call :meth:`rollback_timeout`.

        The caller (coordinator) is responsible for persistence ordering.
        """
        now_mono = self._monotonic()
        transitions: list[tuple[int, TransitionKind]] = []
        for self_id, state in list(self._states.items()):
            if not self._has_pending_signals(state):
                continue
            earliest = state.signals.earliest_mono
            if earliest is None:
                continue
            if now_mono >= earliest + self._offline_timeout:
                # Save pre-transition state for possible rollback
                self._pre_timeout[self_id] = _PreTimeout(
                    confirmed_status=QQStatus.ONLINE,
                    last_status_change=state.last_status_change,
                    offline_since=state.offline_since,
                    signals=copy.deepcopy(state.signals),
                    had_timer=state.pending_timer is not None,
                )
                state.confirmed_status = QQStatus.OFFLINE
                state.last_status_change = self._wall()
                state.offline_since = state.last_status_change
                state.signals.clear_all()
                self._cancel_timer(state)
                kind = TransitionKind.OFFLINE_TIMEOUT
                transitions.append((self_id, kind))
        return transitions

    def rollback_timeout(self, self_id: int) -> None:
        """Roll back a timeout transition for *self_id*.

        Restores the pre-timeout confirmed status, wall-clock fields,
        and runtime failure signals so the next :meth:`check_timeouts`
        or heartbeat can retry persistence.
        """
        pre = self._pre_timeout.pop(self_id, None)
        if pre is None:
            return
        state = self._states.get(self_id)
        if state is None:
            return
        state.confirmed_status = pre.confirmed_status
        state.last_status_change = pre.last_status_change
        state.offline_since = pre.offline_since
        state.signals = pre.signals
        if pre.had_timer:
            self._schedule_timer(state)

    def rollback_heartbeat_recovery(self, self_id: int) -> None:
        """Roll back a heartbeat-recovery transition for *self_id*.

        Restores the pre-recovery OFFLINE status, wall-clock fields,
        and runtime failure signals so the next heartbeat can retry
        persistence.
        """
        pre = self._pre_hb_recovery.pop(self_id, None)
        if pre is None:
            return
        state = self._states.get(self_id)
        if state is None:
            return
        state.confirmed_status = pre.confirmed_status
        state.last_status_change = pre.last_status_change
        state.offline_since = pre.offline_since
        state.signals = pre.signals
        self._schedule_timer(state)

    def fire_transition_event(
        self, self_id: int, new_status: QQStatus, kind: TransitionKind
    ) -> None:
        """Fire the registered transition callback (fire-and-forget).

        The coordinator **must** have persisted before calling this.
        The callback is dispatched as a background task — exceptions
        do **not** roll back the already-persisted state.
        """
        self._fire_callback(self_id, new_status, kind)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def force_offline(self, self_id: int) -> bool:
        """Immediately transition a QQ to OFFLINE (used during shutdown).

        Returns ``True`` if the QQ existed and was moved to OFFLINE.
        Fires ``TransitionKind.SHUTDOWN`` callback (no persistence
        required — final snapshot is taken by the coordinator).
        """
        state = self._states.get(self_id)
        if state is None:
            return False
        state.confirmed_status = QQStatus.OFFLINE
        state.last_status_change = self._wall()
        state.offline_since = state.last_status_change
        state.signals.clear_all()
        self._cancel_timer(state)
        self._fire_callback(self_id, QQStatus.OFFLINE, TransitionKind.SHUTDOWN)
        return True

    # ------------------------------------------------------------------
    # Restart / persistence integration
    # ------------------------------------------------------------------

    def get_snapshot(self) -> dict[str, Any]:
        """Export business state for JSON persistence.

        Returns a **copy** of the data (safe from mutation).  Runtime-only
        fields (monotonic clocks, ``generation``, connection objects,
        timer refs, pending signals) are excluded.

        The ``status`` value is the **confirmed** status (``ONLINE`` or
        ``OFFLINE``).  The runtime-derived ``PENDING`` view is **not**
        stored; :meth:`get_view` computes it.
        """
        out: dict[str, Any] = {}
        for sid, s in self._states.items():
            out[str(sid)] = {
                "self_id": sid,
                "status": s.confirmed_status.value,
                "registered_at": s.registered_at,
                "last_status_change": s.last_status_change,
                "offline_since": s.offline_since,
            }
        return out

    def get_view(self, self_id: int) -> dict[str, Any] | None:
        """Return a safe public view for one QQ, or ``None`` if unknown.

        The ``status`` field reflects the runtime view:
        ``"online"``, ``"pending_offline"``, or ``"offline"``.
        """
        state = self._states.get(self_id)
        if state is None:
            return None
        view_status = self._view_status(state)
        return {
            "self_id": self_id,
            "status": view_status.value if view_status else "pending_offline",
            "registered_at": state.registered_at,
            "last_status_change": state.last_status_change,
            "offline_since": state.offline_since,
            "last_heartbeat_at": state.last_heartbeat_at,
        }

    def get_all_views(self) -> dict[str, Any]:
        """Return safe public views for all registered QQs.

        The ``status`` field is ``"online"``, ``"pending_offline"``, or
        ``"offline"`` — the runtime-derived snapshot view.
        """
        out: dict[str, Any] = {}
        for sid, s in self._states.items():
            view_status = self._view_status(s)
            out[str(sid)] = {
                "self_id": sid,
                "status": view_status.value if view_status else "pending_offline",
                "registered_at": s.registered_at,
                "last_status_change": s.last_status_change,
                "offline_since": s.offline_since,
                "last_heartbeat_at": s.last_heartbeat_at,
            }
        return out

    def load_snapshot(self, data: dict[str, Any]) -> None:
        """Restore state after plugin restart.

        Previously ONLINE QQs keep ``confirmed_status = ONLINE`` (they are
        **not** changed to PENDING).  The runtime view will derive
        ``PENDING`` from a grace-period signal set here.

        The persisted ``status`` is expected to be ``"online"`` or
        ``"offline"`` only.
        """
        now_mono = self._monotonic()
        self._states.clear()

        for key, item in data.items():
            sid = int(key)
            raw = item.get("status", "online")
            old_status = QQStatus(raw)

            s = _QQData(
                self_id=sid,
                confirmed_status=old_status,
                generation=0,
                registered_at=item.get("registered_at", self._wall()),
                last_status_change=item.get("last_status_change", self._wall()),
                offline_since=item.get("offline_since"),
            )

            if old_status == QQStatus.ONLINE:
                # Set grace-period signal for runtime PENDING view
                s.signals.grace_start_mono = now_mono
                self._schedule_timer(s)

            self._states[sid] = s

    def start_grace_timers(self) -> None:
        """Start offline timers for all loaded grace-period QQs.

        Call this after :meth:`load_snapshot` once an asyncio loop is
        available (e.g. in ``initialize()``).  Idempotent.
        """
        for state in self._states.values():
            if state.signals.grace_start_mono is not None:
                self._schedule_timer(state)

    def cancel_all_timers(self) -> None:
        """Cancel every pending offline timer.  Safe to call on stop."""
        for state in self._states.values():
            self._cancel_timer(state)

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------

    def _view_status(self, state: _QQData) -> QQStatus | None:
        """Return the runtime-derived status.

        Returns ``ONLINE``, ``OFFLINE``, or ``None`` for PENDING_OFFLINE.
        """
        if state.confirmed_status == QQStatus.OFFLINE:
            return QQStatus.OFFLINE
        # confirmed_status is ONLINE
        if self._has_pending_signals(state):
            return None  # represents PENDING_OFFLINE
        return QQStatus.ONLINE

    @staticmethod
    def _has_pending_signals(state: _QQData) -> bool:
        """Check whether a QQ has any unresolved failure signals."""
        return state.signals.has_any

    def _save_pre_hb_recovery(self, state: _QQData) -> None:
        """Save pre-transition state for heartbeat recovery rollback."""
        self._pre_hb_recovery[state.self_id] = _PreHbRecovery(
            confirmed_status=QQStatus.OFFLINE,
            last_status_change=state.last_status_change,
            offline_since=state.offline_since,
            signals=copy.deepcopy(state.signals),
        )

    # ---- Timer management ----

    def _schedule_timer(self, state: _QQData) -> None:
        """Schedule or reschedule the offline timer for *state*.

        The timer fires at ``earliest_mono + offline_timeout``.  If no
        signals are active, the timer is cancelled instead.

        The timer **does not** fire transition callbacks — it only
        signals the coordinator via pending signal updates.  The
        coordinator's periodic :meth:`check_timeouts` handles actual
        transitions.
        """
        if not state.signals.has_any:
            self._cancel_timer(state)
            return

        earliest = state.signals.earliest_mono
        if earliest is None:
            self._cancel_timer(state)
            return

        # Cancel existing timer first
        if state.pending_timer is not None and not state.pending_timer.done():
            state.pending_timer.cancel()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop (e.g. sync test context); skip timer.
            return

        # The timer itself no longer fires transitions; it only logs.
        # Transitions are handled by the coordinator via check_timeouts.
        state.pending_timer = loop.create_task(self._timer_task(state.self_id))

    async def _timer_task(self, sid: int) -> None:
        """Background timer task — logs expiry, no transition firing."""
        state = self._states.get(sid)
        if state is None:
            return
        if not state.signals.has_any:
            return
        earliest = state.signals.earliest_mono
        if earliest is None:
            return
        deadline = earliest + self._offline_timeout
        delay = max(0.0, deadline - self._monotonic())
        await asyncio.sleep(delay)
        logger.debug("Timer expired for self_id=%s (delegated to coordinator)", sid)

    def _cancel_timer(self, state: _QQData) -> None:
        task = state.pending_timer
        if task is not None and not task.done():
            task.cancel()
        state.pending_timer = None

    # ---- Callback dispatch ----

    def _fire_callback(
        self, self_id: int, new_status: QQStatus, kind: TransitionKind
    ) -> None:
        """Dispatch transition callback as a background task.

        The coordinator must have persisted before calling this.
        Exceptions from the callback do **not** roll back state.
        """
        cb = self._on_transition
        if cb is None:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _run() -> None:
            try:
                await cb(self_id, new_status, kind)
            except Exception:
                logger.exception(
                    "Transition callback error for self_id=%s kind=%s",
                    self_id,
                    kind.value,
                )

        loop.create_task(_run())
