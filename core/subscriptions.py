"""Independent subscription persistence for Issue #4 notification targets.

Stores subscription records keyed by unified message origin (UMO)
in a separate JSON file from the state machine snapshot. Uses the
same atomic-write strategy as core/persistence.py.

Schema v1 format::

    {
        "_schema_version": 1,
        "subscriptions": {
            "group_123|user_456": {
                "umo": "group_123|user_456",
                "owner_id": "QQ_12345",
                "created_at": 1000.0,
                "updated_at": 1000.0
            }
        }
    }
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1

_ALLOWED_TOP_KEYS: frozenset[str] = frozenset({"_schema_version", "subscriptions"})

_ALLOWED_SUB_KEYS: frozenset[str] = frozenset(
    {"umo", "owner_id", "created_at", "updated_at"}
)


class SubscriptionError(Exception):
    """Raised when subscription persistence or validation fails."""


class SubscriptionRecord:
    """A single subscription record.

    Attributes
    ----------
    umo:
        Unified message origin identifying the target group/channel.
    owner_id:
        AstrBot user ID of the admin who last subscribed.
    created_at:
        UTC epoch of first subscription.
    updated_at:
        UTC epoch of most recent update.
    """

    __slots__ = ("umo", "owner_id", "created_at", "updated_at")

    def __init__(
        self,
        umo: str,
        owner_id: str,
        created_at: float,
        updated_at: float,
    ) -> None:
        self.umo = umo
        self.owner_id = owner_id
        self.created_at = created_at
        self.updated_at = updated_at

    def to_dict(self) -> dict[str, Any]:
        """Export as a plain dict (safe for JSON serialisation)."""
        return {
            "umo": self.umo,
            "owner_id": self.owner_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SubscriptionRecord:
        """Reconstruct from a validated dict."""
        return cls(
            umo=data["umo"],
            owner_id=data["owner_id"],
            created_at=data["created_at"],
            updated_at=data["updated_at"],
        )


class SubscriptionStore:
    """In-memory subscription store with atomic JSON persistence.

    All mutation operations are safe for single-threaded use.  The
    caller is responsible for calling :meth:`save` to persist and
    rolling back on failure.

    Parameters
    ----------
    wall_clock:
        Injectable wall clock (UTC epoch seconds).  Defaults to
        :func:`time.time`.  Injected for test determinism.
    """

    def __init__(self, wall_clock: Callable[[], float] = time.time) -> None:
        self._wall = wall_clock
        self._records: dict[str, SubscriptionRecord] = {}

    # ---- Properties ----

    @property
    def count(self) -> int:
        """Number of currently tracked subscriptions."""
        return len(self._records)

    # ---- Query ----

    def get(self, umo: str) -> SubscriptionRecord | None:
        """Return the record for *umo*, or ``None``."""
        return self._records.get(umo)

    def all(self) -> list[SubscriptionRecord]:
        """Return a shallow-copied list of all records."""
        return list(self._records.values())

    def get_snapshot(self) -> dict[str, dict[str, Any]]:
        """Return a deep-copied dict of all subscription records.

        The returned dict is safe from external mutation — modifying
        it has no effect on store state.
        """
        return {umo: rec.to_dict() for umo, rec in self._records.items()}

    # ---- Mutation ----

    def subscribe(self, umo: str, owner_id: str) -> tuple[SubscriptionRecord, bool]:
        """Register or refresh a subscription for *umo*.

        If *umo* already exists, only ``owner_id`` and ``updated_at``
        are updated; ``created_at`` is preserved.

        Parameters
        ----------
        umo:
            Unified message origin.  Must be a non-empty string after
            stripping leading/trailing whitespace.
        owner_id:
            Owner identifier.  Must be a non-empty string after
            stripping leading/trailing whitespace.

        Returns
        -------
        tuple[SubscriptionRecord, bool]
            ``(record, was_created)`` where ``was_created`` is ``True``
            for a brand-new subscription.

        Raises
        ------
        SubscriptionError
            If *umo* or *owner_id* is empty/blank.
        """
        umo = umo.strip()
        owner_id = owner_id.strip()
        if not umo:
            raise SubscriptionError("UMO must not be empty")
        if not owner_id:
            raise SubscriptionError("Owner ID must not be empty")
        now = self._wall()
        existing = self._records.get(umo)
        if existing is None:
            rec = SubscriptionRecord(
                umo=umo,
                owner_id=owner_id,
                created_at=now,
                updated_at=now,
            )
            self._records[umo] = rec
            return rec, True
        existing.owner_id = owner_id
        existing.updated_at = now
        return existing, False

    def unsubscribe(self, umo: str) -> bool:
        """Remove the subscription for *umo*.

        Returns
        -------
        bool
            ``True`` if the subscription existed and was removed,
            ``False`` if it did not exist.
        """
        if umo in self._records:
            del self._records[umo]
            return True
        return False

    # ---- Bulk load / import ----

    def load_dict(self, data: dict[str, dict[str, Any]]) -> None:
        """Replace all records from a validated dict.

        The dict must have the same structure as returned by
        :meth:`get_snapshot` — ``{umo: {fields...}}``.
        """
        self._records.clear()
        for umo, entry in data.items():
            self._records[umo] = SubscriptionRecord.from_dict(entry)

    # ---- Persistence ----

    def save(self, path: Path) -> None:
        """Atomically write the subscription store to *path*.

        Validates the full in-memory snapshot before writing.  If any
        record is invalid (blank ``umo``/``owner_id``, key mismatch,
        bad timestamps, extra fields) the file is **not** replaced and
        a :class:`SubscriptionError` is raised.

        Uses a temporary file in the same directory, flush/fsync,
        atomic ``os.replace``, and best-effort parent-directory fsync.

        Raises
        ------
        SubscriptionError
            If in-memory validation fails, or the write / rename fails.
        """
        snapshot = self.get_snapshot()
        _validate_snapshot(snapshot)

        tmp_path = path.with_name(path.name + ".tmp")
        try:
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            data: dict[str, Any] = {
                "_schema_version": _SCHEMA_VERSION,
                "subscriptions": snapshot,
            }
            _atomic_write(tmp_path, json.dumps(data, indent=2, sort_keys=True))
            os.replace(str(tmp_path), str(path))
            _fsync_parent(path)
        except OSError as e:
            _cleanup_tmp(tmp_path)
            msg = f"Failed to persist subscriptions to {path}: {e}"
            raise SubscriptionError(msg) from e


# ---- Public load function ----


def load_subscriptions(path: Path) -> dict[str, dict[str, Any]]:
    """Load and validate subscription data from *path*.

    Returns a dict of ``{umo: record_dict}`` compatible with
    :meth:`SubscriptionStore.load_dict`, or raises
    :class:`SubscriptionError` on corruption or schema mismatch.

    Raises
    ------
    SubscriptionError
        When the file is corrupt, schema-incompatible, or contains
        invalid entries.
    FileNotFoundError
        If *path* does not exist (caller treats this as empty state).
    """
    if not path.exists():
        raise FileNotFoundError(str(path))

    try:
        raw = path.read_text(encoding="utf-8")
        data: dict[str, Any] = json.loads(raw)
    except (json.JSONDecodeError, OSError) as e:
        msg = f"Corrupt subscription file {path}: {e}"
        raise SubscriptionError(msg) from e

    if not isinstance(data, dict):
        msg = (
            f"Subscription file {path}: expected a JSON object, "
            f"got {type(data).__name__}"
        )
        raise SubscriptionError(msg)

    # Enforce exactly two top-level fields
    extra = set(data) - _ALLOWED_TOP_KEYS
    if extra:
        msg = f"Subscription file {path}: unexpected top-level keys: {extra}"
        raise SubscriptionError(msg)
    missing = _ALLOWED_TOP_KEYS - set(data)
    if missing:
        msg = f"Subscription file {path}: missing top-level keys: {missing}"
        raise SubscriptionError(msg)

    schema_ver = data.get("_schema_version", 0)
    if not isinstance(schema_ver, int) or schema_ver != _SCHEMA_VERSION:
        msg = (
            f"Subscription file {path}: schema version {schema_ver} is not "
            f"supported (required: {_SCHEMA_VERSION})"
        )
        raise SubscriptionError(msg)

    subscriptions = data.get("subscriptions")
    if not isinstance(subscriptions, dict):
        msg = f"Subscription file {path}: missing or invalid 'subscriptions'"
        raise SubscriptionError(msg)

    for umo, entry in subscriptions.items():
        _validate_entry(path, umo, entry)

    return subscriptions


def subscription_exists(path: Path) -> bool:
    """Return ``True`` if a subscription file exists at *path*."""
    return path.is_file()


# ---- Internal helpers ----


_SENSITIVE_TAIL = 6
"""Number of trailing characters to leave unmasked in error messages."""


def _mask(val: str, tail: int = _SENSITIVE_TAIL) -> str:
    """Short mask helper for error messages — never returns the raw value."""
    if not val:
        return ""
    if len(val) <= tail:
        return "*" * len(val)
    return "*" * (len(val) - tail) + val[-tail:]


def _validate_snapshot(snapshot: dict[str, dict[str, Any]]) -> None:
    """Validate in-memory subscription snapshot before serialisation.

    Raises :class:`SubscriptionError` on the first invalid record.
    """
    for umo, entry in snapshot.items():
        if not isinstance(entry, dict):
            raise SubscriptionError(
                f"Invalid subscription snapshot: entry '{_mask(umo)}' is not a dict"
            )
        unknown = set(entry) - _ALLOWED_SUB_KEYS
        if unknown:
            raise SubscriptionError(
                f"Invalid subscription snapshot: entry '{_mask(umo)}' "
                f"has unexpected keys: {unknown}"
            )
        for req in ("umo", "owner_id"):
            val = entry.get(req)
            if not isinstance(val, str) or not val:
                raise SubscriptionError(
                    f"Invalid subscription snapshot: entry '{_mask(umo)}' "
                    f"'{req}' must be a non-empty string"
                )
        if entry.get("umo") != umo:
            raise SubscriptionError(
                f"Invalid subscription snapshot: entry key '{_mask(umo)}' "
                f"does not match umo field"
            )
        for ts_field in ("created_at", "updated_at"):
            val = entry.get(ts_field)
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                raise SubscriptionError(
                    f"Invalid subscription snapshot: entry '{_mask(umo)}' "
                    f"{ts_field} must be a number, got {type(val).__name__}"
                )


def _validate_entry(path: Path, umo: str, entry: Any) -> None:
    """Validate a single subscription entry.  Raises SubscriptionError."""
    if not isinstance(entry, dict):
        msg = f"Subscription file {path}: entry '{_mask(umo)}' is not a dict"
        raise SubscriptionError(msg)

    unknown = set(entry) - _ALLOWED_SUB_KEYS
    if unknown:
        msg = (
            f"Subscription file {path}: entry '{_mask(umo)}' has "
            f"unexpected keys: {unknown}"
        )
        raise SubscriptionError(msg)

    # umo and owner_id must be non-empty strings
    for req in ("umo", "owner_id"):
        val = entry.get(req)
        if not isinstance(val, str) or not val:
            msg = (
                f"Subscription file {path}: entry '{_mask(umo)}' '{req}' "
                f"must be a non-empty string"
            )
            raise SubscriptionError(msg)

    if entry.get("umo") != umo:
        msg = (
            f"Subscription file {path}: entry key '{_mask(umo)}' "
            f"does not match umo field"
        )
        raise SubscriptionError(msg)

    # Timestamps must be numbers (not bool)
    for ts_field in ("created_at", "updated_at"):
        val = entry.get(ts_field)
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            msg = (
                f"Subscription file {path}: entry '{_mask(umo)}' {ts_field} "
                f"must be a number, got {type(val).__name__}"
            )
            raise SubscriptionError(msg)


def _atomic_write(path: Path, content: str) -> None:
    """Write *content* to *path* atomically using flush and fsync."""
    fd = -1
    try:
        fd = os.open(
            str(path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_SYNC,
            0o600,
        )
        os.write(fd, content.encode("utf-8"))
        os.fsync(fd)
    finally:
        if fd >= 0:
            os.close(fd)


def _fsync_parent(path: Path) -> None:
    """Best-effort fsync on the parent directory for durable rename."""
    try:
        parent_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except OSError:
        pass


def _cleanup_tmp(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
