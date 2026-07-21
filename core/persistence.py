"""Atomic JSON persistence for state machine snapshots.

Uses a temp-file + atomic-rename strategy to prevent partial writes.
On load, corrupt or schema-incompatible data triggers a fail-closed
response: the caller is expected to refuse startup rather than silently
reset state.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Schema version — must be *exactly* 1 for Issue #3.  Missing, 0, or
# future versions cause fail-closed.
_SCHEMA_VERSION = 1

# Allowed keys in a persisted QQ entry.  Runtime-only fields
# (generation, monotonic clocks, signals, timers, connection refs,
# last_heartbeat_*) are excluded.
_PERSISTENT_KEYS: frozenset[str] = frozenset(
    {
        "self_id",
        "status",  # "online" or "offline" only
        "registered_at",
        "last_status_change",
        "offline_since",
    }
)

_VALID_STATUSES: frozenset[str] = frozenset({"online", "offline"})


class PersistenceError(Exception):
    """Raised when persistence read/write fails or data is corrupt."""


# ---- Public API ----


def save_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    """Atomically write a state snapshot to *path*.

    Writes to a temporary file in the same directory first, then performs
    an atomic :func:`os.replace` (POSIX rename).  After the replace,
    ``fsync`` is called on the parent directory to ensure the new name
    is durable.

    Raises:
        PersistenceError: if the write or rename fails.
    """
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        data: dict[str, Any] = {
            "_schema_version": _SCHEMA_VERSION,
            "qq_states": snapshot,
        }
        _atomic_write(tmp_path, json.dumps(data, indent=2, sort_keys=True))
        os.replace(str(tmp_path), str(path))
        _fsync_parent(path)
    except OSError as e:
        _cleanup_tmp(tmp_path)
        msg = f"Failed to persist state to {path}: {e}"
        raise PersistenceError(msg) from e


def _fsync_parent(path: Path) -> None:
    """Call ``fsync`` on the parent directory for durable rename."""
    try:
        parent_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except OSError:
        # Best-effort: some platforms / filesystems may not support
        # directory fsync.
        pass


def load_snapshot(path: Path) -> dict[str, Any]:
    """Load and validate a state snapshot from *path*.

    Returns a snapshot dict compatible with
    :meth:`StateMachine.load_snapshot`, or raises
    :class:`PersistenceError` on parse failure, schema mismatch,
    or missing file.

    Raises:
        PersistenceError: when the file is missing, corrupt,
            schema-incompatible, or contains unexpected fields.
        FileNotFoundError: if *path* does not exist (caller may
            treat this as empty state).
    """
    if not path.exists():
        raise FileNotFoundError(str(path))

    try:
        raw = path.read_text(encoding="utf-8")
        data: dict[str, Any] = json.loads(raw)
    except (json.JSONDecodeError, OSError) as e:
        msg = f"Corrupt state file {path}: {e}"
        raise PersistenceError(msg) from e

    if not isinstance(data, dict):
        msg = f"State file {path}: expected a JSON object, got {type(data).__name__}"
        raise PersistenceError(msg)

    # Enforce exactly two top-level fields: _schema_version and qq_states
    allowed_top: frozenset[str] = frozenset({"_schema_version", "qq_states"})
    extra = set(data) - allowed_top
    missing = allowed_top - set(data)
    if extra:
        msg = f"State file {path}: unexpected top-level keys: {extra}"
        raise PersistenceError(msg)
    if missing:
        msg = f"State file {path}: missing top-level keys: {missing}"
        raise PersistenceError(msg)

    schema_ver = data.get("_schema_version", 0)
    if not isinstance(schema_ver, int) or schema_ver != _SCHEMA_VERSION:
        msg = (
            f"State file {path}: schema version {schema_ver} is not "
            f"supported (required: {_SCHEMA_VERSION})"
        )
        raise PersistenceError(msg)

    qq_states = data.get("qq_states")
    if not isinstance(qq_states, dict):
        msg = f"State file {path}: missing or invalid 'qq_states'"
        raise PersistenceError(msg)

    # Enforce maximum QQ count
    if len(qq_states) > 20:
        msg = f"State file {path}: QQ count {len(qq_states)} exceeds limit (max 20)"
        raise PersistenceError(msg)

    # Validate each entry
    for sid_key, entry in qq_states.items():
        _validate_entry(path, sid_key, entry)

    return qq_states


def _validate_entry(path: Path, sid_key: str, entry: Any) -> None:
    """Validate a single QQ entry.  Raises PersistenceError on failure."""
    if not isinstance(entry, dict):
        msg = f"State file {path}: entry '{sid_key}' is not a dict"
        raise PersistenceError(msg)

    # Reject unknown keys
    unknown = set(entry) - _PERSISTENT_KEYS
    if unknown:
        msg = f"State file {path}: entry '{sid_key}' has unexpected keys: {unknown}"
        raise PersistenceError(msg)

    # Required fields
    for req in ("self_id", "status"):
        if req not in entry:
            msg = f"State file {path}: entry '{sid_key}' missing '{req}'"
            raise PersistenceError(msg)

    # self_id must be a positive integer
    sid_val = entry["self_id"]
    if not isinstance(sid_val, int) or sid_val <= 0:
        msg = (
            f"State file {path}: entry '{sid_key}' self_id must be "
            f"positive int, got {type(sid_val).__name__}"
        )
        raise PersistenceError(msg)

    # QQ key must match self_id
    if str(sid_val) != sid_key:
        msg = (
            f"State file {path}: entry key '{sid_key}' does not match self_id {sid_val}"
        )
        raise PersistenceError(msg)

    # Status must be a valid ONLINE/OFFLINE string
    status_val = entry["status"]
    if not isinstance(status_val, str) or status_val not in _VALID_STATUSES:
        msg = (
            f"State file {path}: entry '{sid_key}' status must be "
            f"'online' or 'offline', got {status_val!r}"
        )
        raise PersistenceError(msg)

    # Numeric fields must be numbers (but not bool)
    for num_field in ("registered_at", "last_status_change"):
        val = entry.get(num_field)
        if val is not None:
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                msg = (
                    f"State file {path}: entry '{sid_key}' {num_field} must be "
                    f"a number, got {type(val).__name__}"
                )
                raise PersistenceError(msg)

    # offline_since must be None or a number
    off_since = entry.get("offline_since")
    if off_since is not None:
        if isinstance(off_since, bool) or not isinstance(off_since, (int, float)):
            msg = (
                f"State file {path}: entry '{sid_key}' offline_since must be "
                f"a number or null, got {type(off_since).__name__}"
            )
            raise PersistenceError(msg)


def snapshot_exists(path: Path) -> bool:
    """Return ``True`` if a persisted snapshot exists at *path*."""
    return path.is_file()


# ---- Internal ----


def _atomic_write(path: Path, content: str) -> None:
    """Write *content* to *path* atomically using flush and fsync."""
    fd = -1
    try:
        fd = os.open(
            str(path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_SYNC,
            0o644,
        )
        os.write(fd, content.encode("utf-8"))
        os.fsync(fd)
    finally:
        if fd >= 0:
            os.close(fd)


def _cleanup_tmp(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
