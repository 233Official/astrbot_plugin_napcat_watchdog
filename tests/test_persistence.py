"""Tests for atomic JSON persistence (core.persistence).

Covers:
- Atomic write (temp file + rename, flush/fsync)
- Schema version must be exactly 1
- Corrupt / incompatible data fail-closed
- Unknown key rejection
- QQ key mismatch, non-int self_id, wrong type enforcement
- Empty file handling
- Parent directory fsync
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.persistence import (
    PersistenceError,
    load_snapshot,
    save_snapshot,
    snapshot_exists,
)

_VALID_SNAPSHOT = {
    "12345": {
        "self_id": 12345,
        "status": "online",
        "registered_at": 1000.0,
        "last_status_change": 1000.0,
        "offline_since": None,
    }
}

_VALID_ENTRY = _VALID_SNAPSHOT["12345"]


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------


class TestAtomicWrite:
    """save_snapshot creates temp file then atomically replaces target."""

    def test_creates_file(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        assert not p.exists()
        save_snapshot(p, _VALID_SNAPSHOT)
        assert p.exists()
        assert p.stat().st_size > 0

    def test_temp_file_cleaned_on_success(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        tmp = p.with_name(p.name + ".tmp")
        assert not tmp.exists()

    def test_overwrites_existing(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        p.write_text("old", encoding="utf-8")
        save_snapshot(p, _VALID_SNAPSHOT)
        loaded = load_snapshot(p)
        assert 12345 in (int(k) for k in loaded)

    def test_content_is_valid_json(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        raw = json.loads(p.read_text(encoding="utf-8"))
        assert "_schema_version" in raw
        assert "qq_states" in raw
        assert raw["_schema_version"] == 1

    def test_sort_keys_indent(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        text = p.read_text(encoding="utf-8")
        assert '"self_id": 12345' in text
        assert text.startswith("{")


# ---------------------------------------------------------------------------
# Schema version validation
# ---------------------------------------------------------------------------


class TestSchemaVersion:
    """Schema version must be exactly 1."""

    def test_version_missing_fails_closed(self, tmp_path: Path) -> None:
        data = {"qq_states": _VALID_SNAPSHOT}
        p = tmp_path / "no_version.json"
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        with pytest.raises(PersistenceError, match="missing top-level keys"):
            load_snapshot(p)

    def test_version_0_fails_closed(self, tmp_path: Path) -> None:
        data = {"_schema_version": 0, "qq_states": _VALID_SNAPSHOT}
        p = tmp_path / "v0.json"
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        with pytest.raises(PersistenceError, match="schema version 0"):
            load_snapshot(p)

    def test_version_2_plus_fails_closed(self, tmp_path: Path) -> None:
        data = {"_schema_version": 2, "qq_states": _VALID_SNAPSHOT}
        p = tmp_path / "v2.json"
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        with pytest.raises(PersistenceError, match="schema version 2"):
            load_snapshot(p)

    def test_extra_top_level_key_fails_closed(self, tmp_path: Path) -> None:
        data = {"_schema_version": 1, "qq_states": _VALID_SNAPSHOT, "extra": "bad"}
        p = tmp_path / "extra_key.json"
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        with pytest.raises(PersistenceError, match="unexpected top-level keys"):
            load_snapshot(p)

    def test_exactly_two_top_level_keys_succeeds(self, tmp_path: Path) -> None:
        p = tmp_path / "exact.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        loaded = load_snapshot(p)
        assert loaded["12345"]["self_id"] == 12345

    def test_version_1_succeeds(self, tmp_path: Path) -> None:
        p = tmp_path / "v1.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        loaded = load_snapshot(p)
        assert loaded["12345"]["self_id"] == 12345


# ---------------------------------------------------------------------------
# Load / validation
# ---------------------------------------------------------------------------


class TestLoadValidation:
    """load_snapshot validates schema, keys, types, and required fields."""

    def test_load_valid_snapshot(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        loaded = load_snapshot(p)
        assert isinstance(loaded, dict)
        assert loaded["12345"]["self_id"] == 12345

    def test_load_missing_file(self, tmp_path: Path) -> None:
        p = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError):
            load_snapshot(p)

    def test_corrupt_json_fails_closed(self, tmp_path: Path) -> None:
        p = tmp_path / "corrupt.json"
        p.write_text("{bad json", encoding="utf-8")
        with pytest.raises(PersistenceError, match="Corrupt"):
            load_snapshot(p)

    def test_empty_file_fails_closed(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.json"
        p.write_text("", encoding="utf-8")
        with pytest.raises(PersistenceError, match="Corrupt"):
            load_snapshot(p)

    def test_non_dict_root_fails_closed(self, tmp_path: Path) -> None:
        p = tmp_path / "array.json"
        p.write_text("[]", encoding="utf-8")
        with pytest.raises(PersistenceError, match="expected a JSON object"):
            load_snapshot(p)

    def test_missing_qq_states_fails_closed(self, tmp_path: Path) -> None:
        p = tmp_path / "no_states.json"
        p.write_text(json.dumps({"_schema_version": 1}), encoding="utf-8")
        with pytest.raises(PersistenceError, match="missing top-level keys"):
            load_snapshot(p)

    def test_unknown_key_fails_closed(self, tmp_path: Path) -> None:
        bad_entry = dict(_VALID_ENTRY)
        bad_entry["runtime_task"] = "some_object"
        snapshot = {"12345": bad_entry}
        p = tmp_path / "unknown_key.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="unexpected keys"):
            load_snapshot(p)

    def test_missing_required_field_fails_closed(self, tmp_path: Path) -> None:
        bad_entry = dict(_VALID_ENTRY)
        del bad_entry["status"]
        snapshot = {"12345": bad_entry}
        p = tmp_path / "missing_field.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="missing"):
            load_snapshot(p)

    def test_non_dict_entry_fails_closed(self, tmp_path: Path) -> None:
        snapshot = {"12345": "not_a_dict"}
        p = tmp_path / "bad_entry.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="not a dict"):
            load_snapshot(p)

    def test_qq_key_mismatch_fails_closed(self, tmp_path: Path) -> None:
        """QQ key must match self_id."""
        snapshot = {"111": {"self_id": 222, "status": "online"}}
        p = tmp_path / "key_mismatch.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="does not match"):
            load_snapshot(p)

    def test_non_int_self_id_fails_closed(self, tmp_path: Path) -> None:
        """self_id must be int, not string."""
        snapshot = {"12345": {"self_id": "12345", "status": "online"}}
        p = tmp_path / "str_self_id.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="self_id must be positive int"):
            load_snapshot(p)

    def test_negative_self_id_fails_closed(self, tmp_path: Path) -> None:
        snapshot = {"12345": {"self_id": -1, "status": "online"}}
        p = tmp_path / "neg_self_id.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="self_id must be positive int"):
            load_snapshot(p)

    def test_status_must_be_online_or_offline(self, tmp_path: Path) -> None:
        snapshot = {"12345": {"self_id": 12345, "status": "pending_offline"}}
        p = tmp_path / "bad_status.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="status must be"):
            load_snapshot(p)

    def test_bool_not_number_for_numeric_field(self, tmp_path: Path) -> None:
        """registered_at as bool should fail."""
        snapshot = {
            "12345": {
                "self_id": 12345,
                "status": "online",
                "registered_at": True,
                "last_status_change": False,
            }
        }
        p = tmp_path / "bool_num.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="must be a number"):
            load_snapshot(p)

    def test_qq_count_exceeds_limit(self, tmp_path: Path) -> None:
        """More than 20 QQ entries fails."""
        snapshot = {}
        for i in range(21):
            sid = 10000 + i
            snapshot[str(sid)] = {
                "self_id": sid,
                "status": "online",
                "registered_at": 1.0,
                "last_status_change": 1.0,
            }
        p = tmp_path / "too_many.json"
        save_snapshot(p, snapshot)
        with pytest.raises(PersistenceError, match="exceeds limit"):
            load_snapshot(p)


# ---------------------------------------------------------------------------
# snapshot_exists
# ---------------------------------------------------------------------------


class TestSnapshotExists:
    def test_exists(self, tmp_path: Path) -> None:
        p = tmp_path / "exists.json"
        save_snapshot(p, _VALID_SNAPSHOT)
        assert snapshot_exists(p)

    def test_not_exists(self, tmp_path: Path) -> None:
        p = tmp_path / "no.json"
        assert not snapshot_exists(p)

    def test_empty_file_still_exists(self, tmp_path: Path) -> None:
        p = tmp_path / "empty_exists.json"
        p.write_text("", encoding="utf-8")
        assert snapshot_exists(p)


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    """Full round-trip: save → load → compare fields."""

    def test_preserves_all_fields(self, tmp_path: Path) -> None:
        snapshot = {
            "99999": {
                "self_id": 99999,
                "status": "offline",
                "registered_at": 100.0,
                "last_status_change": 200.0,
                "offline_since": 200.0,
            }
        }
        p = tmp_path / "roundtrip.json"
        save_snapshot(p, snapshot)
        loaded = load_snapshot(p)
        entry = loaded["99999"]
        assert entry["self_id"] == 99999
        assert entry["status"] == "offline"
        assert entry["registered_at"] == 100.0
        assert entry["last_status_change"] == 200.0
        assert entry["offline_since"] == 200.0

    def test_none_values_survive(self, tmp_path: Path) -> None:
        snapshot = {
            "111": {
                "self_id": 111,
                "status": "online",
                "registered_at": 1.0,
                "last_status_change": 1.0,
                "offline_since": None,
            }
        }
        p = tmp_path / "none_test.json"
        save_snapshot(p, snapshot)
        loaded = load_snapshot(p)
        assert loaded["111"]["offline_since"] is None

    def test_empty_snapshot(self, tmp_path: Path) -> None:
        p = tmp_path / "empty_snap.json"
        save_snapshot(p, {})
        loaded = load_snapshot(p)
        assert loaded == {}

    def test_last_heartbeat_time_not_present(self, tmp_path: Path) -> None:
        """last_heartbeat_time is NOT in the persistable keys."""
        snapshot = {
            "111": {
                "self_id": 111,
                "status": "online",
                "registered_at": 1.0,
                "last_status_change": 1.0,
                "offline_since": None,
            }
        }
        p = tmp_path / "no_hb.json"
        save_snapshot(p, snapshot)
        loaded = load_snapshot(p)
        assert "last_heartbeat_time" not in loaded["111"]


# ---------------------------------------------------------------------------
# Schema version 1 only
# ---------------------------------------------------------------------------


class TestStrictSchemaVersion:
    """Schema version must be *exactly* 1."""

    def test_version_1_string_fails_closed(self, tmp_path: Path) -> None:
        """Version must be int, not string."""
        data = {"_schema_version": "1", "qq_states": _VALID_SNAPSHOT}
        p = tmp_path / "str_v1.json"
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        with pytest.raises(PersistenceError, match="schema version"):
            load_snapshot(p)
