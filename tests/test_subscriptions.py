"""Comprehensive tests for core.subscriptions module (Issue #4).

Covers:
- SubscriptionStore CRUD with injectable wall clock
- Idempotent subscribe (update in-place, preserve created_at)
- Unsubscribe existence / non-existence
- Snapshot returns copy (no internal reference leak)
- JSON round-trip with schema validation
- Corrupt / incompatible / extra-key files fail closed
- Save failure rollback via load_dict
- Atomic write (temp file + flush/fsync + os.replace)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.subscriptions import (
    SubscriptionError,
    SubscriptionRecord,
    SubscriptionStore,
    load_subscriptions,
    subscription_exists,
)

# ---------------------------------------------------------------------------
# Fake clock
# ---------------------------------------------------------------------------


class FakeWall:
    """Injectable wall clock with deterministic time."""

    _time: float = 1000000.0

    def __call__(self) -> float:
        return self._time

    def set(self, t: float) -> None:
        self._time = t


def make_store(wall: FakeWall | None = None) -> SubscriptionStore:
    wall = wall or FakeWall()
    return SubscriptionStore(wall_clock=wall)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_new_store_is_empty(self) -> None:
        store = make_store()
        assert store.count == 0
        assert store.all() == []
        assert store.get_snapshot() == {}

    def test_get_returns_none_for_missing(self) -> None:
        store = make_store()
        assert store.get("non_existent_umo") is None


# ---------------------------------------------------------------------------
# Subscribe
# ---------------------------------------------------------------------------


class TestSubscribe:
    def test_first_subscribe_creates_record(self) -> None:
        wall = FakeWall()
        store = make_store(wall=wall)
        wall.set(500.0)

        rec, created = store.subscribe("umo_test_1", "owner_1")
        assert created is True
        assert rec.umo == "umo_test_1"
        assert rec.owner_id == "owner_1"
        assert rec.created_at == 500.0
        assert rec.updated_at == 500.0
        assert store.count == 1

    def test_repeat_subscribe_updates_owner_and_time(self) -> None:
        wall = FakeWall()
        store = make_store(wall=wall)
        wall.set(100.0)
        store.subscribe("umo_test_1", "owner_1")

        wall.set(200.0)
        rec, created = store.subscribe("umo_test_1", "owner_2")
        assert created is False, "Should indicate record already existed"
        assert rec.owner_id == "owner_2"
        assert rec.created_at == 100.0, "created_at must be preserved"
        assert rec.updated_at == 200.0, "updated_at must be refreshed"
        assert store.count == 1, "Record count must not increase"

    def test_subscribe_preserves_created_at_on_repeat(self) -> None:
        """Repeat subscribe must NOT change created_at."""
        wall = FakeWall()
        store = make_store(wall=wall)
        wall.set(50.0)
        store.subscribe("umo_1", "owner_a")

        for t in (100.0, 200.0, 300.0):
            wall.set(t)
            store.subscribe("umo_1", "owner_b")

        rec = store.get("umo_1")
        assert rec is not None
        assert rec.created_at == 50.0

    def test_multiple_distinct_subscriptions(self) -> None:
        store = make_store()
        store.subscribe("umo_a", "owner_a")
        store.subscribe("umo_b", "owner_b")
        store.subscribe("umo_c", "owner_c")
        assert store.count == 3

    def test_subscribe_returns_record_with_correct_fields(self) -> None:
        store = make_store()
        rec, _ = store.subscribe("test_umo", "test_owner")
        assert isinstance(rec, SubscriptionRecord)
        assert rec.umo == "test_umo"
        assert rec.owner_id == "test_owner"
        assert isinstance(rec.created_at, float)
        assert isinstance(rec.updated_at, float)

    def test_subscribe_raises_on_empty_umo(self) -> None:
        store = make_store()
        with pytest.raises(SubscriptionError, match="UMO must not be empty"):
            store.subscribe("", "owner_1")

    def test_subscribe_raises_on_blank_umo(self) -> None:
        store = make_store()
        with pytest.raises(SubscriptionError, match="UMO must not be empty"):
            store.subscribe("   ", "owner_1")

    def test_subscribe_raises_on_empty_owner_id(self) -> None:
        store = make_store()
        with pytest.raises(SubscriptionError, match="Owner ID must not be empty"):
            store.subscribe("umo_1", "")

    def test_subscribe_raises_on_blank_owner_id(self) -> None:
        store = make_store()
        with pytest.raises(SubscriptionError, match="Owner ID must not be empty"):
            store.subscribe("umo_1", "  ")

    def test_subscribe_strips_umo(self) -> None:
        store = make_store()
        rec, created = store.subscribe("  umo_strip  ", "owner_1")
        assert created is True
        assert rec.umo == "umo_strip"

    def test_subscribe_strips_owner_id(self) -> None:
        store = make_store()
        rec, created = store.subscribe("umo_1", "  owner_strip  ")
        assert created is True
        assert rec.owner_id == "owner_strip"


# ---------------------------------------------------------------------------
# Unsubscribe
# ---------------------------------------------------------------------------


class TestUnsubscribe:
    def test_unsubscribe_existing(self) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        assert store.count == 1

        result = store.unsubscribe("umo_1")
        assert result is True
        assert store.count == 0
        assert store.get("umo_1") is None

    def test_unsubscribe_nonexistent_is_idempotent(self) -> None:
        store = make_store()
        result = store.unsubscribe("non_existent")
        assert result is False
        assert store.count == 0

    def test_unsubscribe_then_resubscribe(self) -> None:
        wall = FakeWall()
        store = make_store(wall=wall)
        wall.set(100.0)
        rec1, _ = store.subscribe("umo_1", "owner_1")
        store.unsubscribe("umo_1")
        wall.set(200.0)
        rec2, created = store.subscribe("umo_1", "owner_2")
        assert created is True
        assert rec2.created_at == 200.0  # fresh created_at
        assert store.count == 1


# ---------------------------------------------------------------------------
# Snapshot isolation
# ---------------------------------------------------------------------------


class TestSnapshotIsolation:
    def test_get_snapshot_returns_copy(self) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        snap = store.get_snapshot()
        snap["umo_1"]["owner_id"] = "hacked"
        # Original must be unchanged
        assert store.get("umo_1").owner_id == "owner_1"

    def test_load_dict_replaces_all(self) -> None:
        store = make_store()
        store.subscribe("umo_a", "owner_a")
        data = {
            "umo_b": {
                "umo": "umo_b",
                "owner_id": "owner_b",
                "created_at": 1.0,
                "updated_at": 2.0,
            }
        }
        store.load_dict(data)
        assert store.count == 1
        assert store.get("umo_b") is not None
        assert store.get("umo_a") is None


# ---------------------------------------------------------------------------
# JSON round-trip with schema validation
# ---------------------------------------------------------------------------


class TestPersistenceRoundTrip:
    def test_save_and_load(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("group_123|user_456", "admin_qq")
        p = tmp_path / "subs.json"
        store.save(p)

        loaded = load_subscriptions(p)
        assert "group_123|user_456" in loaded
        entry = loaded["group_123|user_456"]
        assert entry["umo"] == "group_123|user_456"
        assert entry["owner_id"] == "admin_qq"
        assert isinstance(entry["created_at"], float)
        assert isinstance(entry["updated_at"], float)

    def test_load_into_store_restores_state(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        store.subscribe("umo_2", "owner_2")
        p = tmp_path / "subs.json"
        store.save(p)

        store2 = make_store()
        data = load_subscriptions(p)
        store2.load_dict(data)
        assert store2.count == 2
        assert store2.get("umo_1").owner_id == "owner_1"
        assert store2.get("umo_2").owner_id == "owner_2"

    def test_empty_subscriptions(self, tmp_path: Path) -> None:
        store = make_store()
        p = tmp_path / "empty.json"
        store.save(p)
        data = load_subscriptions(p)
        assert data == {}

    def test_temp_file_cleaned_on_success(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        p = tmp_path / "subs.json"
        store.save(p)
        tmp = p.with_name(p.name + ".tmp")
        assert not tmp.exists()

    def test_schema_version_in_file(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        p = tmp_path / "subs.json"
        store.save(p)
        raw = json.loads(p.read_text(encoding="utf-8"))
        assert raw["_schema_version"] == 1

    def test_sort_keys_indent(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        p = tmp_path / "subs.json"
        store.save(p)
        text = p.read_text(encoding="utf-8")
        assert text.startswith("{")
        assert '"owner_id"' in text


# ---------------------------------------------------------------------------
# Schema validation — fail closed
# ---------------------------------------------------------------------------


class TestLoadValidation:
    def test_missing_file(self, tmp_path: Path) -> None:
        p = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError):
            load_subscriptions(p)

    def test_corrupt_json(self, tmp_path: Path) -> None:
        p = tmp_path / "corrupt.json"
        p.write_text("{bad json", encoding="utf-8")
        with pytest.raises(SubscriptionError, match="Corrupt"):
            load_subscriptions(p)

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.json"
        p.write_text("", encoding="utf-8")
        with pytest.raises(SubscriptionError, match="Corrupt"):
            load_subscriptions(p)

    def test_non_dict_root(self, tmp_path: Path) -> None:
        p = tmp_path / "array.json"
        p.write_text("[]", encoding="utf-8")
        with pytest.raises(SubscriptionError, match="expected a JSON object"):
            load_subscriptions(p)

    def test_extra_top_level_key(self, tmp_path: Path) -> None:
        data = {"_schema_version": 1, "subscriptions": {}, "extra": "bad"}
        p = tmp_path / "extra.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="unexpected top-level keys"):
            load_subscriptions(p)

    def test_missing_top_level_key(self, tmp_path: Path) -> None:
        data = {"_schema_version": 1}
        p = tmp_path / "missing_subs.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="missing top-level keys"):
            load_subscriptions(p)

    def test_wrong_schema_version(self, tmp_path: Path) -> None:
        data = {"_schema_version": 2, "subscriptions": {}}
        p = tmp_path / "v2.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="schema version 2"):
            load_subscriptions(p)

    def test_version_0_fails(self, tmp_path: Path) -> None:
        data = {"_schema_version": 0, "subscriptions": {}}
        p = tmp_path / "v0.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="schema version 0"):
            load_subscriptions(p)

    def test_version_string_fails(self, tmp_path: Path) -> None:
        data = {"_schema_version": "1", "subscriptions": {}}
        p = tmp_path / "str_v.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="schema version"):
            load_subscriptions(p)

    def test_subscriptions_not_dict(self, tmp_path: Path) -> None:
        data = {"_schema_version": 1, "subscriptions": "not_a_dict"}
        p = tmp_path / "bad_subs.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="invalid 'subscriptions'"):
            load_subscriptions(p)

    def test_extra_field_in_entry(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        p = tmp_path / "subs.json"
        store.save(p)
        # Manually add extra field
        raw = json.loads(p.read_text(encoding="utf-8"))
        raw["subscriptions"]["umo_1"]["extra_field"] = "bad"
        p.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="unexpected keys"):
            load_subscriptions(p)

    def test_missing_umo_field(self, tmp_path: Path) -> None:
        data = {
            "_schema_version": 1,
            "subscriptions": {
                "umo_1": {"owner_id": "o", "created_at": 1.0, "updated_at": 2.0}
            },
        }
        p = tmp_path / "no_umo.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="must be a non-empty string"):
            load_subscriptions(p)

    def test_umo_key_mismatch(self, tmp_path: Path) -> None:
        data = {
            "_schema_version": 1,
            "subscriptions": {
                "key_a": {
                    "umo": "key_b",
                    "owner_id": "o",
                    "created_at": 1.0,
                    "updated_at": 2.0,
                }
            },
        }
        p = tmp_path / "mismatch.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="does not match umo"):
            load_subscriptions(p)

    def test_bool_as_timestamp(self, tmp_path: Path) -> None:
        data = {
            "_schema_version": 1,
            "subscriptions": {
                "umo_1": {
                    "umo": "umo_1",
                    "owner_id": "o",
                    "created_at": True,
                    "updated_at": False,
                }
            },
        }
        p = tmp_path / "bool_ts.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(SubscriptionError, match="must be a number"):
            load_subscriptions(p)


# ---------------------------------------------------------------------------
# subscription_exists
# ---------------------------------------------------------------------------


class TestSubscriptionExists:
    def test_exists(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        p = tmp_path / "subs.json"
        store.save(p)
        assert subscription_exists(p)

    def test_not_exists(self, tmp_path: Path) -> None:
        p = tmp_path / "no.json"
        assert not subscription_exists(p)


# ---------------------------------------------------------------------------
# Rollback via load_dict
# ---------------------------------------------------------------------------


class TestRollback:
    def test_load_dict_restores_previous_state(self) -> None:
        store = make_store()
        store.subscribe("umo_a", "owner_a")
        store.subscribe("umo_b", "owner_b")

        before = store.get_snapshot()
        # Mutate
        store.subscribe("umo_c", "owner_c")
        assert store.count == 3
        # Rollback
        store.load_dict(before)
        assert store.count == 2
        assert store.get("umo_c") is None

    def test_rollback_from_empty(self) -> None:
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        before = store.get_snapshot()
        store.unsubscribe("umo_1")
        assert store.count == 0
        store.load_dict(before)
        assert store.count == 1


# ---------------------------------------------------------------------------
# SubscriptionRecord
# ---------------------------------------------------------------------------


class TestSubscriptionRecord:
    def test_to_dict_round_trip(self) -> None:
        rec = SubscriptionRecord(umo="u", owner_id="o", created_at=1.0, updated_at=2.0)
        d = rec.to_dict()
        rec2 = SubscriptionRecord.from_dict(d)
        assert rec2.umo == "u"
        assert rec2.owner_id == "o"
        assert rec2.created_at == 1.0
        assert rec2.updated_at == 2.0

    def test_slots(self) -> None:
        rec = SubscriptionRecord("u", "o", 1.0, 2.0)
        with pytest.raises(AttributeError):
            rec.nonexistent = 1  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Save pre-validation — reject invalid in-memory state
# ---------------------------------------------------------------------------


class TestSavePreValidation:
    """save() must validate in-memory snapshot before touching the file."""

    def test_save_rejects_blank_umo_entry(self, tmp_path: Path) -> None:
        store = make_store()
        store._records[""] = SubscriptionRecord("", "o", 1.0, 2.0)
        p = tmp_path / "subs.json"
        with pytest.raises(SubscriptionError, match="non-empty string"):
            store.save(p)
        assert not p.exists(), "File must not be created"

    def test_save_rejects_blank_owner_id(self, tmp_path: Path) -> None:
        store = make_store()
        store._records["umo_1"] = SubscriptionRecord("umo_1", "", 1.0, 2.0)
        p = tmp_path / "subs.json"
        with pytest.raises(SubscriptionError, match="non-empty string"):
            store.save(p)
        assert not p.exists()

    def test_save_rejects_key_mismatch(self, tmp_path: Path) -> None:
        store = make_store()
        store._records["key_a"] = SubscriptionRecord("key_b", "o", 1.0, 2.0)
        p = tmp_path / "subs.json"
        with pytest.raises(SubscriptionError, match="does not match umo"):
            store.save(p)
        assert not p.exists()

    def test_save_rejects_bool_timestamp(self, tmp_path: Path) -> None:
        store = make_store()
        store._records["umo_1"] = SubscriptionRecord("umo_1", "o", True, False)  # type: ignore[arg-type]
        p = tmp_path / "subs.json"
        with pytest.raises(SubscriptionError, match="must be a number"):
            store.save(p)
        assert not p.exists()

    def test_save_rejects_extra_keys(self, tmp_path: Path) -> None:
        store = make_store()

        orig = SubscriptionRecord("umo_1", "o", 1.0, 2.0)
        store._records["umo_1"] = orig
        # Manually inject extra key via snapshot
        store._records["umo_1"].umo = orig.umo  # reset
        snap = store.get_snapshot()
        snap["umo_1"]["extra_field"] = "bad"
        # Override get_snapshot temporarily
        orig_snap = store.get_snapshot
        store.get_snapshot = lambda: snap  # type: ignore[method-assign]
        p = tmp_path / "subs.json"
        with pytest.raises(SubscriptionError, match="unexpected keys"):
            store.save(p)
        assert not p.exists()
        store.get_snapshot = orig_snap

    def test_save_with_clean_state_preserves_existing_file(
        self, tmp_path: Path
    ) -> None:
        """A valid save should not remove a pre-existing valid file."""
        store = make_store()
        store.subscribe("umo_1", "owner_1")
        p = tmp_path / "subs.json"
        # First save
        store.save(p)
        assert p.exists()
        mtime_before = p.stat().st_mtime_ns
        # Second save (same state)
        store.save(p)
        assert p.stat().st_mtime_ns >= mtime_before


# ---------------------------------------------------------------------------
# File permission (0o600)
# ---------------------------------------------------------------------------


class TestFilePermission:
    def test_subscription_file_permission(self, tmp_path: Path) -> None:
        store = make_store()
        store.subscribe("umo_perm", "owner_1")
        p = tmp_path / "subs.json"
        store.save(p)
        mode = p.stat().st_mode & 0o777
        assert mode == 0o600, f"Expected 0o600, got {oct(mode)}"
