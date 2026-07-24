"""Unit tests for GitLab MR comment polling functionality.

Covers six areas:
1. Deduplication persistence (load -> add -> save -> reload -> verify)
2. Atomic write (crash mid-write leaves original file intact)
3. LRU eviction (>10000 entries drops oldest by timestamp)
4. Bot filtering (mock note objects with various author names)
5. DiffNote /ask -> /ask_line rewrite logic
6. GUNICORN_WORKERS validation (lifespan startup check)

No real GitLab API access or FastAPI server is required.
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# GITLAB.URL must be set before importing gitlab_webhook (module-level check).
os.environ.setdefault("GITLAB__URL", "https://gitlab.example.com")

from pr_agent.config_loader import get_settings
from pr_agent.servers.gitlab_webhook import (
    _build_gitlab_polling_payload,
    _get_mr_head_sha,
    _run_gitlab_polling,
    _should_auto_review_mr,
    handle_ask_line,
    is_bot_user,
    is_draft,
    lifespan,
    should_process_pr_logic,
)
from pr_agent.servers.utils import _load_processed_comments, _save_processed_comments
from tests.unittest._settings_helpers import restore_settings, snapshot_settings


# ---------------------------------------------------------------------------
# Helpers for polling-loop inline logic
# ---------------------------------------------------------------------------

def _polling_is_bot(note, bot_indicators):
    """Replicate the bot-filtering check from ``_run_gitlab_polling``.

    The polling loop reads ``note.author`` (a dict on python-gitlab note objects)
    rather than the webhook payload's ``data['user']``.  This helper mirrors that
    inline logic so tests can exercise it with mock note objects.
    """
    author_name = getattr(note, "author", {}).get("name", "").lower()
    return any(ind in author_name for ind in bot_indicators)


def _polling_rewrite_diffnote(note, comment_body):
    """Replicate the DiffNote /ask -> /ask_line rewrite from ``_run_gitlab_polling``.

    The polling loop reads position data from ``note.position`` (a dict on
    python-gitlab note objects) rather than the webhook payload's
    ``data['object_attributes']['position']``.
    """
    position = note.position
    line_range = position.get("line_range", {})
    start_line = line_range.get("start", {}).get("new_line", 0)
    end_line = line_range.get("end", {}).get("new_line", 0)
    path = position.get("new_path", "")
    side = "RIGHT"
    discussion_id = getattr(note, "discussion_id", "")
    question = comment_body.replace("/ask", "").strip()
    return (
        f"/ask_line --line_start={start_line} "
        f"--line_end={end_line} --side={side} "
        f"--file_name={path} "
        f"--comment_id={discussion_id} {question}"
    )


def _make_note(**kwargs):
    """Create a mock note object with sensible defaults.

    Attributes mirror what python-gitlab note objects expose:
    ``.id``, ``.type``, ``.body``, ``.author``, ``.discussion_id``, ``.position``.
    """
    defaults = {
        "id": 1,
        "type": None,
        "body": "",
        "author": {"name": "Alice"},
        "discussion_id": "",
        "position": {},
    }
    defaults.update(kwargs)
    note = MagicMock()
    for key, value in defaults.items():
        setattr(note, key, value)
    return note


def _default_bot_indicators():
    """Return the lowercased bot_user_indicators from settings."""
    raw = get_settings().get("CONFIG.BOT_USER_INDICATORS", [])
    if isinstance(raw, str):
        raw = [raw]
    try:
        raw = list(raw)
    except TypeError:
        raw = []
    return [s.lower() for s in raw if isinstance(s, str)]


# ---------------------------------------------------------------------------
# 1. Deduplication persistence
# ---------------------------------------------------------------------------

class TestDedupPersistence:
    """Tests for _load_processed_comments and _save_processed_comments round-trip."""

    def test_load_returns_empty_when_file_missing(self, tmp_path):
        path = str(tmp_path / "nonexistent.json")
        assert _load_processed_comments(path) == {}

    def test_load_returns_dict_from_valid_json(self, tmp_path):
        path = str(tmp_path / "processed.json")
        data = {"c1": "2026-01-01T00:00:00+00:00", "c2": "2026-01-02T00:00:00+00:00"}
        with open(path, "w") as f:
            json.dump(data, f)
        assert _load_processed_comments(path) == data

    def test_load_returns_empty_on_corrupt_json(self, tmp_path):
        path = str(tmp_path / "corrupt.json")
        with open(path, "w") as f:
            f.write("{invalid json")
        assert _load_processed_comments(path) == {}

    def test_load_returns_empty_on_non_dict_json(self, tmp_path):
        path = str(tmp_path / "list.json")
        with open(path, "w") as f:
            json.dump(["not", "a", "dict"], f)
        assert _load_processed_comments(path) == {}

    def test_load_returns_empty_on_empty_file(self, tmp_path):
        path = str(tmp_path / "empty.json")
        with open(path, "w") as f:
            f.write("")
        assert _load_processed_comments(path) == {}

    def test_save_and_reload_roundtrip(self, tmp_path):
        path = str(tmp_path / "processed.json")
        data = {"c1": "2026-01-01T00:00:00+00:00", "c2": "2026-01-02T00:00:00+00:00"}
        _save_processed_comments(path, data)
        assert _load_processed_comments(path) == data

    def test_save_creates_parent_directories(self, tmp_path):
        path = str(tmp_path / "subdir" / "nested" / "processed.json")
        _save_processed_comments(path, {"c1": "2026-01-01"})
        assert os.path.exists(path)
        assert _load_processed_comments(path) == {"c1": "2026-01-01"}

    def test_full_lifecycle_load_add_save_reload_verify(self, tmp_path):
        """Full lifecycle: load -> add -> save -> reload -> verify."""
        path = str(tmp_path / "processed.json")

        # Start with empty store
        processed = _load_processed_comments(path)
        assert processed == {}

        # Add entries and persist
        processed["comment_100"] = "2026-01-01T10:00:00+00:00"
        processed["comment_101"] = "2026-01-01T11:00:00+00:00"
        _save_processed_comments(path, processed)

        # Reload and verify persistence
        reloaded = _load_processed_comments(path)
        assert reloaded == processed
        assert "comment_100" in reloaded
        assert "comment_101" in reloaded

        # Add another entry and save again
        reloaded["comment_102"] = "2026-01-01T12:00:00+00:00"
        _save_processed_comments(path, reloaded)

        # Final reload
        final = _load_processed_comments(path)
        assert len(final) == 3
        assert "comment_102" in final


# ---------------------------------------------------------------------------
# 2. Atomic write
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    """Tests that atomic write preserves the original file on crash."""

    def test_original_intact_when_tmp_written_but_not_replaced(self, tmp_path):
        """Simulate crash: write to .tmp but never call os.replace.

        The real _save_processed_comments writes to ``path.tmp`` then
        ``os.replace``s it.  If the process crashes after the temp write but
        before the replace, the original file must remain intact.
        """
        path = str(tmp_path / "processed.json")
        original = {"c1": "2026-01-01"}
        _save_processed_comments(path, original)

        # Simulate crash: write garbage to .tmp but do NOT call os.replace
        tmp_file = f"{path}.tmp"
        with open(tmp_file, "w") as f:
            json.dump({"should_not_appear": "crash"}, f)

        # Original file must still be intact
        loaded = _load_processed_comments(path)
        assert loaded == original
        assert "should_not_appear" not in loaded

    def test_save_overwrites_atomically(self, tmp_path):
        path = str(tmp_path / "processed.json")
        _save_processed_comments(path, {"old": "2026-01-01"})
        _save_processed_comments(path, {"new": "2026-01-02"})
        loaded = _load_processed_comments(path)
        assert loaded == {"new": "2026-01-02"}
        assert "old" not in loaded

    def test_no_tmp_file_left_after_successful_save(self, tmp_path):
        path = str(tmp_path / "processed.json")
        _save_processed_comments(path, {"c1": "2026-01-01"})
        assert not os.path.exists(f"{path}.tmp")

    def test_save_empty_dict(self, tmp_path):
        path = str(tmp_path / "processed.json")
        _save_processed_comments(path, {})
        assert _load_processed_comments(path) == {}


# ---------------------------------------------------------------------------
# 3. LRU eviction
# ---------------------------------------------------------------------------

class TestLRUEviction:
    """Tests that >10000 entries triggers LRU eviction of oldest by timestamp."""

    @staticmethod
    def _ts(i):
        """Generate a sortable ISO timestamp from an integer index."""
        return f"2026-01-01T{i // 3600:02d}:{(i % 3600) // 60:02d}:{i % 60:02d}+00:00"

    def test_eviction_keeps_10000_newest(self, tmp_path):
        path = str(tmp_path / "processed.json")
        processed = {f"c{i}": self._ts(i) for i in range(10005)}
        _save_processed_comments(path, processed)
        loaded = _load_processed_comments(path)
        assert len(loaded) == 10000
        # Oldest 5 should be evicted
        for i in range(5):
            assert f"c{i}" not in loaded
        # Newest 10000 should be kept
        for i in range(5, 10005):
            assert f"c{i}" in loaded

    def test_no_eviction_at_exactly_10000(self, tmp_path):
        path = str(tmp_path / "processed.json")
        processed = {f"c{i}": self._ts(i) for i in range(10000)}
        _save_processed_comments(path, processed)
        loaded = _load_processed_comments(path)
        assert len(loaded) == 10000

    def test_eviction_drops_oldest_by_timestamp_not_insertion_order(self, tmp_path):
        """Verify eviction is by timestamp value, not dict insertion order."""
        path = str(tmp_path / "processed.json")
        processed = {}
        # Insert newest first, oldest last (reverse insertion order)
        for i in range(10002, -1, -1):
            processed[f"c{i}"] = self._ts(i)
        _save_processed_comments(path, processed)
        loaded = _load_processed_comments(path)
        assert len(loaded) == 10000
        # c0, c1, c2 have the oldest timestamps and should be evicted
        assert "c0" not in loaded
        assert "c1" not in loaded
        assert "c2" not in loaded
        # c3 through c10002 should remain
        assert "c3" in loaded
        assert "c10002" in loaded

    def test_eviction_preserves_correct_entries_with_mixed_timestamps(self, tmp_path):
        path = str(tmp_path / "processed.json")
        processed = {
            "oldest": "2020-01-01T00:00:00+00:00",
            "middle": "2025-06-01T00:00:00+00:00",
            "newest": "2026-01-01T00:00:00+00:00",
        }
        # 9998 filler entries -> total 10001 -> triggers eviction of 1 oldest
        for i in range(9998):
            processed[f"c{i}"] = f"2025-12-31T{i // 3600:02d}:{(i % 3600) // 60:02d}:{i % 60:02d}+00:00"
        _save_processed_comments(path, processed)
        loaded = _load_processed_comments(path)
        assert len(loaded) == 10000
        assert "oldest" not in loaded
        assert "newest" in loaded


# ---------------------------------------------------------------------------
# 4. Bot filtering
# ---------------------------------------------------------------------------

class TestBotFiltering:
    """Tests for is_bot_user (webhook path) and polling-loop bot filtering."""

    @pytest.fixture
    def restore_indicators(self):
        snapshot = snapshot_settings(["CONFIG.BOT_USER_INDICATORS"])
        yield
        restore_settings(snapshot)

    @staticmethod
    def _make_data(name):
        return {"user": {"name": name, "username": name.lower()}}

    # --- is_bot_user (webhook path, dict-based) ---

    def test_regular_user_not_bot(self, restore_indicators):
        assert is_bot_user(self._make_data("Alice")) is False

    def test_codium_bot_detected(self, restore_indicators):
        assert is_bot_user(self._make_data("Codium AI")) is True

    def test_bot_underscore_prefix_detected(self, restore_indicators):
        assert is_bot_user(self._make_data("bot_reviewer")) is True

    def test_bot_dash_prefix_detected(self, restore_indicators):
        assert is_bot_user(self._make_data("bot-reviewer")) is True

    def test_bot_underscore_suffix_detected(self, restore_indicators):
        assert is_bot_user(self._make_data("my_bot")) is True

    def test_bot_dash_suffix_detected(self, restore_indicators):
        assert is_bot_user(self._make_data("my-bot")) is True

    def test_case_insensitive_matching(self, restore_indicators):
        assert is_bot_user(self._make_data("CODIUM")) is True
        assert is_bot_user(self._make_data("BOT_Reviewer")) is True

    def test_empty_name_not_bot(self, restore_indicators):
        assert is_bot_user(self._make_data("")) is False

    def test_missing_user_key_not_bot(self, restore_indicators):
        assert is_bot_user({}) is False

    def test_missing_name_key_not_bot(self, restore_indicators):
        assert is_bot_user({"user": {}}) is False

    @pytest.mark.parametrize("name,expected", [
        ("Alice", False),
        ("Bob", False),
        ("codium", True),
        ("Codium AI", True),
        ("bot_reviewer", True),
        ("bot-reviewer", True),
        ("my_bot", True),
        ("my-bot", True),
        ("regular_user", False),
        ("dependabot", False),
        ("renovate[bot]", False),
    ])
    def test_parametrized_bot_detection(self, restore_indicators, name, expected):
        assert is_bot_user(self._make_data(name)) is expected

    # --- Polling-loop bot filtering (note-object-based) ---

    def test_polling_regular_user_not_bot(self, restore_indicators):
        note = _make_note(author={"name": "Alice"})
        assert _polling_is_bot(note, _default_bot_indicators()) is False

    def test_polling_codium_detected(self, restore_indicators):
        note = _make_note(author={"name": "Codium AI"})
        assert _polling_is_bot(note, _default_bot_indicators()) is True

    def test_polling_bot_prefix_detected(self, restore_indicators):
        note = _make_note(author={"name": "bot_reviewer"})
        assert _polling_is_bot(note, _default_bot_indicators()) is True

    def test_polling_bot_suffix_detected(self, restore_indicators):
        note = _make_note(author={"name": "my-bot"})
        assert _polling_is_bot(note, _default_bot_indicators()) is True

    def test_polling_case_insensitive(self, restore_indicators):
        note = _make_note(author={"name": "BOT_Reviewer"})
        assert _polling_is_bot(note, _default_bot_indicators()) is True

    def test_polling_empty_author_name_not_bot(self, restore_indicators):
        note = _make_note(author={})
        assert _polling_is_bot(note, _default_bot_indicators()) is False

    def test_polling_missing_author_attribute_not_bot(self, restore_indicators):
        """Note without author attribute should default to {} and not be a bot."""
        note = MagicMock(spec=["id", "type", "body", "discussion_id", "position"])
        note.id = 1
        note.type = None
        note.body = "/review"
        note.discussion_id = ""
        note.position = {}
        assert _polling_is_bot(note, _default_bot_indicators()) is False

    @pytest.mark.parametrize("name,expected", [
        ("Alice", False),
        ("Codium AI", True),
        ("bot_reviewer", True),
        ("bot-reviewer", True),
        ("my_bot", True),
        ("my-bot", True),
        ("regular_user", False),
        ("dependabot", False),
    ])
    def test_polling_parametrized(self, restore_indicators, name, expected):
        note = _make_note(author={"name": name})
        assert _polling_is_bot(note, _default_bot_indicators()) is expected


# ---------------------------------------------------------------------------
# 5. DiffNote /ask -> /ask_line rewrite
# ---------------------------------------------------------------------------

class TestDiffNoteRewrite:
    """Tests for handle_ask_line (webhook) and polling-loop DiffNote rewrite."""

    @staticmethod
    def _make_webhook_data(start_line=10, end_line=15, new_path="src/main.py",
                           discussion_id="disc_123"):
        return {
            "object_attributes": {
                "position": {
                    "line_range": {
                        "start": {"new_line": start_line, "type": "new"},
                        "end": {"new_line": end_line, "type": "new"},
                    },
                    "new_path": new_path,
                },
                "discussion_id": discussion_id,
                "type": "DiffNote",
                "id": 12345,
                "body": "/ask what does this do?",
            }
        }

    # --- handle_ask_line (webhook path, dict-based) ---

    def test_handle_ask_line_basic_rewrite(self):
        body = "/ask what does this do?"
        data = self._make_webhook_data()
        result = handle_ask_line(body, data)
        assert result.startswith("/ask_line")
        assert "--line_start=10" in result
        assert "--line_end=15" in result
        assert "--side=RIGHT" in result
        assert "--file_name=src/main.py" in result
        assert "--comment_id=disc_123" in result
        assert "what does this do?" in result

    def test_handle_ask_line_exact_output(self):
        body = "/ask why this change?"
        data = self._make_webhook_data(
            start_line=10, end_line=12, new_path="src/app.py", discussion_id="disc-1"
        )
        result = handle_ask_line(body, data)
        assert result == (
            "/ask_line --line_start=10 --line_end=12 --side=RIGHT "
            "--file_name=src/app.py --comment_id=disc-1 why this change?"
        )

    def test_handle_ask_line_complex_question(self):
        body = "/ask Why is this function using a recursive approach instead of iterative?"
        data = self._make_webhook_data(
            start_line=42, end_line=50, new_path="lib/utils.py", discussion_id="abc"
        )
        result = handle_ask_line(body, data)
        assert "--line_start=42" in result
        assert "--line_end=50" in result
        assert "--file_name=lib/utils.py" in result
        assert "--comment_id=abc" in result
        assert "Why is this function using a recursive approach instead of iterative?" in result

    def test_handle_ask_line_preserves_question_text(self):
        body = "/ask explain this code"
        data = self._make_webhook_data()
        result = handle_ask_line(body, data)
        assert "explain this code" in result

    def test_handle_ask_line_returns_original_on_missing_position(self):
        body = "/ask explain"
        data = {"object_attributes": {}}
        result = handle_ask_line(body, data)
        assert result == "/ask explain"

    def test_handle_ask_line_returns_original_on_exception(self):
        body = "/ask explain"
        data = {"object_attributes": {"position": "not_a_dict"}}
        result = handle_ask_line(body, data)
        assert result == "/ask explain"

    # --- Polling-loop DiffNote rewrite (note-object-based) ---

    def test_polling_diffnote_rewrite_basic(self):
        """Test with a mock note having .position, .type, .id, .body, .author."""
        note = _make_note(
            id=67890,
            type="DiffNote",
            body="/ask what is this?",
            author={"name": "Alice"},
            discussion_id="disc_456",
            position={
                "line_range": {
                    "start": {"new_line": 20, "type": "new"},
                    "end": {"new_line": 25, "type": "new"},
                },
                "new_path": "src/app.py",
            },
        )
        result = _polling_rewrite_diffnote(note, note.body)
        assert result.startswith("/ask_line")
        assert "--line_start=20" in result
        assert "--line_end=25" in result
        assert "--side=RIGHT" in result
        assert "--file_name=src/app.py" in result
        assert "--comment_id=disc_456" in result
        assert "what is this?" in result

    def test_polling_diffnote_rewrite_exact_output(self):
        note = _make_note(
            type="DiffNote",
            body="/ask why this change?",
            discussion_id="disc-1",
            position={
                "line_range": {
                    "start": {"new_line": 10},
                    "end": {"new_line": 12},
                },
                "new_path": "src/app.py",
            },
        )
        result = _polling_rewrite_diffnote(note, "/ask why this change?")
        assert result == (
            "/ask_line --line_start=10 --line_end=12 --side=RIGHT "
            "--file_name=src/app.py --comment_id=disc-1 why this change?"
        )

    def test_polling_diffnote_rewrite_preserves_question(self):
        note = _make_note(
            type="DiffNote",
            body="/ask explain this code",
            discussion_id="d1",
            position={
                "line_range": {
                    "start": {"new_line": 1},
                    "end": {"new_line": 5},
                },
                "new_path": "file.py",
            },
        )
        result = _polling_rewrite_diffnote(note, "/ask explain this code")
        assert "explain this code" in result

    def test_polling_diffnote_rewrite_with_all_note_attributes(self):
        """Verify rewrite works with a fully-populated mock note object."""
        note = _make_note(
            id=99999,
            type="DiffNote",
            body="/ask what does this do?",
            author={"name": "Bob"},
            discussion_id="disc_789",
            position={
                "line_range": {
                    "start": {"new_line": 30, "type": "new"},
                    "end": {"new_line": 35, "type": "new"},
                },
                "new_path": "src/main.py",
            },
        )
        # Verify all attributes are accessible
        assert note.type == "DiffNote"
        assert note.id == 99999
        assert note.body == "/ask what does this do?"
        assert note.author == {"name": "Bob"}
        assert note.discussion_id == "disc_789"
        assert note.position["new_path"] == "src/main.py"

        result = _polling_rewrite_diffnote(note, note.body)
        assert result == (
            "/ask_line --line_start=30 --line_end=35 --side=RIGHT "
            "--file_name=src/main.py --comment_id=disc_789 what does this do?"
        )


# ---------------------------------------------------------------------------
# 6. GUNICORN_WORKERS validation
# ---------------------------------------------------------------------------

class TestGunicornWorkersValidation:
    """Tests for the lifespan startup check on GUNICORN_WORKERS."""

    @pytest.fixture
    def settings_snapshot(self):
        snapshot = snapshot_settings(["GITLAB.POLLING_ENABLED"])
        yield
        restore_settings(snapshot)

    @pytest.fixture
    def fresh_shutdown_event(self, monkeypatch):
        """Provide a fresh asyncio.Event bound to the current event loop.

        The module-level ``_shutdown_event`` is created at import time and may
        be bound to a different event loop.  Patching it with a fresh Event
        avoids ``RuntimeError: ... is bound to a different event loop``.
        """
        event = asyncio.Event()
        monkeypatch.setattr("pr_agent.servers.gitlab_webhook._shutdown_event", event)
        return event

    async def test_raises_when_workers_gt_1_and_polling_enabled(
        self, settings_snapshot, fresh_shutdown_event, monkeypatch
    ):
        get_settings().set("GITLAB.POLLING_ENABLED", True)
        monkeypatch.setenv("GUNICORN_WORKERS", "4")
        with pytest.raises(RuntimeError, match="GUNICORN_WORKERS must be 1"):
            async with lifespan(MagicMock()):
                pass

    async def test_no_raise_when_workers_is_1_and_polling_enabled(
        self, settings_snapshot, fresh_shutdown_event, monkeypatch
    ):
        get_settings().set("GITLAB.POLLING_ENABLED", True)
        monkeypatch.setenv("GUNICORN_WORKERS", "1")
        with patch("pr_agent.servers.gitlab_webhook._run_gitlab_polling", new_callable=AsyncMock):
            async with lifespan(MagicMock()):
                pass

    async def test_no_raise_when_workers_unset_and_polling_enabled(
        self, settings_snapshot, fresh_shutdown_event, monkeypatch
    ):
        get_settings().set("GITLAB.POLLING_ENABLED", True)
        monkeypatch.delenv("GUNICORN_WORKERS", raising=False)
        with patch("pr_agent.servers.gitlab_webhook._run_gitlab_polling", new_callable=AsyncMock):
            async with lifespan(MagicMock()):
                pass

    async def test_no_raise_when_polling_disabled_even_with_workers_gt_1(
        self, settings_snapshot, fresh_shutdown_event, monkeypatch
    ):
        get_settings().set("GITLAB.POLLING_ENABLED", False)
        monkeypatch.setenv("GUNICORN_WORKERS", "4")
        # No mock needed: polling task is not created when polling is disabled
        async with lifespan(MagicMock()):
            pass

    async def test_no_raise_when_workers_is_0_and_polling_enabled(
        self, settings_snapshot, fresh_shutdown_event, monkeypatch
    ):
        """Workers=0 is not >1, so no error (though 0 workers is unusual)."""
        get_settings().set("GITLAB.POLLING_ENABLED", True)
        monkeypatch.setenv("GUNICORN_WORKERS", "0")
        with patch("pr_agent.servers.gitlab_webhook._run_gitlab_polling", new_callable=AsyncMock):
            async with lifespan(MagicMock()):
                pass

    async def test_raises_value_error_when_workers_not_integer(
        self, settings_snapshot, fresh_shutdown_event, monkeypatch
    ):
        """Non-integer GUNICORN_WORKERS causes ValueError from int() call."""
        get_settings().set("GITLAB.POLLING_ENABLED", True)
        monkeypatch.setenv("GUNICORN_WORKERS", "not-a-number")
        with pytest.raises(ValueError):
            async with lifespan(MagicMock()):
                pass


# ---------------------------------------------------------------------------
# 7. Processed MRs persistence (processed_mrs.json)
# ---------------------------------------------------------------------------

class TestProcessedMrsPersistence:
    """Tests for processed_mrs.json load/save round-trip and pruning.

    The MR-state file uses the same atomic JSON helpers as
    processed_comments.json but stores ``{str(iid): head_sha}``.
    """

    def test_load_returns_empty_when_file_missing(self, tmp_path):
        path = str(tmp_path / "processed_mrs.json")
        assert _load_processed_comments(path) == {}

    def test_save_and_reload_roundtrip(self, tmp_path):
        path = str(tmp_path / "processed_mrs.json")
        state = {"1": "abc123", "2": "def456"}
        _save_processed_comments(path, state)
        assert _load_processed_comments(path) == state

    def test_pruning_closed_iids(self, tmp_path):
        """State with a closed IID plus open IIDs prunes to only open IIDs."""
        path = str(tmp_path / "processed_mrs.json")
        state = {"1": "shaA", "2": "shaB", "3": "shaC"}
        _save_processed_comments(path, state)
        open_iids = {"1", "2"}
        loaded = _load_processed_comments(path)
        pruned = {iid: sha for iid, sha in loaded.items() if iid in open_iids}
        assert pruned == {"1": "shaA", "2": "shaB"}
        assert "3" not in pruned

    def test_atomic_save_no_tmp_file(self, tmp_path):
        path = str(tmp_path / "processed_mrs.json")
        _save_processed_comments(path, {"1": "shaA"})
        assert not os.path.exists(f"{path}.tmp")

    def test_correct_path_under_data_dir(self, tmp_path):
        data_dir = str(tmp_path / "poller")
        path = os.path.join(data_dir, "processed_mrs.json")
        _save_processed_comments(path, {"1": "shaA"})
        assert os.path.exists(path)
        assert path.endswith("processed_mrs.json")
        assert os.path.dirname(path) == data_dir


# ---------------------------------------------------------------------------
# 8. Defensive SHA accessor (_get_mr_head_sha)
# ---------------------------------------------------------------------------

class TestGetMrHeadSha:
    """Tests for _get_mr_head_sha defensive SHA accessor."""

    def test_returns_sha_when_present(self):
        mr = MagicMock()
        mr.sha = "abc123"
        mr.iid = 1
        project = MagicMock()
        assert _get_mr_head_sha(mr, project) == "abc123"
        project.mergerequests.get.assert_not_called()

    def test_fetches_full_mr_when_sha_missing(self):
        mr = MagicMock()
        mr.sha = None
        mr.iid = 42
        full_mr = MagicMock()
        full_mr.sha = "full_sha"
        project = MagicMock()
        project.mergerequests.get.return_value = full_mr
        assert _get_mr_head_sha(mr, project) == "full_sha"
        project.mergerequests.get.assert_called_once_with(42)

    def test_returns_none_when_sha_still_missing(self):
        mr = MagicMock()
        mr.sha = None
        mr.iid = 1
        full_mr = MagicMock()
        full_mr.sha = None
        project = MagicMock()
        project.mergerequests.get.return_value = full_mr
        assert _get_mr_head_sha(mr, project) is None

    def test_returns_none_when_sha_empty_string(self):
        mr = MagicMock()
        mr.sha = ""
        mr.iid = 1
        full_mr = MagicMock()
        full_mr.sha = ""
        project = MagicMock()
        project.mergerequests.get.return_value = full_mr
        assert _get_mr_head_sha(mr, project) is None

    def test_does_not_swallow_exceptions(self):
        mr = MagicMock()
        mr.sha = None
        mr.iid = 1
        project = MagicMock()
        project.mergerequests.get.side_effect = RuntimeError("API error")
        with pytest.raises(RuntimeError, match="API error"):
            _get_mr_head_sha(mr, project)


# ---------------------------------------------------------------------------
# 9. Synthetic webhook payload (_build_gitlab_polling_payload)
# ---------------------------------------------------------------------------

class TestBuildGitlabPollingPayload:
    """Tests for _build_gitlab_polling_payload synthetic webhook construction."""

    @staticmethod
    def _make_mr(**kwargs):
        defaults = {
            "iid": 1,
            "title": "Test MR",
            "source_branch": "feature",
            "target_branch": "main",
            "draft": None,
            "work_in_progress": False,
            "labels": [],
            "author": {"username": "alice", "name": "Alice"},
            "web_url": "https://gitlab.example.com/org/repo/-/merge_requests/1",
        }
        defaults.update(kwargs)
        mr = MagicMock()
        for key, value in defaults.items():
            setattr(mr, key, value)
        return mr

    def test_draft_true_payload(self):
        mr = self._make_mr(draft=True)
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["object_attributes"]["draft"] is True
        assert is_draft(payload) is True

    def test_work_in_progress_true_payload(self):
        mr = self._make_mr(draft=None, work_in_progress=True)
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["object_attributes"]["draft"] is True
        assert is_draft(payload) is True

    def test_not_draft_payload(self):
        mr = self._make_mr(draft=False, work_in_progress=False)
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["object_attributes"]["draft"] is False
        assert is_draft(payload) is False

    def test_label_conversion(self):
        mr = self._make_mr(labels=["bug", "feature"])
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["object_attributes"]["labels"] == [
            {"title": "bug"}, {"title": "feature"}
        ]

    def test_label_conversion_filters_non_strings(self):
        mr = self._make_mr(labels=["bug", 123, None, "feature"])
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["object_attributes"]["labels"] == [
            {"title": "bug"}, {"title": "feature"}
        ]

    def test_missing_author_fields(self):
        mr = self._make_mr(author=None)
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["user"]["username"] == ""
        assert payload["user"]["name"] == ""

    def test_partial_author_fields(self):
        mr = self._make_mr(author={"username": "bob"})
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["user"]["username"] == "bob"
        assert payload["user"]["name"] == ""

    def test_head_sha_included_in_last_commit(self):
        mr = self._make_mr()
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert payload["object_attributes"]["last_commit"] == {"id": "sha123"}

    def test_head_sha_empty_last_commit(self):
        mr = self._make_mr()
        payload = _build_gitlab_polling_payload(mr, "org/repo", None)
        assert payload["object_attributes"]["last_commit"] == {}

    def test_should_process_pr_logic_passes(self):
        mr = self._make_mr()
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is True

    @pytest.fixture
    def restore_ignore_filters(self):
        keys = [
            "CONFIG.IGNORE_PR_LABELS",
            "CONFIG.IGNORE_PR_TITLE",
            "CONFIG.IGNORE_PR_SOURCE_BRANCHES",
            "CONFIG.IGNORE_PR_TARGET_BRANCHES",
            "CONFIG.IGNORE_REPOSITORIES",
            "CONFIG.IGNORE_PR_AUTHORS",
        ]
        snapshot = snapshot_settings(keys)
        yield
        restore_settings(snapshot)

    def test_should_process_pr_logic_ignore_labels(self, restore_ignore_filters):
        get_settings().set("CONFIG.IGNORE_PR_LABELS", ["skip-review"])
        mr = self._make_mr(labels=["skip-review"])
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is False

    def test_should_process_pr_logic_ignore_title(self, restore_ignore_filters):
        get_settings().set("CONFIG.IGNORE_PR_TITLE", ["^Test"])
        mr = self._make_mr(title="Test MR")
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is False

    def test_should_process_pr_logic_ignore_source_branch(self, restore_ignore_filters):
        get_settings().set("CONFIG.IGNORE_PR_SOURCE_BRANCHES", ["^feature"])
        mr = self._make_mr(source_branch="feature")
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is False

    def test_should_process_pr_logic_ignore_target_branch(self, restore_ignore_filters):
        get_settings().set("CONFIG.IGNORE_PR_TARGET_BRANCHES", ["^main$"])
        mr = self._make_mr(target_branch="main")
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is False

    def test_should_process_pr_logic_ignore_repositories(self, restore_ignore_filters):
        get_settings().set("CONFIG.IGNORE_REPOSITORIES", ["^org/repo$"])
        mr = self._make_mr()
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is False

    def test_should_process_pr_logic_ignore_authors(self, restore_ignore_filters):
        get_settings().set("CONFIG.IGNORE_PR_AUTHORS", ["^alice$"])
        mr = self._make_mr(author={"username": "alice", "name": "Alice"})
        payload = _build_gitlab_polling_payload(mr, "org/repo", "sha123")
        assert should_process_pr_logic(payload) is False


# ---------------------------------------------------------------------------
# 10. Auto-review trigger decision (_should_auto_review_mr)
# ---------------------------------------------------------------------------

class TestShouldAutoReviewMr:
    """Tests for _should_auto_review_mr decision helper.

    Rules evaluated in order:
    (a) draft -> (None, "remove")
    (b) no head_sha -> (None, "skip")
    (c) bot user -> (None, "persist")
    (d) new iid created after app startup -> ("pr_commands", "persist")
    (e) new iid created at/before startup or undated -> (None, "persist")
    (f) sha changed + handle_push_trigger -> ("push_commands", "persist")
    (g) sha changed + no push trigger -> (None, "persist")
    (h) same sha -> (None, "skip")
    """

    @pytest.fixture
    def restore_indicators(self):
        snapshot = snapshot_settings(["CONFIG.BOT_USER_INDICATORS"])
        yield
        restore_settings(snapshot)

    @staticmethod
    def _make_payload(*, draft=False, author_name="Alice", author_username="alice"):
        return {
            "object_attributes": {
                "iid": 1,
                "title": "Test MR",
                "source_branch": "feature",
                "target_branch": "main",
                "draft": draft,
                "labels": [],
                "url": "https://gitlab.example.com/mr/1",
                "last_commit": {"id": "sha123"},
            },
            "user": {"username": author_username, "name": author_name},
            "project": {"path_with_namespace": "org/repo"},
        }

    _sentinel = object()

    def _call(self, processed, iid, head_sha, payload, handle_push_trigger=False,
              *, mr_created_at=_sentinel, app_startup=_sentinel):
        """Invoke _should_auto_review_mr with default test timestamps."""
        if app_startup is self._sentinel:
            app_startup = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if mr_created_at is self._sentinel:
            mr_created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return _should_auto_review_mr(
            processed, iid, head_sha, payload, handle_push_trigger,
            mr_created_at, app_startup,
        )

    def test_new_mr_created_after_startup_returns_pr_commands_persist(self, restore_indicators):
        result = self._call({}, 1, "sha123", self._make_payload(), False)
        assert result == ("pr_commands", "persist")

    def test_new_mr_created_at_startup_returns_persist(self, restore_indicators):
        startup = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = self._call(
            {}, 1, "sha123", self._make_payload(), False,
            mr_created_at=startup, app_startup=startup,
        )
        assert result == (None, "persist")

    def test_new_mr_created_before_startup_returns_persist(self, restore_indicators):
        startup = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        created = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        result = self._call(
            {}, 1, "sha123", self._make_payload(), False,
            mr_created_at=created, app_startup=startup,
        )
        assert result == (None, "persist")

    def test_new_mr_missing_created_at_returns_persist(self, restore_indicators):
        result = self._call(
            {}, 1, "sha123", self._make_payload(), False,
            mr_created_at=None,
        )
        assert result == (None, "persist")

    def test_unchanged_sha_returns_skip(self, restore_indicators):
        processed = {"1": "sha123"}
        result = self._call(processed, 1, "sha123", self._make_payload(), False)
        assert result == (None, "skip")

    def test_changed_sha_with_push_trigger_returns_push_commands(self, restore_indicators):
        processed = {"1": "old_sha"}
        result = self._call(processed, 1, "new_sha", self._make_payload(), True)
        assert result == ("push_commands", "persist")

    def test_changed_sha_without_push_trigger_returns_persist(self, restore_indicators):
        processed = {"1": "old_sha"}
        result = self._call(processed, 1, "new_sha", self._make_payload(), False)
        assert result == (None, "persist")

    def test_draft_returns_remove(self, restore_indicators):
        result = self._call(
            {"1": "sha123"}, 1, "sha123", self._make_payload(draft=True), False
        )
        assert result == (None, "remove")

    def test_bot_author_returns_persist(self, restore_indicators):
        payload = self._make_payload(author_name="bot_reviewer")
        result = self._call({}, 1, "sha123", payload, False)
        assert result == (None, "persist")

    def test_missing_head_sha_returns_skip(self, restore_indicators):
        result = self._call({}, 1, None, self._make_payload(), False)
        assert result == (None, "skip")

    def test_empty_string_head_sha_returns_skip(self, restore_indicators):
        result = self._call({}, 1, "", self._make_payload(), False)
        assert result == (None, "skip")

    def test_corrupt_processed_mrs_treated_as_empty(self, restore_indicators):
        result = self._call(
            ["not", "a", "dict"], 1, "sha123", self._make_payload(), False
        )
        assert result == ("pr_commands", "persist")

    def test_does_not_mutate_processed_mrs(self, restore_indicators):
        processed = {"1": "old_sha"}
        original = dict(processed)
        self._call(processed, 1, "new_sha", self._make_payload(), True)
        assert processed == original

    def test_draft_takes_precedence_over_head_sha(self, restore_indicators):
        """Draft check comes before head_sha check."""
        result = self._call(
            {"1": "sha"}, 1, None, self._make_payload(draft=True), False
        )
        assert result == (None, "remove")

    def test_bot_takes_precedence_over_new_mr(self, restore_indicators):
        """Bot check comes before new-MR check."""
        payload = self._make_payload(author_name="bot_reviewer")
        result = self._call({}, 1, "sha123", payload, False)
        assert result == (None, "persist")


# ---------------------------------------------------------------------------
# 11. Wired polling loop (_run_gitlab_polling)
# ---------------------------------------------------------------------------

def _make_polling_mr(*, iid=1, sha: str | None = "sha1", draft=None,
                     work_in_progress=False, title="Test MR",
                     source_branch="feature", target_branch="main",
                     labels=None, author=None, web_url=None, notes=None,
                     created_at="2026-01-01T00:00:00Z"):
    """Create a mock MR object for the polling loop.

    Attributes mirror what python-gitlab MR objects expose plus ``.notes``
    with a ``.list()`` method for the comment-processing pass.
    """
    mr = MagicMock()
    mr.iid = iid
    mr.sha = sha
    mr.title = title
    mr.source_branch = source_branch
    mr.target_branch = target_branch
    mr.draft = draft
    mr.work_in_progress = work_in_progress
    mr.labels = labels if labels is not None else []
    mr.author = author if author is not None else {"username": "alice", "name": "Alice"}
    mr.web_url = web_url or f"https://gitlab.example.com/org/repo/-/merge_requests/{iid}"
    mr.created_at = created_at
    notes_mock = MagicMock()
    notes_mock.list.return_value = notes if notes is not None else []
    mr.notes = notes_mock
    return mr


async def _run_polling_with_mocks(cycle_mrs, *, data_dir, shutdown_event,
                                  handle_push_trigger=False,
                                  perform_side_effect=None,
                                  full_mr_sha=None,
                                  startup_time=None):
    """Run _run_gitlab_polling with mocked dependencies for a fixed number of cycles.

    The shutdown event is set after the last cycle's processed_mrs.json save,
    so the auto-review and comment passes complete before the loop exits.

    Args:
        cycle_mrs: list of lists of mock MRs, one per cycle.
        data_dir: path to use as POLLING_DATA_DIR (should be a tmp_path).
        shutdown_event: asyncio.Event to signal shutdown (patched via fixture).
        handle_push_trigger: value for gitlab.handle_push_trigger setting.
        perform_side_effect: optional side effect for _perform_commands_gitlab.
        full_mr_sha: SHA to return from project.mergerequests.get() (default None).
        startup_time: app startup timestamp to pass to the poller.  Defaults to
                      1970-01-01 so mock MRs (created in 2026) behave as new.

    Returns:
        Tuple of (perform_mock, pr_agent_mock) for assertions.
    """
    get_settings().set("gitlab.handle_push_trigger", handle_push_trigger)

    call_count = 0
    total_cycles = len(cycle_mrs)
    save_count = 0

    def list_side_effect(*args, **kwargs):
        nonlocal call_count
        idx = call_count
        call_count += 1
        if idx < total_cycles:
            return cycle_mrs[idx]
        return []

    def save_side_effect(path, data):
        nonlocal save_count
        _save_processed_comments(path, data)
        if path.endswith("processed_mrs.json"):
            save_count += 1
            if save_count >= total_cycles:
                shutdown_event.set()

    project = MagicMock()
    project.mergerequests.list.side_effect = list_side_effect
    full_mr = MagicMock()
    full_mr.sha = full_mr_sha
    project.mergerequests.get.return_value = full_mr

    gl_client = MagicMock()
    gl_client.projects.get.return_value = project

    pr_agent_mock = MagicMock()
    pr_agent_mock.handle_request = AsyncMock(return_value=True)

    if startup_time is None:
        startup_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

    with patch("gitlab.Gitlab", return_value=gl_client), \
         patch("pr_agent.servers.gitlab_webhook.PRAgent", return_value=pr_agent_mock), \
         patch("pr_agent.servers.gitlab_webhook._perform_commands_gitlab",
               new_callable=AsyncMock) as perform, \
         patch("pr_agent.servers.utils._save_processed_comments",
               side_effect=save_side_effect):
        if perform_side_effect:
            perform.side_effect = perform_side_effect
        await _run_gitlab_polling(startup_time)
        return perform, pr_agent_mock


class TestRunGitlabPollingAutoReview:
    """Tests for the wired auto-review + comment polling loop."""

    @pytest.fixture
    def polling_settings(self, tmp_path):
        keys = [
            "GITLAB.POLLING_INTERVAL",
            "GITLAB.PROJECT_PATH",
            "GITLAB.POLLING_DATA_DIR",
            "GITLAB.URL",
            "GITLAB.PERSONAL_ACCESS_TOKEN",
            "GITLAB.AUTH_TYPE",
            "GITLAB.SSL_VERIFY",
            "gitlab.handle_push_trigger",
            "CONFIG.BOT_USER_INDICATORS",
        ]
        snapshot = snapshot_settings(keys)
        get_settings().set("GITLAB.POLLING_INTERVAL", 0.1)
        get_settings().set("GITLAB.PROJECT_PATH", "org/repo")
        get_settings().set("GITLAB.POLLING_DATA_DIR", str(tmp_path))
        get_settings().set("GITLAB.URL", "https://gitlab.example.com")
        get_settings().set("GITLAB.PERSONAL_ACCESS_TOKEN", "test-token")
        get_settings().set("GITLAB.AUTH_TYPE", "oauth_token")
        get_settings().set("GITLAB.SSL_VERIFY", True)
        get_settings().set("gitlab.handle_push_trigger", False)
        yield
        restore_settings(snapshot)

    @pytest.fixture
    def shutdown_event(self, monkeypatch):
        event = asyncio.Event()
        monkeypatch.setattr("pr_agent.servers.gitlab_webhook._shutdown_event", event)
        return event

    @staticmethod
    def _mrs_path(tmp_path):
        return os.path.join(str(tmp_path), "processed_mrs.json")

    async def test_two_open_mrs_fire_pr_commands(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Two new MRs both fire pr_commands and are persisted."""
        mr1 = _make_polling_mr(iid=1, sha="sha1")
        mr2 = _make_polling_mr(iid=2, sha="sha2")
        perform, _ = await _run_polling_with_mocks(
            [[mr1, mr2]], data_dir=str(tmp_path), shutdown_event=shutdown_event
        )
        assert perform.call_count == 2
        assert perform.call_args_list[0].args[0] == "pr_commands"
        assert perform.call_args_list[1].args[0] == "pr_commands"
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "sha1", "2": "sha2"}

    async def test_sha_change_fires_push_commands_when_handle_push_trigger_true(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Known MR with changed SHA fires push_commands when handle_push_trigger=true."""
        _save_processed_comments(self._mrs_path(tmp_path), {"1": "old_sha"})
        mr1 = _make_polling_mr(iid=1, sha="new_sha")
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event,
            handle_push_trigger=True
        )
        assert perform.call_count == 1
        assert perform.call_args_list[0].args[0] == "push_commands"
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "new_sha"}

    async def test_draft_mr_does_not_trigger_and_removed_from_state(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Draft MR does not trigger commands and is removed from state."""
        _save_processed_comments(self._mrs_path(tmp_path), {"1": "sha1"})
        mr1 = _make_polling_mr(iid=1, sha="sha1", draft=True)
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event
        )
        assert perform.call_count == 0
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert "1" not in saved

    async def test_bot_author_persisted(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Bot-author MR is persisted without running commands."""
        mr1 = _make_polling_mr(
            iid=1, sha="sha1",
            author={"username": "bot_reviewer", "name": "bot_reviewer"}
        )
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event
        )
        assert perform.call_count == 0
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "sha1"}

    async def test_closed_mrs_removed_from_state(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Closed MRs are pruned from state each cycle."""
        _save_processed_comments(self._mrs_path(tmp_path), {"1": "sha1", "2": "sha2"})
        mr1 = _make_polling_mr(iid=1, sha="sha1")
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event
        )
        assert perform.call_count == 0
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "sha1"}
        assert "2" not in saved

    async def test_comments_still_processed(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Comment pass still runs in the same cycle as auto-review."""
        _save_processed_comments(self._mrs_path(tmp_path), {"1": "sha1"})
        note = _make_note(id=12345, body="/review", author={"name": "Alice"})
        mr1 = _make_polling_mr(iid=1, sha="sha1", notes=[note])
        perform, pr_agent = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event
        )
        assert perform.call_count == 0
        pr_agent.handle_request.assert_called_once()
        assert pr_agent.handle_request.call_args.args[1] == "/review"

    async def test_sha_none_skipped_and_not_persisted(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """MR with sha=None is skipped and not persisted."""
        mr1 = _make_polling_mr(iid=1, sha=None)
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event,
            full_mr_sha=None
        )
        assert perform.call_count == 0
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {}

    async def test_cross_cycle_draft_to_ready_re_trigger(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Draft MR in cycle 1 is removed; ready in cycle 2 re-triggers pr_commands."""
        mr_draft = _make_polling_mr(iid=1, sha="sha1", draft=True)
        mr_ready = _make_polling_mr(iid=1, sha="sha1", draft=None, work_in_progress=False)
        perform, _ = await _run_polling_with_mocks(
            [[mr_draft], [mr_ready]], data_dir=str(tmp_path),
            shutdown_event=shutdown_event
        )
        assert perform.call_count == 1
        assert perform.call_args_list[0].args[0] == "pr_commands"
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "sha1"}

    async def test_unchanged_sha_skip_preserves_state(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Unchanged SHA skip preserves state."""
        _save_processed_comments(self._mrs_path(tmp_path), {"1": "sha1"})
        mr1 = _make_polling_mr(iid=1, sha="sha1")
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event
        )
        assert perform.call_count == 0
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "sha1"}

    async def test_exception_in_perform_commands_leaves_old_sha_for_retry(
        self, polling_settings, shutdown_event, tmp_path
    ):
        """Exception in _perform_commands_gitlab leaves old SHA for retry."""
        _save_processed_comments(self._mrs_path(tmp_path), {"1": "old_sha"})
        mr1 = _make_polling_mr(iid=1, sha="new_sha")
        perform, _ = await _run_polling_with_mocks(
            [[mr1]], data_dir=str(tmp_path), shutdown_event=shutdown_event,
            handle_push_trigger=True,
            perform_side_effect=RuntimeError("API error")
        )
        assert perform.call_count == 1
        saved = _load_processed_comments(self._mrs_path(tmp_path))
        assert saved == {"1": "old_sha"}
