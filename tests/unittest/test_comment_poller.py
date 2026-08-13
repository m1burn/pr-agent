"""Unit tests for the standalone GitLab polling server (transport layer).

The poller reuses the webhook server's helpers read-only
(_perform_commands_gitlab, is_bot_user) and needs no changes in
gitlab_webhook.py. These tests cover:
1. JSON state persistence (atomic write, corrupt-file tolerance)
2. Pure decision logic (MR transitions, note delivery, emoji dedup)
3. Payload probes feeding the webhook helpers (filters, bot detection)
4. Leader-lock behavior
5. Full poll-cycle integration with fake python-gitlab objects
6. Dispatch to PRAgent (auto-commands, command comments, eyes notify)

No real GitLab API access or FastAPI server is required.
"""

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pr_agent.config_loader import get_settings
from pr_agent.servers import gitlab_polling as gp
from pr_agent.servers.gitlab_webhook import (handle_ask_line, is_bot_user,
                                             should_process_pr_logic)
from tests.unittest._settings_helpers import restore_settings, snapshot_settings

BOT_ID = 999
PROJECT = "group/proj"
MR_URL = f"https://gitlab.example.com/{PROJECT}/-/merge_requests/7"
PAST = "2026-07-27T09:00:00.000Z"
PR_COMMAND_BODIES = ["/describe", "/review", "/improve"]  # default gitlab.pr_commands
PUSH_COMMAND_BODIES = ["/describe", "/review"]  # default gitlab.push_commands


# ---------------------------------------------------------------------------
# Fake python-gitlab objects
# ---------------------------------------------------------------------------

class FakeAwardEmojis:
    def __init__(self, names=()):
        self.awards = [
            SimpleNamespace(name=name, user={"id": BOT_ID}, id=i + 1)
            for i, name in enumerate(names)
        ]

    def list(self, get_all=True):
        return list(self.awards)

    def create(self, data):
        award = SimpleNamespace(name=data["name"], user={"id": BOT_ID}, id=len(self.awards) + 1)
        self.awards.append(award)
        return award

    def names(self):
        return {a.name for a in self.awards}


class FakeNote:
    def __init__(self, note_id, body, created_at, author=None, note_type=None, position=None, emojis=()):
        self.id = note_id
        self.body = body
        self.created_at = created_at
        self.type = note_type
        self.position = position
        self.discussion_id = "disc-1"
        self.author = author or {"username": "alice", "name": "Alice", "id": 1}
        self.awardemojis = FakeAwardEmojis(emojis)
        self.deleted = False

    def delete(self):
        self.deleted = True


class FakeNotesManager:
    def __init__(self, notes):
        self._notes = notes

    def list(self, **kwargs):
        return list(self._notes)


class FakeMR:
    def __init__(self, iid, sha="sha1", draft=False, updated_at=PAST, notes=None, author=None):
        self.iid = iid
        self.sha = sha
        self.draft = draft
        self.work_in_progress = draft
        self.updated_at = updated_at
        self.web_url = f"https://gitlab.example.com/{PROJECT}/-/merge_requests/{iid}"
        self.title = f"MR {iid}"
        self.labels = []
        self.source_branch = "feature"
        self.target_branch = "main"
        self.author = author or {"username": "alice", "name": "Alice", "id": 1}
        self.created_at = PAST
        self.notes = FakeNotesManager(notes or [])


class FakeMRManager:
    def __init__(self, mrs):
        self._mrs = {m.iid: m for m in mrs}

    def list(self, state="opened", get_all=True):
        return list(self._mrs.values())

    def get(self, iid):
        return self._mrs[iid]


class FakeProject:
    def __init__(self, mrs):
        self.mergerequests = FakeMRManager(mrs)


class FakeGitlab:
    def __init__(self, projects):
        self.projects = SimpleNamespace(get=lambda path: projects[path])
        self.user = SimpleNamespace(id=BOT_ID)


def _future_iso(seconds=5):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _make_agent(side_effect=None):
    agent = SimpleNamespace()
    if side_effect is not None:
        agent.handle_request = AsyncMock(side_effect=side_effect)
    else:
        agent.handle_request = AsyncMock(return_value=True)
    return agent


async def _run_cycle(gl, data_dir, retry_budget=None, agent=None, max_retries=2):
    """Run one poll cycle against fakes, returning the PRAgent mock."""
    agent = agent or _make_agent()
    provider = SimpleNamespace(add_eyes_reaction=lambda comment_id: None)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gp, "PRAgent", lambda: agent)
        mp.setattr(gp, "get_git_provider_with_context", lambda pr_url: provider)
        mp.setattr(gp, "apply_repo_settings", lambda api_url: None)
        # _perform_commands_gitlab resolves apply_repo_settings in its own module
        mp.setattr("pr_agent.servers.gitlab_webhook.apply_repo_settings", lambda api_url: None)
        await gp._poll_project(gl, PROJECT, str(data_dir), BOT_ID,
                               retry_budget if retry_budget is not None else {},
                               max_retries)
    return agent


def _dispatched_bodies(agent):
    return [call.args[1] for call in agent.handle_request.await_args_list]


# ---------------------------------------------------------------------------
# 1. JSON state helpers
# ---------------------------------------------------------------------------

class TestJsonState:
    def test_load_missing_file_returns_empty(self, tmp_path):
        assert gp._load_json_state(str(tmp_path / "missing.json")) == {}

    def test_roundtrip(self, tmp_path):
        path = str(tmp_path / "sub" / "state.json")
        gp._save_json_state(path, {"a": 1, "b": {"c": "d"}})
        assert gp._load_json_state(path) == {"a": 1, "b": {"c": "d"}}

    def test_corrupt_file_returns_empty(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("{not json")
        assert gp._load_json_state(str(path)) == {}

    def test_non_dict_json_returns_empty(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text('["a", "b"]')
        assert gp._load_json_state(str(path)) == {}

    def test_no_tmp_file_left_after_save(self, tmp_path):
        path = str(tmp_path / "state.json")
        gp._save_json_state(path, {"x": 1})
        assert not os.path.exists(f"{path}.tmp")

    def test_save_overwrites(self, tmp_path):
        path = str(tmp_path / "state.json")
        gp._save_json_state(path, {"x": 1})
        gp._save_json_state(path, {"y": 2})
        assert gp._load_json_state(path) == {"y": 2}


# ---------------------------------------------------------------------------
# 2. Pure decision logic
# ---------------------------------------------------------------------------

class TestParseTs:
    def test_z_suffix(self):
        parsed = gp._parse_ts("2026-07-27T10:00:00.000Z")
        assert parsed is not None and parsed.tzinfo is not None

    def test_naive_gets_utc(self):
        assert gp._parse_ts("2026-07-27T10:00:00").tzinfo == timezone.utc

    def test_garbage_returns_none(self):
        assert gp._parse_ts("not-a-date") is None
        assert gp._parse_ts(None) is None
        assert gp._parse_ts("") is None


class TestPollingProjects:
    @pytest.fixture
    def restore(self):
        snapshot = snapshot_settings(["gitlab.polling_projects"])
        yield
        restore_settings(snapshot)

    def test_list_from_settings(self, restore):
        get_settings().set("gitlab.polling_projects", ["a/b", " c/d "])
        assert gp._polling_projects() == ["a/b", "c/d"]

    def test_string_value_coerced_to_list(self, restore):
        get_settings().set("gitlab.polling_projects", "a/b")
        assert gp._polling_projects() == ["a/b"]

    def test_empty_when_unset(self, restore):
        get_settings().set("gitlab.polling_projects", [])
        assert gp._polling_projects() == []


class TestDecideMrEvent:
    def _entry(self, sha="sha1", draft=False):
        return {"sha": sha, "draft": draft, "updated_at": PAST}

    def test_first_cycle_freeze_records_without_event(self):
        assert gp._decide_mr_event(None, self._entry(), bot=False, initialized=False) is None

    def test_new_mr_fires_open(self):
        assert gp._decide_mr_event(None, self._entry(), bot=False, initialized=True) == "open"

    def test_new_draft_mr_fires_nothing(self):
        assert gp._decide_mr_event(None, self._entry(draft=True), bot=False, initialized=True) is None

    def test_new_bot_mr_fires_nothing(self):
        assert gp._decide_mr_event(None, self._entry(), bot=True, initialized=True) is None

    def test_new_mr_without_sha_fires_nothing(self):
        assert gp._decide_mr_event(None, self._entry(sha=None), bot=False, initialized=True) is None

    def test_same_sha_fires_nothing(self):
        prev = self._entry()
        assert gp._decide_mr_event(prev, self._entry(), bot=False, initialized=True) is None

    def test_sha_change_fires_push(self):
        prev = self._entry(sha="sha1")
        assert gp._decide_mr_event(prev, self._entry(sha="sha2"), bot=False, initialized=True) == "push"

    def test_draft_to_ready_fires_draft_ready(self):
        prev = self._entry(draft=True)
        assert gp._decide_mr_event(prev, self._entry(sha="sha2"), bot=False, initialized=True) == "draft_ready"

    def test_still_draft_fires_nothing(self):
        prev = self._entry(draft=True)
        assert gp._decide_mr_event(prev, self._entry(draft=True), bot=False, initialized=True) is None

    def test_resolved_sha_after_unknown_is_not_a_push(self):
        prev = self._entry(sha=None)
        assert gp._decide_mr_event(prev, self._entry(sha="sha1"), bot=False, initialized=True) is None


class TestShouldDeliverNote:
    cursor = datetime(2026, 7, 27, 10, 0, tzinfo=timezone.utc)
    at_cursor = cursor
    after_cursor = datetime(2026, 7, 27, 10, 1, tzinfo=timezone.utc)

    def test_plain_comment_skipped(self):
        assert gp._should_deliver_note("hello", self.after_cursor, set(), self.cursor, 0) is False

    def test_old_command_skipped(self):
        old = datetime(2026, 7, 27, 9, 0, tzinfo=timezone.utc)
        assert gp._should_deliver_note("/review", old, set(), self.cursor, 0) is False

    def test_fresh_command_delivered(self):
        assert gp._should_deliver_note("/review", self.after_cursor, set(), self.cursor, 0) is True

    def test_command_at_cursor_delivered(self):
        assert gp._should_deliver_note("/review", self.at_cursor, set(), self.cursor, 0) is True

    def test_done_emoji_skipped(self):
        assert gp._should_deliver_note("/review", self.after_cursor, {"white_check_mark"}, self.cursor, 5) is False

    def test_eyes_with_budget_retried(self):
        assert gp._should_deliver_note("/review", self.after_cursor, {"eyes"}, self.cursor, 1) is True

    def test_eyes_without_budget_skipped(self):
        assert gp._should_deliver_note("/review", self.after_cursor, {"eyes"}, self.cursor, 0) is False

    def test_done_beats_eyes(self):
        assert gp._should_deliver_note("/review", self.after_cursor,
                                       {"eyes", "white_check_mark"}, self.cursor, 5) is False

    def test_missing_created_at_skipped(self):
        assert gp._should_deliver_note("/review", None, set(), self.cursor, 0) is False


class TestOwnEmojiNames:
    def test_only_our_users_emojis_collected(self):
        note = FakeNote(1, "/review", PAST, emojis=["eyes"])
        note.awardemojis.awards.append(SimpleNamespace(name="rocket", user={"id": 42}, id=99))
        assert gp._own_emoji_names(note, BOT_ID) == {"eyes"}

    def test_failure_returns_empty(self):
        note = FakeNote(1, "/review", PAST)
        note.awardemojis = SimpleNamespace(list=lambda **kw: 1 / 0)
        assert gp._own_emoji_names(note, BOT_ID) == set()


class TestSlugAndPaths:
    def test_slug_replaces_slashes(self):
        assert gp._project_slug("group/sub/proj") == "group_sub_proj"

    def test_state_path(self, tmp_path):
        assert gp._state_path(str(tmp_path), "group/sub/proj").endswith("group_sub_proj.state.json")


# ---------------------------------------------------------------------------
# 3. Payload probes feeding the webhook helpers
# ---------------------------------------------------------------------------

class TestPayloadProbes:
    @pytest.fixture
    def restore(self):
        keys = ["CONFIG.IGNORE_REPOSITORIES", "CONFIG.IGNORE_PR_AUTHORS",
                "CONFIG.IGNORE_PR_LABELS", "CONFIG.IGNORE_PR_TITLE"]
        snapshot = snapshot_settings(keys)
        yield
        restore_settings(snapshot)

    def test_payload_shape(self):
        data = gp._build_mr_payload(FakeMR(7), PROJECT, "sha1")
        attrs = data["object_attributes"]
        assert data["project"]["path_with_namespace"] == PROJECT
        assert attrs["iid"] == 7
        assert attrs["last_commit"] == {"id": "sha1"}
        assert data["user"]["name"] == "Alice"

    def test_bot_detected_through_probe(self, restore):
        bot_author = {"username": "codiumai-bot", "name": "codiumai-bot", "id": 2}
        assert gp._is_bot(FakeMR(7, author=bot_author)) is True
        assert gp._is_bot(FakeMR(7)) is False

    def test_is_bot_user_accepts_note_probe(self, restore):
        bot_author = {"username": "codiumai-bot", "name": "codiumai-bot", "id": 2}
        note = FakeNote(1, "/review", PAST, author=bot_author)
        assert gp._is_bot(note) is True
        assert gp._is_bot(FakeNote(1, "/review", PAST)) is False

    def test_payload_passes_default_filters(self, restore):
        data = gp._build_mr_payload(FakeMR(7), PROJECT, "sha1")
        assert should_process_pr_logic(data) is True

    def test_payload_drives_ignore_repo(self, restore):
        get_settings().set("CONFIG.IGNORE_REPOSITORIES", ["^group/.*"])
        data = gp._build_mr_payload(FakeMR(7), PROJECT, "sha1")
        assert should_process_pr_logic(data) is False

    def test_payload_drives_ignore_author(self, restore):
        get_settings().set("CONFIG.IGNORE_PR_AUTHORS", ["alice"])
        data = gp._build_mr_payload(FakeMR(7), PROJECT, "sha1")
        assert should_process_pr_logic(data) is False

    def test_payload_drives_ignore_label(self, restore):
        get_settings().set("CONFIG.IGNORE_PR_LABELS", ["wip"])
        mr = FakeMR(7)
        mr.labels = ["wip", "ui"]
        data = gp._build_mr_payload(mr, PROJECT, "sha1")
        assert data["object_attributes"]["labels"] == [{"title": "wip"}, {"title": "ui"}]
        assert should_process_pr_logic(data) is False

    def test_is_bot_user_directly(self, restore):
        assert is_bot_user({"user": {"name": "codiumai-bot"}}) is True
        assert is_bot_user({"user": {"name": "Alice"}}) is False


class TestAskLineBody:
    def _note(self, body="/ask what does this do?"):
        position = {
            "new_path": "src/main.py",
            "line_range": {"start": {"new_line": 3}, "end": {"new_line": 5}},
        }
        return FakeNote(11, body, PAST, note_type="DiffNote", position=position)

    def test_rewrite_through_shared_handler(self):
        note = self._note()
        note.discussion_id = "disc-42"
        body = gp._ask_line_body(note.body, note)
        assert body == ("/ask_line --line_start=3 --line_end=5 --side=RIGHT "
                        "--file_name=src/main.py --comment_id=disc-42 what does this do?")

    def test_only_first_ask_token_replaced(self):
        note = self._note("/ask what does /ask_line do?")
        assert gp._ask_line_body(note.body, note).endswith("what does /ask_line do?")

    def test_missing_position_returns_original(self):
        note = FakeNote(11, "/ask q", PAST)
        note.position = None
        assert gp._ask_line_body(note.body, note) == "/ask q"

    def test_webhook_handle_ask_line_replace_once(self):
        """The webhook's own handle_ask_line no longer mangles '/ask' in prose."""
        data = {"object_attributes": {
            "position": {
                "new_path": "a.py",
                "line_range": {"start": {"new_line": 1}, "end": {"new_line": 2}},
            },
            "discussion_id": "d1",
        }}
        body = handle_ask_line("/ask what does /ask_line do?", data)
        assert body.endswith("what does /ask_line do?")


# ---------------------------------------------------------------------------
# 4. Leader lock
# ---------------------------------------------------------------------------

class TestLeaderLock:
    def test_second_acquire_fails_until_released(self, tmp_path):
        lock_path = str(tmp_path / "poller.lock")
        first = gp._try_acquire_lock(lock_path)
        assert first is not None
        assert gp._try_acquire_lock(lock_path) is None
        first.close()
        third = None
        try:
            third = gp._try_acquire_lock(lock_path)
            assert third is not None
        finally:
            if third is not None:
                third.close()


# ---------------------------------------------------------------------------
# 5. Poll-cycle integration
# ---------------------------------------------------------------------------

class TestPollCycle:
    async def test_first_cycle_records_without_dispatching(self, tmp_path):
        note = FakeNote(1, "/review", PAST)  # predates the boot cursor
        gl = FakeGitlab({PROJECT: FakeProject([FakeMR(7, notes=[note])])})
        agent = await _run_cycle(gl, tmp_path)

        assert agent.handle_request.await_count == 0
        state = gp._load_json_state(gp._state_path(str(tmp_path), PROJECT))
        assert state["initialized"] is True
        assert state["mrs"]["7"]["sha"] == "sha1"

    async def test_new_command_dispatched_and_checkmarked(self, tmp_path):
        note = FakeNote(1, "/review", PAST)
        mr = FakeMR(7, notes=[note])
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)  # first cycle: freeze

        note.created_at = _future_iso()
        mr.updated_at = _future_iso()
        agent = await _run_cycle(gl, tmp_path)

        assert _dispatched_bodies(agent) == ["/review"]
        assert agent.handle_request.await_args_list[0].args[0] == mr.web_url
        assert "white_check_mark" in note.awardemojis.names()
        assert note.deleted is False  # webhook parity: comments stay by default

    async def test_processed_comment_not_redispatched(self, tmp_path):
        note = FakeNote(1, "/review", PAST)
        mr = FakeMR(7, notes=[note])
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)  # freeze

        note.created_at = _future_iso()
        mr.updated_at = _future_iso()
        agent1 = await _run_cycle(gl, tmp_path)
        assert _dispatched_bodies(agent1) == ["/review"]

        mr.updated_at = _future_iso(10)
        agent2 = await _run_cycle(gl, tmp_path)
        assert agent2.handle_request.await_count == 0  # checkmark dedupes

    async def test_failed_command_retried_then_checkmarked(self, tmp_path):
        note = FakeNote(1, "/review", PAST)
        mr = FakeMR(7, notes=[note])
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)  # freeze

        note.created_at = _future_iso()
        mr.updated_at = _future_iso()
        retry_budget = {}
        agent = _make_agent(side_effect=[False, True])
        await _run_cycle(gl, tmp_path, retry_budget=retry_budget, agent=agent)
        assert retry_budget == {f"{PROJECT}:7:1": 2}
        assert "white_check_mark" not in note.awardemojis.names()

        mr.updated_at = _future_iso(10)
        await _run_cycle(gl, tmp_path, retry_budget=retry_budget, agent=agent)
        assert agent.handle_request.await_count == 2
        assert retry_budget == {}
        assert "white_check_mark" in note.awardemojis.names()

    async def test_bot_comment_ignored(self, tmp_path):
        bot_author = {"username": "codiumai-bot", "name": "codiumai-bot", "id": 2}
        note = FakeNote(1, "/review", PAST, author=bot_author)
        mr = FakeMR(7, notes=[note])
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)  # freeze

        note.created_at = _future_iso()
        mr.updated_at = _future_iso()
        agent = await _run_cycle(gl, tmp_path)
        assert agent.handle_request.await_count == 0

    async def test_new_mr_runs_pr_commands(self, tmp_path):
        gl = FakeGitlab({PROJECT: FakeProject([])})
        await _run_cycle(gl, tmp_path)  # freeze with no MRs

        gl.projects = SimpleNamespace(get=lambda path: FakeProject([FakeMR(8, sha="shaX")]))
        agent = await _run_cycle(gl, tmp_path)
        assert _dispatched_bodies(agent) == PR_COMMAND_BODIES
        for call in agent.handle_request.await_args_list:
            assert call.args[0].endswith("/merge_requests/8")

    async def test_push_runs_push_commands_when_enabled(self, tmp_path):
        snapshot = snapshot_settings(["gitlab.handle_push_trigger"])
        try:
            get_settings().set("gitlab.handle_push_trigger", True)
            mr = FakeMR(7, sha="sha1")
            gl = FakeGitlab({PROJECT: FakeProject([mr])})
            await _run_cycle(gl, tmp_path)  # freeze

            mr.sha = "sha2"
            mr.updated_at = _future_iso()
            agent = await _run_cycle(gl, tmp_path)
            assert _dispatched_bodies(agent) == PUSH_COMMAND_BODIES
        finally:
            restore_settings(snapshot)

    async def test_push_skipped_when_trigger_disabled(self, tmp_path):
        mr = FakeMR(7, sha="sha1")  # handle_push_trigger defaults to false
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)

        mr.sha = "sha2"
        mr.updated_at = _future_iso()
        agent = await _run_cycle(gl, tmp_path)
        assert agent.handle_request.await_count == 0

    async def test_draft_to_ready_runs_pr_commands(self, tmp_path):
        mr = FakeMR(7, draft=True)
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)  # freeze: draft recorded

        mr.draft = False
        mr.work_in_progress = False
        mr.updated_at = _future_iso()
        agent = await _run_cycle(gl, tmp_path)
        assert _dispatched_bodies(agent) == PR_COMMAND_BODIES

    async def test_closed_mr_pruned_and_reopen_refires(self, tmp_path):
        mr = FakeMR(7)
        project = FakeProject([mr])
        gl = FakeGitlab({PROJECT: project})
        await _run_cycle(gl, tmp_path)

        project.mergerequests = FakeMRManager([])  # MR closed
        await _run_cycle(gl, tmp_path)
        state = gp._load_json_state(gp._state_path(str(tmp_path), PROJECT))
        assert state["mrs"] == {}

        project.mergerequests = FakeMRManager([FakeMR(7, sha="sha9")])  # reopened
        agent = await _run_cycle(gl, tmp_path)
        assert _dispatched_bodies(agent) == PR_COMMAND_BODIES

    async def test_bot_mr_recorded_without_commands(self, tmp_path):
        gl = FakeGitlab({PROJECT: FakeProject([])})
        await _run_cycle(gl, tmp_path)

        bot_author = {"username": "codiumai-bot", "name": "codiumai-bot", "id": 2}
        gl.projects = SimpleNamespace(get=lambda path: FakeProject([FakeMR(9, author=bot_author)]))
        agent = await _run_cycle(gl, tmp_path)
        assert agent.handle_request.await_count == 0
        state = gp._load_json_state(gp._state_path(str(tmp_path), PROJECT))
        assert "9" in state["mrs"]

    async def test_ignored_repo_runs_no_commands(self, tmp_path):
        snapshot = snapshot_settings(["CONFIG.IGNORE_REPOSITORIES"])
        try:
            get_settings().set("CONFIG.IGNORE_REPOSITORIES", ["^group/.*"])
            gl = FakeGitlab({PROJECT: FakeProject([])})
            await _run_cycle(gl, tmp_path)

            gl.projects = SimpleNamespace(get=lambda path: FakeProject([FakeMR(8, sha="shaX")]))
            agent = await _run_cycle(gl, tmp_path)
            assert agent.handle_request.await_count == 0
        finally:
            restore_settings(snapshot)

    async def test_untouched_mr_notes_not_fetched(self, tmp_path):
        mr = FakeMR(7)
        gl = FakeGitlab({PROJECT: FakeProject([mr])})
        await _run_cycle(gl, tmp_path)

        class ExplodingNotes:
            def list(self, **kwargs):
                raise AssertionError("notes.list must not be called for untouched MRs")

        mr.notes = ExplodingNotes()
        await _run_cycle(gl, tmp_path)  # same updated_at -> comment pass skips the MR

    async def test_cursor_survives_restart(self, tmp_path):
        """Comments created while the poller was down are delivered on the next cycle."""
        gl = FakeGitlab({PROJECT: FakeProject([FakeMR(7)])})
        await _run_cycle(gl, tmp_path)

        note = FakeNote(5, "/describe", _future_iso())
        mr = FakeMR(7, notes=[note], updated_at=_future_iso())
        gl.projects = SimpleNamespace(get=lambda path: FakeProject([mr]))
        agent = await _run_cycle(gl, tmp_path, retry_budget={})
        assert _dispatched_bodies(agent) == ["/describe"]


# ---------------------------------------------------------------------------
# 6. Dispatch to PRAgent
# ---------------------------------------------------------------------------

class TestDispatch:
    async def test_comment_dispatched_with_eyes_notify(self):
        eyes_added = []
        provider = SimpleNamespace(add_eyes_reaction=lambda comment_id: eyes_added.append(comment_id))
        agent = _make_agent()
        note = FakeNote(11, "/review", PAST)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(gp, "PRAgent", lambda: agent)
            mp.setattr(gp, "get_git_provider_with_context", lambda pr_url: provider)
            success = await gp._dispatch_command_comment(FakeMR(7), note, PROJECT)
        assert success is True
        call = agent.handle_request.await_args_list[0]
        assert call.args[0] == MR_URL
        assert call.args[1] == "/review"
        notify = call.kwargs["notify"]
        notify()
        assert eyes_added == [11]

    async def test_diffnote_ask_rewritten_to_ask_line(self):
        provider = SimpleNamespace(add_eyes_reaction=lambda comment_id: None)
        agent = _make_agent()
        position = {
            "new_path": "a.py",
            "line_range": {"start": {"new_line": 1}, "end": {"new_line": 2}},
        }
        note = FakeNote(11, "/ask why?", PAST, note_type="DiffNote", position=position)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(gp, "PRAgent", lambda: agent)
            mp.setattr(gp, "get_git_provider_with_context", lambda pr_url: provider)
            await gp._dispatch_command_comment(FakeMR(7), note, PROJECT)
        body = agent.handle_request.await_args_list[0].args[1]
        assert body.startswith("/ask_line --line_start=1 --line_end=2")
        assert body.endswith("why?")

    async def test_failed_command_returns_false(self):
        provider = SimpleNamespace(add_eyes_reaction=lambda comment_id: None)
        agent = _make_agent(side_effect=[False])
        note = FakeNote(11, "/review", PAST)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(gp, "PRAgent", lambda: agent)
            mp.setattr(gp, "get_git_provider_with_context", lambda pr_url: provider)
            success = await gp._dispatch_command_comment(FakeMR(7), note, PROJECT)
        assert success is False
