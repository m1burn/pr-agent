"""Standalone GitLab polling server - a transport alternative to webhooks.

Reuses the webhook server's logic read-only (``_perform_commands_gitlab``,
``is_bot_user``, ``handle_ask_line`` from pr_agent.servers.gitlab_webhook)
and dispatches comments to PRAgent directly, so PR-Agent behaves
identically no matter how events arrive. Polling only *transports* events.

Run directly (see the ``gitlab_polling`` Docker target):

    python pr_agent/servers/gitlab_polling.py

Multiple instances may be started (e.g. several workers or replicas): only
one holds the file lock under ``polling_data_dir`` at a time, the rest
stand by and take over if the holder exits.

Processed-comment deduplication is stored in GitLab itself: a comment that
PR-Agent picked up carries the bot's ``eyes`` reaction (added via the
notify callback), and a successfully processed one additionally carries
``white_check_mark``. A comment that failed carries only ``eyes`` and is
retried a bounded number of times; deleting the ``eyes`` reaction on the MR
re-queues it. Because the marker lives on the comment, a comment delivered
via webhook is never re-delivered by the poller and vice versa.
"""

import asyncio
import copy
import fcntl
import json
import os
import signal
import threading
from datetime import datetime, timezone

import gitlab
import uvicorn
from starlette_context import request_cycle_context

from pr_agent.agent.pr_agent import PRAgent
from pr_agent.config_loader import get_settings, global_settings
from pr_agent.git_providers import get_git_provider_with_context
from pr_agent.git_providers.utils import apply_repo_settings
from pr_agent.log import LoggingFormat, get_logger, setup_logger
from pr_agent.servers.gitlab_webhook import (_perform_commands_gitlab,
                                             handle_ask_line, is_bot_user)

setup_logger(fmt=LoggingFormat.JSON, level=get_settings().get("CONFIG.LOG_LEVEL", "DEBUG"))

_EYES_EMOJI = "eyes"  # added by PR-Agent itself when a command starts (notify callback)
_DONE_EMOJI = "white_check_mark"  # added by the poller after successful processing
_NOTES_PAGE_SIZE = 100


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value) -> datetime | None:
    """Parse a GitLab API timestamp into an aware datetime; None when missing/unparseable."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _project_slug(project_path: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in project_path)


def _state_path(data_dir: str, project_path: str) -> str:
    return os.path.join(data_dir, f"{_project_slug(project_path)}.state.json")


def _polling_projects() -> list[str]:
    """Projects to poll, from gitlab.polling_projects."""
    projects = get_settings().get("gitlab.polling_projects", []) or []
    if isinstance(projects, str):
        projects = [projects]
    return [p.strip() for p in projects if isinstance(p, str) and p.strip()]


def _load_json_state(path: str) -> dict:
    """Load a JSON state file; empty dict when missing/corrupt/not an object."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_json_state(path: str, state: dict) -> None:
    """Persist a JSON state file atomically (tmp file + os.replace)."""
    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp_path, path)


def _try_acquire_lock(lock_path: str):
    """Take an exclusive non-blocking flock; return the open file holding it, or None.

    The caller must keep the returned file object open for the lock to remain
    held; closing it (or process exit) releases the lock.
    """
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    fd = open(lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fd.close()
        return None
    fd.seek(0)
    fd.truncate()
    fd.write(str(os.getpid()))
    fd.flush()
    return fd


# ---------------------------------------------------------------------------
# Webhook-payload probes (feed the webhook server's own helpers)
# ---------------------------------------------------------------------------

def _author_name(obj) -> str:
    author = getattr(obj, "author", None)
    return author.get("name", "") if isinstance(author, dict) else ""


def _is_bot(obj) -> bool:
    """Bot detection via the webhook server's is_bot_user on a minimal probe."""
    return is_bot_user({"user": {"name": _author_name(obj)}})


def _resolve_draft(mr) -> bool:
    return bool(getattr(mr, "draft", None) or getattr(mr, "work_in_progress", False))


def _build_mr_payload(mr, project_path: str, sha: str | None) -> dict:
    """Webhook-shaped merge_request payload for the shared webhook helpers.

    Contains the fields consumed by should_process_pr_logic (called inside
    _perform_commands_gitlab) and is_bot_user.
    """
    author = getattr(mr, "author", None)
    if not isinstance(author, dict):
        author = {}
    return {
        "object_kind": "merge_request",
        "user": {
            "username": author.get("username", "") or "",
            "name": author.get("name", "") or "",
        },
        "project": {"path_with_namespace": project_path},
        "object_attributes": {
            "iid": mr.iid,
            "title": getattr(mr, "title", "") or "",
            "source_branch": getattr(mr, "source_branch", "") or "",
            "target_branch": getattr(mr, "target_branch", "") or "",
            "draft": _resolve_draft(mr),
            "labels": [{"title": label} for label in (getattr(mr, "labels", None) or [])
                       if isinstance(label, str)],
            "url": getattr(mr, "web_url", "") or "",
            "last_commit": {"id": sha} if sha else {},
        },
    }


def _ask_line_body(body: str, note) -> str:
    """Rewrite a DiffNote /ask via the webhook's handle_ask_line (position probe)."""
    probe = {"object_attributes": {
        "position": getattr(note, "position", None),
        "discussion_id": getattr(note, "discussion_id", None),
    }}
    return handle_ask_line(body, probe)


# ---------------------------------------------------------------------------
# Pure decision helpers (unit-tested without any GitLab objects)
# ---------------------------------------------------------------------------

def _decide_mr_event(prev: dict | None, entry: dict, *, bot: bool, initialized: bool) -> str | None:
    """Map an MR's state transition to an event kind.

    Pure: returns one of "open", "push", "draft_ready" or None. ``entry`` is
    the state the caller persists for the MR regardless of the decision, so a
    skipped MR is never re-evaluated every cycle.

    - First-ever poll cycle (not initialized): record everything, fire nothing.
    - Newly seen MR (including reopened ones): "open", unless draft/bot/no-sha.
    - Was draft, now ready: "draft_ready".
    - Head SHA changed: "push".
    """
    if prev is None:
        if not initialized or bot or entry["draft"] or not entry["sha"]:
            return None
        return "open"
    if entry["draft"]:
        return None
    if prev.get("draft"):
        return "draft_ready"
    prev_sha = prev.get("sha")
    if entry["sha"] and prev_sha and entry["sha"] != prev_sha:
        return "push"
    return None


def _should_deliver_note(body: str, created_at: datetime | None, own_emojis: set,
                         cursor: datetime, retries_left: int) -> bool:
    """Decide whether a comment should be (re-)delivered to the agent.

    Only ``/``-prefixed comments created at or after the cursor qualify. A
    comment carrying our ``white_check_mark`` was processed; one carrying only
    ``eyes`` failed earlier and is retried only while it has retry budget.
    """
    if not body or not body.strip().startswith("/"):
        return False
    if created_at is None or created_at < cursor:
        return False
    if _DONE_EMOJI in own_emojis:
        return False
    if _EYES_EMOJI in own_emojis:
        return retries_left > 0
    return True


# ---------------------------------------------------------------------------
# GitLab API helpers (all blocking; called via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _own_emoji_names(note, our_user_id) -> set:
    """Emoji names our bot user awarded on a note; empty on any failure."""
    names = set()
    try:
        for award in note.awardemojis.list(get_all=True):
            user = getattr(award, "user", None)
            uid = user.get("id") if isinstance(user, dict) else getattr(user, "id", None)
            if uid == our_user_id:
                names.add(getattr(award, "name", ""))
    except Exception as e:
        get_logger().warning(f"Failed to fetch award emojis for note {note.id}: {e}")
    return names


def _add_award(note, name: str) -> None:
    try:
        note.awardemojis.create({"name": name})
    except Exception as e:
        get_logger().warning(f"Failed to add '{name}' reaction to note {note.id}: {e}")


def _get_mr_head_sha(mr, project) -> str | None:
    """Head SHA of an MR, fetching the full MR when the list endpoint omitted it."""
    sha = getattr(mr, "sha", None)
    if sha:
        return sha
    full_mr = project.mergerequests.get(mr.iid)
    return getattr(full_mr, "sha", None) or None


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

async def _dispatch_mr_event(kind: str, mr, project_path: str, sha: str | None) -> None:
    """Run the auto-review commands for a detected MR transition.

    Uses the webhook server's own _perform_commands_gitlab (which applies
    repo settings, honors disable_auto_feedback, and re-checks the ignore
    filters via should_process_pr_logic). Mirrors the webhook route's
    merge_request branch: open/reopen and draft-ready run
    gitlab.pr_commands; a head-SHA change runs gitlab.push_commands only
    when push triggers are enabled. Each event is handled in a fresh
    settings context so repo-specific overrides never leak into the next
    event (same as the webhook's per-request isolation).
    """
    url = getattr(mr, "web_url", "") or ""
    data = _build_mr_payload(mr, project_path, sha)
    log_context = {"server_type": "gitlab_poller", "project": project_path,
                   "action": kind, "event": "merge_request", "api_url": url}
    with request_cycle_context({"settings": copy.deepcopy(global_settings)}):
        if kind == "push":
            apply_repo_settings(url)
            if not get_settings().get("gitlab.push_commands", {}) or \
                    not get_settings().get("gitlab.handle_push_trigger", False):
                get_logger().info("Push event, but no push commands found or push trigger is disabled")
                return
            await _perform_commands_gitlab("push_commands", PRAgent(), url, log_context, data)
        else:  # "open" and "draft_ready" behave like an opened MR
            await _perform_commands_gitlab("pr_commands", PRAgent(), url, log_context, data)


async def _dispatch_command_comment(mr, note, project_path: str) -> bool:
    """Run one command comment through PRAgent; True on success.

    Mirrors the webhook server's note branch, including the eyes-reaction
    notify callback and the DiffNote /ask -> /ask_line rewrite.
    """
    url = getattr(mr, "web_url", "") or ""
    body = getattr(note, "body", "") or ""
    if getattr(note, "type", None) == "DiffNote" and "/ask" in body:
        body = _ask_line_body(body, note)
    author = getattr(note, "author", None)
    sender = author.get("username", "unknown") if isinstance(author, dict) else "unknown"
    log_context = {"server_type": "gitlab_poller", "project": project_path,
                   "action": body, "event": "comment", "api_url": url, "sender": sender}
    with request_cycle_context({"settings": copy.deepcopy(global_settings)}):
        provider = await asyncio.to_thread(get_git_provider_with_context, pr_url=url)
        with get_logger().contextualize(**log_context):
            return bool(await PRAgent().handle_request(
                url, body, notify=lambda: provider.add_eyes_reaction(note.id)))


# ---------------------------------------------------------------------------
# Polling core
# ---------------------------------------------------------------------------

async def _poll_project(gl, project_path: str, data_dir: str, our_user_id,
                        retry_budget: dict, max_retries: int) -> None:
    """Run one poll cycle for a single project."""
    state_file = _state_path(data_dir, project_path)
    state = _load_json_state(state_file)
    initialized = bool(state.get("initialized"))
    cycle_cursor = _parse_ts(state.get("notes_cursor")) or _now_utc()
    new_cursor = cycle_cursor
    mrs_state = state.get("mrs") if isinstance(state.get("mrs"), dict) else {}

    project = await asyncio.to_thread(gl.projects.get, project_path)
    open_mrs = await asyncio.to_thread(project.mergerequests.list, state="opened", get_all=True)

    def _persist() -> None:
        _save_json_state(state_file, {
            "initialized": True,
            "notes_cursor": new_cursor.isoformat(),
            "mrs": new_mrs_state,
        })

    # --- MR pass: auto-review for opened/pushed/readied MRs ---
    new_mrs_state = {}
    for mr in open_mrs:
        iid = str(mr.iid)
        prev = mrs_state.get(iid)
        if not isinstance(prev, dict):
            prev = None
        try:
            sha = await asyncio.to_thread(_get_mr_head_sha, mr, project)
        except Exception as e:
            get_logger().warning(f"Failed to resolve head SHA for MR {mr.web_url}: {e}")
            sha = None
        entry = {
            "sha": sha or (prev or {}).get("sha"),
            "draft": _resolve_draft(mr),
            "updated_at": getattr(mr, "updated_at", None),
        }
        kind = _decide_mr_event(prev, entry, bot=_is_bot(mr), initialized=initialized)
        if kind:
            get_logger().info(f"Polled MR transition '{kind}' for {mr.web_url}")
            try:
                await _dispatch_mr_event(kind, mr, project_path, entry["sha"])
            except Exception as e:
                get_logger().exception(f"Failed to dispatch {kind} event for MR {mr.web_url}: {e}")
        new_mrs_state[iid] = entry  # closed MRs vanish from open_mrs and are pruned here
    if not initialized:
        get_logger().info(f"First poll cycle for '{project_path}': recorded {len(new_mrs_state)} "
                          f"open MRs without triggering reviews")
    _persist()

    # --- Comment pass: deliver new command comments ---
    for mr in open_mrs:
        iid = str(mr.iid)
        prev = mrs_state.get(iid) or {}
        prev_updated = _parse_ts(prev.get("updated_at"))
        mr_updated = _parse_ts(getattr(mr, "updated_at", None))
        retry_prefix = f"{project_path}:{iid}:"
        has_pending_retry = any(k.startswith(retry_prefix) and v > 0 for k, v in retry_budget.items())
        # Comments always bump the MR's updated_at, so an untouched MR cannot have new ones.
        if initialized and prev_updated and mr_updated and mr_updated <= prev_updated and not has_pending_retry:
            continue
        try:
            notes = await asyncio.to_thread(mr.notes.list, order_by="updated_at", sort="desc",
                                            per_page=_NOTES_PAGE_SIZE, get_all=False)
        except Exception as e:
            get_logger().exception(f"Failed to fetch notes for MR {mr.web_url}: {e}")
            continue

        for note in notes:
            created = _parse_ts(getattr(note, "created_at", None))
            if created and created > new_cursor:
                new_cursor = created

        candidates = []
        for note in notes:
            body = getattr(note, "body", "") or ""
            created = _parse_ts(getattr(note, "created_at", None))
            retry_id = f"{project_path}:{iid}:{note.id}"
            if not body.strip().startswith("/") or created is None or created < cycle_cursor \
                    or _is_bot(note):
                # Never fetch award emojis for comments that can never qualify.
                if retry_budget.get(retry_id, 0) > 0:
                    retry_budget.pop(retry_id, None)
                continue
            own_emojis = await asyncio.to_thread(_own_emoji_names, note, our_user_id)
            if _should_deliver_note(body, created, own_emojis, cycle_cursor, retry_budget.get(retry_id, 0)):
                candidates.append((created, note, retry_id))

        for created, note, retry_id in sorted(candidates, key=lambda c: c[0]):
            if retry_budget.get(retry_id, 0) > 0:
                retry_budget[retry_id] -= 1
                get_logger().info(f"Retrying failed command comment {note.id} on MR {mr.web_url}")
            try:
                success = await _dispatch_command_comment(mr, note, project_path)
            except Exception as e:
                get_logger().exception(f"Failed to dispatch command comment {note.id} on MR {mr.web_url}: {e}")
                success = False
            if success:
                retry_budget.pop(retry_id, None)
                await asyncio.to_thread(_add_award, note, _DONE_EMOJI)
            else:
                retry_budget[retry_id] = max_retries
                get_logger().warning(f"Command comment {note.id} on MR {mr.web_url} failed; "
                                     f"{max_retries} retries remaining. Remove the 'eyes' reaction to re-queue it.")

    _persist()


async def polling_loop() -> None:
    projects = _polling_projects()
    if not projects:
        raise ValueError("Set gitlab.polling_projects to enable polling")
    gitlab_url = get_settings().get("GITLAB.URL")
    token = get_settings().get("GITLAB.PERSONAL_ACCESS_TOKEN")
    if not gitlab_url or not token:
        raise ValueError("GITLAB.URL and GITLAB.PERSONAL_ACCESS_TOKEN are required for polling")
    get_settings().config.git_provider = "gitlab"
    poll_interval = int(get_settings().get("gitlab.polling_interval", 30))
    data_dir = get_settings().get("gitlab.polling_data_dir", "/var/lib/pr-agent-poller")
    max_retries = int(get_settings().get("gitlab.polling_max_comment_retries", 2))

    auth_method = get_settings().get("GITLAB.AUTH_TYPE", "oauth_token")
    ssl_verify = get_settings().get("GITLAB.SSL_VERIFY", True)
    credentials = {"oauth_token": token} if auth_method == "oauth_token" else {"private_token": token}
    gl = gitlab.Gitlab(url=gitlab_url, ssl_verify=ssl_verify, **credentials)
    await asyncio.to_thread(gl.auth)
    our_user_id = gl.user.id

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-Unix platforms
            pass

    lock_path = os.path.join(data_dir, "gitlab_polling.lock")
    lock_fd = None
    retry_budget: dict[str, int] = {}
    get_logger().info(f"Starting GitLab polling for projects {projects} (interval {poll_interval}s, "
                      f"state dir {data_dir})")
    try:
        while not stop.is_set():
            if lock_fd is None:
                lock_fd = _try_acquire_lock(lock_path)
                if lock_fd is None:
                    get_logger().info("Another process holds the polling lock; standing by")
                else:
                    get_logger().info(f"Acquired the polling lock (pid {os.getpid()}); active poller")
            if lock_fd is not None:
                for project_path in projects:
                    if stop.is_set():
                        break
                    try:
                        await _poll_project(gl, project_path, data_dir, our_user_id,
                                            retry_budget, max_retries)
                    except Exception as e:
                        get_logger().exception(f"Polling cycle failed for project '{project_path}': {e}")
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass
    finally:
        if lock_fd is not None:
            lock_fd.close()
    get_logger().info("GitLab polling stopped")


async def _health_app(scope, receive, send):
    """Minimal ASGI health endpoint: GET / -> {"status": "ok"}."""
    if scope["type"] != "http":
        return
    await receive()
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/json"]],
    })
    await send({
        "type": "http.response.body",
        "body": b'{"status": "ok"}',
    })


def start_health_server(host: str = "0.0.0.0", port: int | None = None) -> None:
    """Start a lightweight HTTP health endpoint in a daemon thread.

    Provides GET / -> 200 OK {"status": "ok"} for health checks. The poller
    has no HTTP surface of its own.
    """
    port = port if port is not None else int(os.environ.get("PORT", "3000"))
    config = uvicorn.Config(_health_app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()


if __name__ == "__main__":
    start_health_server()
    asyncio.run(polling_loop())
