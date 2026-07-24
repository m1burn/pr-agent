import asyncio
import copy
import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import APIRouter, FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.background import BackgroundTasks
from starlette.middleware import Middleware
from starlette_context import context
from starlette_context.middleware import RawContextMiddleware

from pr_agent.agent.pr_agent import PRAgent
from pr_agent.algo.utils import update_settings_from_args
from pr_agent.config_loader import get_settings, global_settings
from pr_agent.git_providers.utils import apply_repo_settings
from pr_agent.log import LoggingFormat, get_logger, setup_logger
from pr_agent.secret_providers import get_secret_provider
from pr_agent.git_providers import get_git_provider_with_context

setup_logger(fmt=LoggingFormat.JSON, level=get_settings().get("CONFIG.LOG_LEVEL", "DEBUG"))
router = APIRouter()
_shutdown_event = asyncio.Event()

secret_provider = get_secret_provider() if get_settings().get("CONFIG.SECRET_PROVIDER") else None


async def handle_request(api_url: str, body: str, log_context: dict, sender_id: str, notify=None):
    log_context["action"] = body
    log_context["event"] = "pull_request" if body == "/review" else "comment"
    log_context["api_url"] = api_url
    log_context["app_name"] = get_settings().get("CONFIG.APP_NAME", "Unknown")

    with get_logger().contextualize(**log_context):
        await PRAgent().handle_request(api_url, body, notify)

async def _perform_commands_gitlab(commands_conf: str, agent: PRAgent, api_url: str,
                                   log_context: dict, data: dict):
    apply_repo_settings(api_url)
    if commands_conf == "pr_commands" and get_settings().config.disable_auto_feedback:  # auto commands for PR, and auto feedback is disabled
        get_logger().info(f"Auto feedback is disabled, skipping auto commands for PR {api_url=}", **log_context)
        return
    if not should_process_pr_logic(data): # Here we already updated the configurations
        return
    commands = get_settings().get(f"gitlab.{commands_conf}", {})
    get_settings().set("config.is_auto_command", True)
    for command in commands:
        try:
            split_command = command.split(" ")
            command = split_command[0]
            args = split_command[1:]
            other_args = update_settings_from_args(args)
            new_command = ' '.join([command] + other_args)
            get_logger().info(f"Performing command: {new_command}")
            with get_logger().contextualize(**log_context):
                await agent.handle_request(api_url, new_command)
        except Exception as e:
            get_logger().error(f"Failed to perform command {command}: {e}")


def is_bot_user(data) -> bool:
    try:
        # logic to ignore bot users (unlike Github, no direct flag for bot users in gitlab)
        sender_name = data.get("user", {}).get("name", "unknown").lower()
        # Indicators are sourced from config.bot_user_indicators in configuration.toml so the
        # default list has a single source of truth and can be reused by other providers in
        # the future. Normalize the value defensively: a misconfigured .pr_agent.toml (string
        # instead of list, non-string entries) should not break bot detection, and matching
        # is documented as case-insensitive.
        raw_indicators = get_settings().get("config.bot_user_indicators", [])
        if isinstance(raw_indicators, str):
            raw_indicators = [raw_indicators]
        try:
            raw_indicators = list(raw_indicators)
        except TypeError:
            get_logger().warning(
                f"Ignoring non-iterable gitlab.bot_user_indicators value: {raw_indicators!r}"
            )
            raw_indicators = []
        bot_indicators = [s.lower() for s in raw_indicators if isinstance(s, str)]
        if any(indicator in sender_name for indicator in bot_indicators):
            get_logger().info(f"Skipping GitLab bot user: {sender_name}")
            return True
    except Exception as e:
        get_logger().error(f"Failed 'is_bot_user' logic: {e}")
    return False

def is_draft(data) -> bool:
    try:
        if 'draft' in data.get('object_attributes', {}):
            return data['object_attributes']['draft']

        # for gitlab server version before 16
        elif 'Draft:' in data.get('object_attributes', {}).get('title'):
            return True
    except Exception as e:
        get_logger().error(f"Failed 'is_draft' logic: {e}")
    return False

def is_draft_ready(data) -> bool:
    try:
        if 'draft' in data.get('changes', {}):
            # Handle both boolean values and string values for compatibility
            previous = data['changes']['draft']['previous']
            current = data['changes']['draft']['current']

            # Convert to boolean if they're strings
            if isinstance(previous, str):
                previous = previous.lower() == 'true'
            if isinstance(current, str):
                current = current.lower() == 'true'

            if previous is True and current is False:
                return True

        # for gitlab server version before 16
        elif 'title' in data.get('changes', {}):
            if 'Draft:' in data['changes']['title']['previous'] and 'Draft:' not in data['changes']['title']['current']:
                return True
    except Exception as e:
        get_logger().error(f"Failed 'is_draft_ready' logic: {e}")
    return False

def should_process_pr_logic(data) -> bool:
    try:
        if not data.get('object_attributes', {}):
            return False
        title = data['object_attributes'].get('title')
        sender = data.get("user", {}).get("username", "")
        repo_full_name = data.get('project', {}).get('path_with_namespace', "")

        # logic to ignore PRs from specific repositories
        ignore_repos = get_settings().get("CONFIG.IGNORE_REPOSITORIES", [])
        if ignore_repos and repo_full_name:
            if any(re.search(regex, repo_full_name) for regex in ignore_repos):
                get_logger().info(f"Ignoring MR from repository '{repo_full_name}' due to 'config.ignore_repositories' setting")
                return False

        # logic to ignore PRs from specific users
        ignore_pr_users = get_settings().get("CONFIG.IGNORE_PR_AUTHORS", [])
        if ignore_pr_users and sender:
            if any(re.search(regex, sender) for regex in ignore_pr_users):
                get_logger().info(f"Ignoring PR from user '{sender}' due to 'config.ignore_pr_authors' settings")
                return False

        # logic to ignore MRs for titles, labels and source, target branches.
        ignore_mr_title = get_settings().get("CONFIG.IGNORE_PR_TITLE", [])
        ignore_mr_labels = get_settings().get("CONFIG.IGNORE_PR_LABELS", [])
        ignore_mr_source_branches = get_settings().get("CONFIG.IGNORE_PR_SOURCE_BRANCHES", [])
        ignore_mr_target_branches = get_settings().get("CONFIG.IGNORE_PR_TARGET_BRANCHES", [])

        #
        if ignore_mr_source_branches:
            source_branch = data['object_attributes'].get('source_branch')
            if any(re.search(regex, source_branch) for regex in ignore_mr_source_branches):
                get_logger().info(
                    f"Ignoring MR with source branch '{source_branch}' due to gitlab.ignore_mr_source_branches settings")
                return False

        if ignore_mr_target_branches:
            target_branch = data['object_attributes'].get('target_branch')
            if any(re.search(regex, target_branch) for regex in ignore_mr_target_branches):
                get_logger().info(
                    f"Ignoring MR with target branch '{target_branch}' due to gitlab.ignore_mr_target_branches settings")
                return False

        if ignore_mr_labels:
            labels = [label['title'] for label in data['object_attributes'].get('labels', [])]
            if any(label in ignore_mr_labels for label in labels):
                labels_str = ", ".join(labels)
                get_logger().info(f"Ignoring MR with labels '{labels_str}' due to gitlab.ignore_mr_labels settings")
                return False

        if ignore_mr_title:
            if any(re.search(regex, title) for regex in ignore_mr_title):
                get_logger().info(f"Ignoring MR with title '{title}' due to gitlab.ignore_mr_title settings")
                return False
    except Exception as e:
        get_logger().error(f"Failed 'should_process_pr_logic': {e}")
    return True


def _get_mr_head_sha(mr, project):
    """Return the head SHA of a merge request, fetching the full MR when the list endpoint omitted it.

    The GitLab list endpoint may return ``sha=None`` on some versions; in that case we fetch
    the full MR via ``project.mergerequests.get(mr.iid)``.  Exceptions are not swallowed —
    callers decide how to handle fetch failures.  Returns ``None`` when the SHA is absent
    even after fetching the full MR.
    """
    sha = getattr(mr, "sha", None)
    if sha:
        return sha
    full_mr = project.mergerequests.get(mr.iid)
    return getattr(full_mr, "sha", None) or None


def _build_gitlab_polling_payload(mr, project_path, head_sha):
    """Build a synthetic GitLab webhook ``data`` dict from a python-gitlab MR object.

    The returned dict contains exactly the fields consumed by ``should_process_pr_logic``,
    ``is_draft``, and ``is_bot_user`` so the polling path can reuse the webhook command
    runners unchanged.  No GitLab API calls are made inside this helper.
    """
    author = getattr(mr, "author", None)
    if not isinstance(author, dict):
        author = {}
    username = author.get("username", "") or ""
    name = author.get("name", "") or ""

    raw_labels = getattr(mr, "labels", None) or []
    labels = [{"title": label} for label in raw_labels if isinstance(label, str)]

    draft = getattr(mr, "draft", None) or getattr(mr, "work_in_progress", False)

    return {
        "object_attributes": {
            "iid": mr.iid,
            "title": getattr(mr, "title", "") or "",
            "source_branch": getattr(mr, "source_branch", "") or "",
            "target_branch": getattr(mr, "target_branch", "") or "",
            "draft": draft,
            "labels": labels,
            "url": getattr(mr, "web_url", "") or "",
            "last_commit": {"id": head_sha} if head_sha else {},
        },
        "user": {
            "username": username,
            "name": name,
        },
        "project": {
            "path_with_namespace": project_path,
        },
    }


def _parse_gitlab_datetime(value) -> datetime | None:
    """Parse an ISO 8601 timestamp from the GitLab API into an aware ``datetime``.

    GitLab returns timestamps such as ``2021-01-01T00:00:00.000Z``.  Returns
    ``None`` when the value is missing or cannot be parsed.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _should_auto_review_mr(
    processed_mrs,
    iid,
    head_sha,
    payload,
    handle_push_trigger,
    mr_created_at: datetime | None,
    app_startup: datetime,
) -> tuple[str | None, str]:
    """Decide whether a polled MR should trigger an auto-review command.

    Returns a ``(command, action)`` tuple where ``command`` is the settings key
    to look up (``"pr_commands"`` / ``"push_commands"``) or ``None``, and
    ``action`` is one of ``"persist"``, ``"remove"``, or ``"skip"`` describing
    how the caller should update ``processed_mrs``.

    The helper is pure: it never calls ``_perform_commands_gitlab`` and never
    mutates ``processed_mrs``.  A non-dict ``processed_mrs`` (corrupt state
    file) is treated as empty and a warning is logged.
    """
    if not isinstance(processed_mrs, dict):
        get_logger().warning(
            f"processed_mrs is not a dict (got {type(processed_mrs).__name__}); treating as empty"
        )
        processed_mrs = {}

    # (a) Draft MRs are removed from state so ready->draft->ready re-triggers pr_commands.
    if is_draft(payload):
        return None, "remove"

    # (b) No head SHA available — nothing to persist, skip this cycle.
    if not head_sha:
        return None, "skip"

    # (c) Bot-author MRs are persisted without running commands so they are not re-evaluated.
    if is_bot_user(payload):
        return None, "persist"

    key = str(iid)
    if key not in processed_mrs:
        # (d) Newly seen MR — run pr_commands only if it was created after the
        # application started.  Pre-existing or undated MRs are recorded as seen
        # so they are not re-evaluated on every cycle.
        if mr_created_at and mr_created_at > app_startup:
            return "pr_commands", "persist"
        if not mr_created_at:
            get_logger().info(
                f"Skipping auto-review for MR IID {iid}: missing created_at"
            )
        else:
            get_logger().info(
                f"Skipping auto-review for MR IID {iid}: created before "
                f"application startup ({mr_created_at.isoformat()} <= "
                f"{app_startup.isoformat()})"
            )
        return None, "persist"

    if processed_mrs[key] != head_sha:
        # (e)/(f) SHA changed since last cycle — run push_commands only when configured.
        if handle_push_trigger:
            return "push_commands", "persist"
        return None, "persist"

    # (g) Already reviewed at this SHA — skip.
    return None, "skip"


@router.post("/webhook")
async def gitlab_webhook(background_tasks: BackgroundTasks, request: Request):
    start_time = datetime.now()
    request_json = await request.json()
    context["settings"] = copy.deepcopy(global_settings)

    async def inner(data: dict):
        log_context = {"server_type": "gitlab_app"}
        get_logger().debug("Received a GitLab webhook")
        if request.headers.get("X-Gitlab-Token") and secret_provider:
            request_token = request.headers.get("X-Gitlab-Token")
            secret = secret_provider.get_secret(request_token)
            if not secret:
                get_logger().warning(f"Empty secret retrieved, request_token: {request_token}")
                return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED,
                                    content=jsonable_encoder({"message": "unauthorized"}))
            try:
                secret_dict = json.loads(secret)
                gitlab_token = secret_dict["gitlab_token"]
                log_context["token_id"] = secret_dict.get("token_name", secret_dict.get("id", "unknown"))
                context["settings"].gitlab.personal_access_token = gitlab_token
            except Exception as e:
                get_logger().error(f"Failed to validate secret {request_token}: {e}")
                return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content=jsonable_encoder({"message": "unauthorized"}))
        elif get_settings().get("GITLAB.SHARED_SECRET"):
            secret = get_settings().get("GITLAB.SHARED_SECRET")
            if not request.headers.get("X-Gitlab-Token") == secret:
                get_logger().error("Failed to validate secret")
                return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content=jsonable_encoder({"message": "unauthorized"}))
        else:
            get_logger().error("Failed to validate secret")
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content=jsonable_encoder({"message": "unauthorized"}))
        gitlab_token = get_settings().get("GITLAB.PERSONAL_ACCESS_TOKEN", None)
        if not gitlab_token:
            get_logger().error("No gitlab token found")
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content=jsonable_encoder({"message": "unauthorized"}))

        get_logger().info("GitLab data", artifact=data)
        sender = data.get("user", {}).get("username", "unknown")
        sender_id = data.get("user", {}).get("id", "unknown")

        # ignore bot users
        if is_bot_user(data):
            return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder({"message": "success"}))

        log_context["sender"] = sender
        if data.get('object_kind') == 'merge_request':
            # ignore MRs based on title, labels, source and target branches
            if not should_process_pr_logic(data):
                return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder({"message": "success"}))
            object_attributes = data.get('object_attributes', {})
            if object_attributes.get('action') in ['open', 'reopen']:
                url = object_attributes.get('url')
                get_logger().info(f"New merge request: {url}")
                if is_draft(data):
                    get_logger().info(f"Skipping draft MR: {url}")
                    return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder({"message": "success"}))

                await _perform_commands_gitlab("pr_commands", PRAgent(), url, log_context, data)

            # for push event triggered merge requests
            elif object_attributes.get('action') == 'update' and object_attributes.get('oldrev'):
                url = object_attributes.get('url')
                get_logger().info(f"New merge request: {url}")
                if is_draft(data):
                    get_logger().info(f"Skipping draft MR: {url}")
                    return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder({"message": "success"}))

                # Apply repo settings before checking push commands or handle_push_trigger
                apply_repo_settings(url)

                commands_on_push = get_settings().get(f"gitlab.push_commands", {})
                handle_push_trigger = get_settings().get(f"gitlab.handle_push_trigger", False)
                if not commands_on_push or not handle_push_trigger:
                    get_logger().info("Push event, but no push commands found or push trigger is disabled")
                    return JSONResponse(status_code=status.HTTP_200_OK,
                                        content=jsonable_encoder({"message": "success"}))

                get_logger().debug(f'A push event has been received: {url}')
                await _perform_commands_gitlab("push_commands", PRAgent(), url, log_context, data)
                
            # for draft to ready triggered merge requests
            elif object_attributes.get('action') == 'update' and is_draft_ready(data):
                url = object_attributes.get('url')
                get_logger().info(f"Draft MR is ready: {url}")

                # same as open MR
                await _perform_commands_gitlab("pr_commands", PRAgent(), url, log_context, data)

        elif data.get('object_kind') == 'note' and data.get('event_type') == 'note': # comment on MR
            if 'merge_request' in data:
                mr = data['merge_request']
                url = mr.get('url')
                comment_id = data.get('object_attributes', {}).get('id')
                provider = get_git_provider_with_context(pr_url=url)

                get_logger().info(f"A comment has been added to a merge request: {url}")
                body = data.get('object_attributes', {}).get('note')
                if data.get('object_attributes', {}).get('type') == 'DiffNote' and '/ask' in body: # /ask_line
                    body = handle_ask_line(body, data)

                await handle_request(url, body, log_context, sender_id, notify=lambda: provider.add_eyes_reaction(comment_id))

    background_tasks.add_task(inner, request_json)
    end_time = datetime.now()
    get_logger().info(f"Processing time: {end_time - start_time}", request=request_json)
    return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder({"message": "success"}))


def handle_ask_line(body, data):
    try:
        line_range_ = data['object_attributes']['position']['line_range']
        # if line_range_['start']['type'] == 'new':
        start_line = line_range_['start']['new_line']
        end_line = line_range_['end']['new_line']
        # else:
        #     start_line = line_range_['start']['old_line']
        #     end_line = line_range_['end']['old_line']
        question = body.replace('/ask', '').strip()
        path = data['object_attributes']['position']['new_path']
        side = 'RIGHT'  # if line_range_['start']['type'] == 'new' else 'LEFT'
        comment_id = data['object_attributes']["discussion_id"]
        get_logger().info("Handling line ")
        body = f"/ask_line --line_start={start_line} --line_end={end_line} --side={side} --file_name={path} --comment_id={comment_id} {question}"
    except Exception as e:
        get_logger().error(f"Failed to handle ask line comment: {e}")
    return body


@router.get("/")
async def root():
    return {"status": "ok"}

gitlab_url = get_settings().get("GITLAB.URL", None)
if not gitlab_url:
    raise ValueError("GITLAB.URL is not set")
get_settings().config.git_provider = "gitlab"
middleware = [Middleware(RawContextMiddleware)]


async def _run_gitlab_polling(startup_time: datetime):
    """Poll open MRs for new command comments and dispatch to PRAgent.

    Args:
        startup_time: The UTC timestamp captured when the application started.
                      Auto-review of "new" MRs is gated to those created later
                      than this moment.
    """
    import gitlab
    from pr_agent.servers.utils import _load_processed_comments, _save_processed_comments

    poll_interval = get_settings().get("GITLAB.POLLING_INTERVAL", 30)
    project_path = get_settings().get("GITLAB.PROJECT_PATH", "")
    data_dir = get_settings().get("GITLAB.POLLING_DATA_DIR", "/var/lib/pr-agent/poller")
    raw_indicators = get_settings().get("CONFIG.BOT_USER_INDICATORS", [])
    if isinstance(raw_indicators, str):
        raw_indicators = [raw_indicators]
    bot_indicators = [s.lower() for s in raw_indicators if isinstance(s, str)]

    if not project_path:
        get_logger().error("gitlab.project_path is required for polling")
        return

    processed_path = os.path.join(data_dir, "processed_comments.json")
    processed_comments = _load_processed_comments(processed_path)

    # MR-state persistence: {str(iid): head_sha} — pruned of closed MRs each cycle
    processed_mrs_path = os.path.join(data_dir, "processed_mrs.json")
    processed_mrs = _load_processed_comments(processed_mrs_path)

    gitlab_url = get_settings().get("GITLAB.URL", None)
    if not gitlab_url:
        get_logger().error("GitLab URL is not set in the config file")
        return
    gitlab_access_token = get_settings().get("GITLAB.PERSONAL_ACCESS_TOKEN", None)
    if not gitlab_access_token:
        get_logger().error("GitLab personal access token is not set in the config file")
        return
    auth_method = get_settings().get("GITLAB.AUTH_TYPE", "oauth_token")
    ssl_verify = get_settings().get("GITLAB.SSL_VERIFY", True)

    try:
        if auth_method == "oauth_token":
            gl_client = gitlab.Gitlab(
                url=gitlab_url,
                oauth_token=gitlab_access_token,
                ssl_verify=ssl_verify,
            )
        else:
            gl_client = gitlab.Gitlab(
                url=gitlab_url,
                private_token=gitlab_access_token,
                ssl_verify=ssl_verify,
            )
    except Exception as e:
        get_logger().exception(f"Failed to create GitLab client for polling: {e}")
        return

    get_logger().info(
        f"Starting GitLab MR polling for project '{project_path}' "
        f"with interval {poll_interval}s"
    )

    while not _shutdown_event.is_set():
        try:
            project = gl_client.projects.get(project_path)
            open_mrs = project.mergerequests.list(state='opened', get_all=True)

            # Auto-review pass: run before comment processing so new/updated MRs
            # receive pr_commands/push_commands even when no command comments exist.
            handle_push_trigger = get_settings().get("gitlab.handle_push_trigger", False)
            for mr in open_mrs:
                if _shutdown_event.is_set():
                    break
                try:
                    head_sha = _get_mr_head_sha(mr, project)
                    payload = _build_gitlab_polling_payload(mr, project_path, head_sha)
                    created_at_str = getattr(mr, "created_at", None)
                    mr_created_at = _parse_gitlab_datetime(created_at_str)
                    command, action = _should_auto_review_mr(
                        processed_mrs,
                        mr.iid,
                        head_sha,
                        payload,
                        handle_push_trigger,
                        mr_created_at,
                        startup_time,
                    )
                    if command in ("pr_commands", "push_commands"):
                        log_context = {
                            "server_type": "gitlab_app",
                            "action": command,
                            "event": "merge_request",
                            "api_url": mr.web_url,
                            "sender": payload.get("user", {}).get("username", "unknown"),
                        }
                        await _perform_commands_gitlab(
                            command, PRAgent(), mr.web_url, log_context, payload
                        )
                    # Apply action to processed_mrs after _perform_commands_gitlab returns.
                    # _should_auto_review_mr only returns "persist" when head_sha is truthy,
                    # so the guard doubles as type narrowing for the dict[str, str] value.
                    key = str(mr.iid)
                    if action == "persist" and head_sha:
                        processed_mrs[key] = head_sha
                    elif action == "remove":
                        processed_mrs.pop(key, None)
                    # "skip" leaves processed_mrs unchanged
                except Exception as e:
                    get_logger().exception(
                        f"Auto-review failed for MR {mr.web_url}: {e}"
                    )

            for mr in open_mrs:
                if _shutdown_event.is_set():
                    break

                mr_url = mr.web_url
                notes = mr.notes.list(per_page=20, page=1)

                for note in notes:
                    if _shutdown_event.is_set():
                        break

                    comment_id = str(note.id)

                    # Skip already-processed comments
                    if comment_id in processed_comments:
                        continue

                    # Skip bot users
                    author_name = getattr(note, 'author', {}).get('name', '').lower()
                    if any(indicator in author_name for indicator in bot_indicators):
                        continue

                    # Only process command comments (/-prefixed)
                    comment_body = note.body or ''
                    if not comment_body.strip().startswith('/'):
                        continue

                    # Handle DiffNote /ask -> /ask_line rewrite
                    # (mirrors webhook handle_ask_line)
                    if getattr(note, 'type', None) == 'DiffNote' and '/ask' in comment_body:
                        try:
                            position = note.position
                            line_range = position.get('line_range', {})
                            start_line = line_range.get('start', {}).get('new_line', 0)
                            end_line = line_range.get('end', {}).get('new_line', 0)
                            path = position.get('new_path', '')
                            side = 'RIGHT'
                            discussion_id = getattr(note, 'discussion_id', '')
                            question = comment_body.replace('/ask', '').strip()
                            comment_body = (
                                f"/ask_line --line_start={start_line} "
                                f"--line_end={end_line} --side={side} "
                                f"--file_name={path} "
                                f"--comment_id={discussion_id} {question}"
                            )
                        except Exception as e:
                            get_logger().warning(
                                f"Failed to rewrite DiffNote /ask "
                                f"for MR {mr_url}: {e}"
                            )
                            continue

                    # Mark as processed BEFORE dispatching
                    # (prevents duplicate processing)
                    processed_comments[comment_id] = datetime.now(timezone.utc).isoformat()
                    _save_processed_comments(processed_path, processed_comments)

                    try:
                        success = await PRAgent().handle_request(
                            mr_url,
                            comment_body,
                            notify=None,
                        )
                    except Exception as e:
                        get_logger().exception(
                            f"Failed to handle command for MR {mr_url}: {e}"
                        )
                        success = False

                    if success:
                        try:
                            note.delete()
                            get_logger().info(
                                f"Deleted processed comment {comment_id} "
                                f"from MR {mr_url}"
                            )
                        except Exception as e:
                            get_logger().warning(
                                f"Failed to delete comment {comment_id}: {e}"
                            )
                    else:
                        get_logger().warning(
                            f"Command failed for comment {comment_id}, "
                            f"leaving comment on MR {mr_url}"
                        )

            # Prune closed-MR IIDs from processed_mrs state and persist once per cycle
            open_iids = {str(mr.iid) for mr in open_mrs}
            processed_mrs = {
                iid: sha for iid, sha in processed_mrs.items() if iid in open_iids
            }
            _save_processed_comments(processed_mrs_path, processed_mrs)

        except Exception as e:
            get_logger().exception(f"Error during polling cycle: {e}")

        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=poll_interval)
        except asyncio.TimeoutError:
            pass  # Normal poll interval elapsed


@asynccontextmanager
async def lifespan(app: FastAPI):
    poller_task = None
    if get_settings().get("GITLAB.POLLING_ENABLED", False):
        # Enforce single-worker when polling is enabled
        workers = os.environ.get("GUNICORN_WORKERS", "")
        if workers and int(workers) > 1:
            get_logger().error(
                "gitlab.polling_enabled requires GUNICORN_WORKERS=1. "
                f"Current: {workers}"
            )
            raise RuntimeError(
                "GUNICORN_WORKERS must be 1 when polling is enabled"
            )

        app_startup = datetime.now(timezone.utc)
        poller_task = asyncio.create_task(_run_gitlab_polling(app_startup))
    yield
    if poller_task:
        _shutdown_event.set()
        try:
            await asyncio.wait_for(poller_task, timeout=60)
        except asyncio.TimeoutError:
            get_logger().warning(
                "Polling loop did not shut down within 60s, cancelling"
            )
            poller_task.cancel()


app = FastAPI(middleware=middleware, lifespan=lifespan)
app.include_router(router)


def start():
    """
    Start the GitLab webhook server.

    The server port can be configured via the PORT environment variable.
    Defaults to 3000 if PORT is not set or invalid.
    """
    raw_port = os.environ.get("PORT")
    try:
        port = int(raw_port) if raw_port else 3000
        if not (1 <= port <= 65535):
            raise ValueError(f"Port {port} is out of valid range")
        if raw_port:
            get_logger().info(f"Using custom PORT from environment: {port}")
    except ValueError as e:
        get_logger().warning(f"Invalid PORT environment variable ({e}), using default port 3000")
        port = 3000
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == '__main__':
    start()
