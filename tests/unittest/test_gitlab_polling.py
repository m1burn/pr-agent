"""Unit tests for the GitLab polling server's atomic state persistence.

``_save_json_state`` writes through a tmp file + ``os.replace`` and retries
transient rename failures (seen on network mounts such as the App Service
/home share) with a fresh unique tmp name before giving up.
"""

import errno
import json
import os
import time

import pytest

from pr_agent.servers import gitlab_polling


@pytest.fixture
def sleep_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(time, "sleep", calls.append)
    return calls


def test_save_json_state_roundtrips_and_leaves_no_tmp(tmp_path):
    state_file = tmp_path / "project.state.json"

    gitlab_polling._save_json_state(str(state_file), {"a": 1, "mrs": {"3": {"sha": "x"}}})

    assert json.loads(state_file.read_text(encoding="utf-8")) == {"a": 1, "mrs": {"3": {"sha": "x"}}}
    assert list(tmp_path.iterdir()) == [state_file]


def test_save_json_state_retries_transient_rename_failures(sleep_calls, tmp_path, monkeypatch):
    state_file = tmp_path / "project.state.json"
    attempts = []
    real_replace = os.replace

    def flaky_replace(src, dst):
        attempts.append(src)
        if len(attempts) < 3:
            raise FileNotFoundError(errno.ENOENT, "no such file or directory", src)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)

    gitlab_polling._save_json_state(str(state_file), {"ok": True})

    assert json.loads(state_file.read_text(encoding="utf-8")) == {"ok": True}
    assert len(attempts) == 3
    assert len(set(attempts)) == 3  # a fresh unique tmp name per attempt
    assert sleep_calls == [gitlab_polling._SAVE_RETRY_BACKOFF_SECS, gitlab_polling._SAVE_RETRY_BACKOFF_SECS * 2]
    assert [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


@pytest.mark.parametrize("err", [errno.ENOENT, errno.EACCES, errno.EBUSY, errno.EXDEV])
def test_save_json_state_retries_retryable_errnos(sleep_calls, tmp_path, monkeypatch, err):
    state_file = tmp_path / "project.state.json"
    calls = []
    real_replace = os.replace

    def flaky_replace(src, dst):
        calls.append(src)
        if len(calls) < 2:
            raise OSError(err, "transient", src, None, dst)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)

    gitlab_polling._save_json_state(str(state_file), {"ok": True})

    assert json.loads(state_file.read_text(encoding="utf-8")) == {"ok": True}
    assert len(calls) == 2
    assert len(sleep_calls) == 1
    assert [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


def test_save_json_state_gives_up_after_final_retry(sleep_calls, tmp_path, monkeypatch):
    calls = []

    def always_fail(src, dst):
        calls.append(src)
        raise FileNotFoundError(errno.ENOENT, "no such file or directory", src)

    monkeypatch.setattr(os, "replace", always_fail)

    with pytest.raises(FileNotFoundError):
        gitlab_polling._save_json_state(str(tmp_path / "project.state.json"), {"k": 1})

    assert len(calls) == gitlab_polling._SAVE_RETRY_ATTEMPTS
    assert sleep_calls == [gitlab_polling._SAVE_RETRY_BACKOFF_SECS, gitlab_polling._SAVE_RETRY_BACKOFF_SECS * 2]
    assert [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


def test_save_json_state_non_retryable_errno_fails_fast(sleep_calls, tmp_path, monkeypatch):
    calls = []

    def io_error(src, dst):
        calls.append(src)
        raise OSError(errno.EIO, "permanent", src, None, dst)

    monkeypatch.setattr(os, "replace", io_error)

    with pytest.raises(OSError) as excinfo:
        gitlab_polling._save_json_state(str(tmp_path / "project.state.json"), {"k": 1})

    assert excinfo.value.errno == errno.EIO
    assert len(calls) == 1
    assert sleep_calls == []
    assert [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


def test_save_json_state_retries_when_tmp_create_fails(sleep_calls, tmp_path, monkeypatch):
    state_file = tmp_path / "sub" / "project.state.json"
    calls = []
    real_open = open

    def flaky_open(file, mode="r", **kwargs):
        if str(file).endswith(".tmp") and not calls:
            calls.append(file)
            raise FileNotFoundError(errno.ENOENT, "dir vanished", file)
        return real_open(file, mode, **kwargs)

    monkeypatch.setattr("builtins.open", flaky_open)

    gitlab_polling._save_json_state(str(state_file), {"ok": True})

    assert json.loads(state_file.read_text(encoding="utf-8")) == {"ok": True}
    assert len(calls) == 1
    assert len(sleep_calls) == 1
