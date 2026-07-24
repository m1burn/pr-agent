import hashlib
import hmac
import json
import os
import time
from collections import defaultdict
from typing import Any, Callable

from fastapi import HTTPException


def verify_signature(payload_body, secret_token, signature_header):
    """Verify that the payload was sent from GitHub by validating SHA256.

    Raise and return 403 if not authorized.

    Args:
        payload_body: original request body to verify (request.body())
        secret_token: GitHub app webhook token (WEBHOOK_SECRET)
        signature_header: header received from GitHub (x-hub-signature-256)
    """
    if not signature_header:
        raise HTTPException(status_code=403, detail="x-hub-signature-256 header is missing!")
    hash_object = hmac.new(secret_token.encode('utf-8'), msg=payload_body, digestmod=hashlib.sha256)
    expected_signature = "sha256=" + hash_object.hexdigest()
    if not hmac.compare_digest(expected_signature, signature_header):
        raise HTTPException(status_code=403, detail="Request signatures didn't match!")


class RateLimitExceeded(Exception):
    """Raised when the git provider API rate limit has been exceeded."""
    pass


class DefaultDictWithTimeout(defaultdict):
    """A defaultdict with a time-to-live (TTL)."""

    def __init__(
        self,
        default_factory: Callable[[], Any] = None,
        ttl: int = None,
        refresh_interval: int = 60,
        update_key_time_on_get: bool = True,
        *args,
        **kwargs,
    ):
        """
        Args:
            default_factory: The default factory to use for keys that are not in the dictionary.
            ttl: The time-to-live (TTL) in seconds.
            refresh_interval: How often to refresh the dict and delete items older than the TTL.
            update_key_time_on_get: Whether to update the access time of a key also on get (or only when set).
        """
        super().__init__(default_factory, *args, **kwargs)
        self.__key_times = dict()
        self.__ttl = ttl
        self.__refresh_interval = refresh_interval
        self.__update_key_time_on_get = update_key_time_on_get
        self.__last_refresh = self.__time() - self.__refresh_interval

    @staticmethod
    def __time():
        return time.monotonic()

    def __refresh(self):
        if self.__ttl is None:
            return
        request_time = self.__time()
        if request_time - self.__last_refresh < self.__refresh_interval:
            return
        to_delete = [key for key, key_time in self.__key_times.items() if request_time - key_time > self.__ttl]
        for key in to_delete:
            del self[key]
        self.__last_refresh = request_time

    def __getitem__(self, __key):
        if self.__update_key_time_on_get:
            self.__key_times[__key] = self.__time()
        self.__refresh()
        return super().__getitem__(__key)

    def __setitem__(self, __key, __value):
        self.__key_times[__key] = self.__time()
        return super().__setitem__(__key, __value)

    def __delitem__(self, __key):
        del self.__key_times[__key]
        return super().__delitem__(__key)


def _load_processed_comments(path: str) -> dict[str, str]:
    """Load processed-comments state from a JSON file.

    Args:
        path: Filesystem path to the JSON file holding the processed-comments map.

    Returns:
        The deserialized mapping of comment IDs to ISO timestamps. Returns an empty
        dict if the file does not exist or cannot be parsed.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _save_processed_comments(path: str, processed: dict[str, str]) -> None:
    """Persist processed-comments state to a JSON file atomically.

    Writes to a sibling ``.tmp`` file first and then ``os.replace``s it onto the
    target path so concurrent readers never observe a partially written file.
    If the mapping exceeds 10000 entries, the oldest entries (by ISO timestamp
    value) are evicted so only the 10000 newest remain.

    Args:
        path: Destination file path. Parent directories are created if missing.
        processed: Mapping of comment IDs to ISO timestamps to persist.
    """
    if len(processed) > 10000:
        sorted_items = sorted(processed.items(), key=lambda kv: kv[1])
        processed = dict(sorted_items[-10000:])

    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(processed, f, indent=2)
    os.replace(tmp_path, path)
