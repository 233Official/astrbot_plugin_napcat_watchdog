"""Access token initialization and persistence logic.

This module is deliberately kept free of AstrBot framework imports
so it can be unit-tested without the full plugin runtime.
"""

from __future__ import annotations

import logging
import secrets

_logger = logging.getLogger(__name__)


def ensure_access_token(
    config: object,
    _log: logging.Logger | None = None,
) -> str:
    """Ensure ``access_token`` is set, persisted, and return it.

    Parameters
    ----------
    config :
        An object providing ``get(key, default)``, ``__setitem__`` and
        ``save_config()`` — matches the subset of ``AstrBotConfig`` used
        by this plugin.

    Returns
    -------
    str
        The (possibly newly generated) access token.

    Raises
    ------
    RuntimeError
        When the token was empty, generation succeeded but
        :meth:`save_config` raised an exception.  In that case the
        in-memory token is reset to ``""`` so the caller can safely
        refuse to start.
    """
    log = _log or _logger
    token: str = config.get("access_token", "")  # type: ignore[union-attr]

    if not token:
        token = secrets.token_urlsafe(32)
        config["access_token"] = token  # type: ignore[index]
        try:
            config.save_config()  # type: ignore[union-attr]
        except Exception:
            log.warning("无法持久化 access_token 到配置文件，WS 服务不会启动")
            config["access_token"] = ""  # type: ignore[index]
            raise RuntimeError("access_token persistence failed") from None

    return token
