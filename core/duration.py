"""Shared human-readable duration formatting."""

from __future__ import annotations

import math
from typing import Any


def format_natural_duration(seconds: Any) -> str:
    """Format a duration using whole natural Chinese day/hour/minute units."""
    try:
        value = float(seconds)
    except (TypeError, ValueError, OverflowError):
        value = 0.0
    if not math.isfinite(value) or value < 0:
        value = 0.0

    total_minutes = int(value // 60)
    if total_minutes == 0:
        return "不到1分钟"

    days, remainder = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}天")
    if hours:
        parts.append(f"{hours}小时")
    if minutes:
        parts.append(f"{minutes}分钟")
    return " ".join(parts)
