"""Canonical event model shared by every source adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class Event:
    case_id: str
    activity: str
    event_ts_utc_ms: int
    event_pos: int
    lifecycle: str | None
    resource: str | None


class SourceContractError(ValueError):
    """Raised when a source log violates the canonical ingestion contract."""


COMPLETE_LIFECYCLE = "COMPLETE"
"""The single lifecycle value the canonical completion perspective includes."""


def utc_epoch_ms(moment: datetime) -> int:
    """Convert an offset-aware moment to canonical UTC epoch milliseconds.

    The arithmetic is fixed and shared: adapters must never reimplement it, so
    every source format produces identical millisecond values.
    """

    if moment.tzinfo is None or moment.utcoffset() is None:
        raise SourceContractError("timestamp must carry an explicit UTC offset")
    delta = moment.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000 + delta.microseconds // 1_000
