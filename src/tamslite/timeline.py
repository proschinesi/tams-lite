"""TAI timeline helpers on top of BBC's mediatimestamp.

TAMS expresses time as TAI `seconds:nanoseconds` strings and ranges like
`[1694429247:0_1694429248:0)`. Internally we normalise every range to
half-open nanosecond intervals [start_ns, end_ns) which map directly onto
Postgres int8range.
"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction

from mediatimestamp.immutable import TimeRange, Timestamp

NS = 1_000_000_000


@dataclass(frozen=True)
class RangeNS:
    """Half-open [start_ns, end_ns) interval. None means unbounded."""

    start_ns: int | None
    end_ns: int | None

    @property
    def bounded(self) -> bool:
        return self.start_ns is not None and self.end_ns is not None

    def duration_ns(self) -> int:
        if not self.bounded:
            raise ValueError("unbounded range has no duration")
        return self.end_ns - self.start_ns


def parse_timerange(s: str) -> TimeRange:
    return TimeRange.from_str(s)


def timerange_to_ns(s: str | TimeRange) -> RangeNS:
    """Normalise a TAMS timerange to a half-open ns interval.

    Inclusive ends become end+1ns exclusive; exclusive starts become
    start+1ns inclusive, per the nanosecond resolution of the format.
    """
    tr = TimeRange.from_str(s) if isinstance(s, str) else s
    if tr.is_empty():
        return RangeNS(0, 0)
    start_ns: int | None = None
    end_ns: int | None = None
    if tr.start is not None:
        start_ns = tr.start.to_nanosec()
        if not tr.includes_start():
            start_ns += 1
    if tr.end is not None:
        end_ns = tr.end.to_nanosec()
        if tr.includes_end():
            end_ns += 1
    return RangeNS(start_ns, end_ns)


def ns_to_timerange(start_ns: int | None, end_ns: int | None) -> str:
    """Render a half-open ns interval as a TAMS timerange string."""
    if start_ns is None and end_ns is None:
        return "_"
    if start_ns is not None and end_ns is not None:
        tr = TimeRange(
            Timestamp.from_nanosec(start_ns),
            Timestamp.from_nanosec(end_ns),
            TimeRange.INCLUDE_START,
        )
    elif start_ns is not None:
        tr = TimeRange.from_start(Timestamp.from_nanosec(start_ns), TimeRange.INCLUDE_START)
    else:
        tr = TimeRange.from_end(Timestamp.from_nanosec(end_ns), TimeRange.EXCLUDE_END)
    return tr.to_sec_nsec_range()


def ts_to_ns(s: str) -> int:
    """Parse a TAMS timestamp (`sec:ns`, possibly negative) to nanoseconds."""
    return Timestamp.from_str(s).to_nanosec()


def ns_to_ts(ns: int) -> str:
    return Timestamp.from_nanosec(ns).to_sec_nsec()


def now_tai_ns() -> int:
    """Current time on the TAI timeline (mediatimestamp applies UTC->TAI offset)."""
    return Timestamp.get_time().to_nanosec()


def seconds_str_to_ns(s: str) -> int:
    """Exact decimal-seconds string (e.g. ffprobe '1.480000') to nanoseconds."""
    return int(Fraction(s) * NS)
