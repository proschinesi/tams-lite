"""Unit tests for the TAI timeline helpers (no services required)."""

from tamslite import timeline as tl


def test_roundtrip_half_open():
    r = tl.timerange_to_ns("[10:0_12:500000000)")
    assert (r.start_ns, r.end_ns) == (10_000_000_000, 12_500_000_000)
    assert tl.ns_to_timerange(r.start_ns, r.end_ns) == "[10:0_12:500000000)"


def test_inclusive_end_becomes_exclusive_plus_one():
    r = tl.timerange_to_ns("[10:0_12:0]")
    assert r.end_ns == 12_000_000_001


def test_open_ranges():
    assert tl.timerange_to_ns("[5:0_") == tl.RangeNS(5_000_000_000, None)
    assert tl.timerange_to_ns("_10:0)") == tl.RangeNS(None, 10_000_000_000)
    assert tl.timerange_to_ns("_") == tl.RangeNS(None, None)
    assert tl.ns_to_timerange(None, None) == "_"


def test_eternity_and_never():
    assert not tl.timerange_to_ns("_").bounded
    never = tl.timerange_to_ns("()")
    assert never.bounded and never.duration_ns() == 0


def test_timestamps():
    assert tl.ts_to_ns("1694429247:500000000") == 1_694_429_247_500_000_000
    assert tl.ns_to_ts(1_500_000_000) == "1:500000000"
    assert tl.ts_to_ns("-2:500000000") == -2_500_000_000


def test_ffprobe_seconds_exact():
    assert tl.seconds_str_to_ns("1.480000") == 1_480_000_000
    assert tl.seconds_str_to_ns("2.000000") == 2_000_000_000
