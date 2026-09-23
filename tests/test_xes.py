from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from piw.xes import XesContractError, iter_xes_events, source_trace_count


def test_required_timestamp_failure_is_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "bad.xes"
    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="case-1"/>
    <event><string key="concept:name" value="A"/></event>
  </trace>
</log>
""",
        encoding="utf-8",
    )
    with pytest.raises(XesContractError, match="time:timestamp"):
        list(iter_xes_events(source))


def test_offset_aware_timestamp_normalizes_to_utc_ms(tmp_path: Path) -> None:
    source = tmp_path / "offset.xes"
    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="case-1"/>
    <event>
      <string key="concept:name" value="A"/>
      <date key="time:timestamp" value="1970-01-01T01:00:00.123+01:00"/>
    </event>
  </trace>
</log>
""",
        encoding="utf-8",
    )
    assert list(iter_xes_events(source))[0].event_ts_utc_ms == 123


def test_malformed_xml_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "truncated.xes"
    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="case-1"/>
""",
        encoding="utf-8",
    )
    with pytest.raises(XesContractError, match="malformed XES XML"):
        list(iter_xes_events(source))
    with pytest.raises(XesContractError, match="malformed XES XML"):
        source_trace_count(source)


def test_unreadable_gzip_stream_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "mislabeled.xes.gz"
    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/"><trace/></log>
""",
        encoding="utf-8",
    )
    with pytest.raises(XesContractError, match="not a valid gzip stream"):
        list(iter_xes_events(source))


def test_gzipped_xes_streams_the_same_events(tmp_path: Path) -> None:
    plain = tmp_path / "plain.xes"
    plain.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="case-1"/>
    <event>
      <string key="concept:name" value="A"/>
      <date key="time:timestamp" value="2024-03-01T08:00:00+01:00"/>
    </event>
  </trace>
</log>
""",
        encoding="utf-8",
    )
    compressed = tmp_path / "compressed.xes.gz"
    compressed.write_bytes(gzip.compress(plain.read_bytes()))

    assert list(iter_xes_events(compressed)) == list(iter_xes_events(plain))
    assert source_trace_count(compressed) == 1
