"""The vendored validator must accept the official examples shipped with the spec."""

import json
from pathlib import Path

import pytest

from tamslite.store import validation

EXAMPLES = Path(__file__).resolve().parents[2] / "vendor" / "tams-examples-8.1"


def load(name: str):
    return json.loads((EXAMPLES / name).read_text())


@pytest.mark.parametrize(
    "example,schema",
    [
        ("flow-put.json", "flow.json"),
        ("flow-get-200-video-h264.json", "flow.json"),
        ("flow-get-200-audio-aac.json", "flow.json"),
        ("flow-segment-post.json", "flow-segment-post.json"),
        ("flow-storage-post-201.json", "flow-storage.json"),
        ("source-get-200-basic.json", "source.json"),
    ],
)
def test_official_example_validates(example, schema):
    assert validation.validate(schema, load(example)) == []


def test_official_segment_list_validates():
    for item in load("flow-segments-get-200.json"):
        assert validation.validate("flow-segment.json", item) == []
