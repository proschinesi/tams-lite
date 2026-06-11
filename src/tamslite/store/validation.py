"""Request/response validation against the official BBC TAMS v8.1 JSON Schemas.

The schemas are vendored verbatim from github.com/bbc/tams tag 8.1 and
reference each other by relative filename, so we resolve refs straight from
the vendor directory.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

# repo layout: <root>/src/tamslite/store/validation.py -> <root>/vendor/...
SCHEMA_DIR = Path(__file__).resolve().parents[3] / "vendor" / "tams-schemas-8.1"
if not SCHEMA_DIR.is_dir():
    SCHEMA_DIR = Path(os.environ.get("TAMS_SCHEMA_DIR", "/app/vendor/tams-schemas-8.1"))


def _retrieve(uri: str) -> Resource:
    path = SCHEMA_DIR / Path(uri).name
    contents = json.loads(path.read_text())
    return Resource.from_contents(contents, default_specification=DRAFT202012)


_REGISTRY = Registry(retrieve=_retrieve)


@lru_cache(maxsize=None)
def validator_for(schema_name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMA_DIR / schema_name).read_text())
    return Draft202012Validator(schema, registry=_REGISTRY)


def validate(schema_name: str, instance) -> list[str]:
    """Return a list of human-readable validation errors (empty if valid)."""
    v = validator_for(schema_name)
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in v.iter_errors(instance)
    ]
