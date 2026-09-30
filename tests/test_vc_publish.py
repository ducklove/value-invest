"""ecosystem/python/vc_publish.py — published-data envelope v1 contract.

Also pins every config/schemas/summary/<tool>.schema.json against its fixture
tests/fixtures/ecosystem/<tool>.summary.json (jsonschema when installed,
otherwise the small draft-2020-12 subset validator below).
"""

from __future__ import annotations

import ast
import importlib.util
import json
import math
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "ecosystem"
SCHEMAS = ROOT / "config" / "schemas"

_spec = importlib.util.spec_from_file_location("vc_publish", ROOT / "ecosystem" / "python" / "vc_publish.py")
vp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vp)

GEN = "2026-09-30T09:00:00+09:00"
SRC = [{"id": "unit", "name": "unit test"}]


def _env(data=None, **kw):
    kw.setdefault("as_of", "2026-09-30")
    kw.setdefault("sources", SRC)
    kw.setdefault("generated_at", GEN)
    return vp.build_envelope("holding_value", {"a": 1} if data is None else data, **kw)


# ---------------------------------------------------------------------------
# canonical JSON / hash
# ---------------------------------------------------------------------------

def test_canonical_json_sorts_keys_compact_and_keeps_unicode():
    assert vp.canonical_json({"b": 1, "a": [True, None, "한글"], "A": {"z": 0, "y": "x"}}) == (
        '{"A":{"y":"x","z":0},"a":[true,null,"한글"],"b":1}'
    )


@pytest.mark.parametrize("value, text", [
    (1.0, "1"), (-0.0, "0"), (0.1, "0.1"), (1e-7, "1e-7"), (1.5e-7, "1.5e-7"), (0.000001, "0.000001"),
    (123456789012345.0, "123456789012345"), (1e15, "1000000000000000"), (760.56, "760.56"),
    (-1.07, "-1.07"), (5e-324, "5e-324"), (25000000000.0, "25000000000"), (7, "7"), (-3, "-3"),
])
def test_numbers_follow_ecmascript_to_string(value, text):
    assert vp.canonical_json(value) == text


def test_control_characters_escaped_like_json_stringify():
    s = "".join(chr(c) for c in range(0x20)) + '"\\ \x7f'
    assert vp.canonical_json(s) == json.dumps(s, ensure_ascii=False)
    assert "\\u001f" in vp.canonical_json(s)


def test_content_hash_is_deterministic_and_order_independent():
    a = {"x": [1, 2.5, {"k": "v"}], "y": None}
    b = {"y": None, "x": [1, 2.5, {"k": "v"}]}
    assert vp.content_hash(a) == vp.content_hash(b)
    assert vp.content_hash(a) == vp.content_hash(json.loads(json.dumps(a)))
    assert vp.content_hash({}) == "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    assert vp.content_hash({"x": 1}) != vp.content_hash({"x": 1.5})
    # int and integral float are the same JSON number.
    assert vp.content_hash({"x": 1}) == vp.content_hash({"x": 1.0})


@pytest.mark.parametrize("bad", [
    {"x": math.nan}, {"x": math.inf}, {"x": -math.inf}, {"x": 2**53}, {"x": float(2**53)},
    {1: "int key"}, {"x": "\ud800"}, {"x": {1, 2}}, {"x": date(2026, 1, 1)},
])
def test_rejects_non_json_or_unsafe_values(bad):
    with pytest.raises(vp.EnvelopeError):
        vp.content_hash(bad)


def test_dumps_compact_is_canonical():
    obj = {"b": [1.0, 2], "a": "x"}
    assert vp.dumps_compact(obj) == '{"a":"x","b":[1,2]}'


# ---------------------------------------------------------------------------
# envelope
# ---------------------------------------------------------------------------

def test_build_envelope_fields():
    env = _env({"k": 1}, as_of=date(2026, 9, 29))
    assert env["schemaVersion"] == 1 and env["tool"] == "holding_value" and env["kind"] == "summary"
    assert env["generatedAt"] == GEN
    assert env["asOf"] == "2026-09-29"
    assert env["contentHash"] == vp.content_hash({"k": 1})
    assert env["sources"] == SRC
    assert vp.validate_envelope(env) is env


def test_build_envelope_converts_datetimes_to_kst():
    utc = datetime(2026, 9, 26, 22, 11, 51, tzinfo=timezone.utc)
    env = _env(as_of=utc, generated_at=utc)
    assert env["asOf"] == "2026-09-27T07:11:51+09:00"
    assert env["generatedAt"] == "2026-09-27T07:11:51+09:00"
    naive = datetime(2026, 9, 30, 15, 30)
    assert _env(as_of=naive)["asOf"] == "2026-09-30T15:30:00+09:00"


def test_default_generated_at_is_kst_now():
    env = vp.build_envelope("eiayn", {"a": 1}, as_of="2026-09-30", sources=SRC)
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+09:00$", env["generatedAt"])
    parsed = datetime.fromisoformat(env["generatedAt"])
    assert abs(parsed - datetime.now(timezone(timedelta(hours=9)))) < timedelta(minutes=1)


def _mutated(**changes):
    env = dict(_env())
    for key, value in changes.items():
        if value is KeyError:
            env.pop(key)
        else:
            env[key] = value
    return env


@pytest.mark.parametrize("env", [
    _mutated(schemaVersion=2),
    _mutated(schemaVersion=True),
    _mutated(tool="Holding Value"),
    _mutated(kind="full"),
    _mutated(generatedAt="2026-09-30T00:00:00Z"),
    _mutated(generatedAt="2026-09-30"),
    _mutated(asOf="2026-09-30T15:30:00Z"),
    _mutated(asOf="20260930"),
    _mutated(sources=[]),
    _mutated(sources=[{"id": "a", "name": "A"}, {"id": "a", "name": "B"}]),
    _mutated(sources=[{"id": "a", "name": ""}]),
    _mutated(sources=[{"id": "a", "name": "A", "url": "ftp://x"}]),
    _mutated(sources=[{"id": "a", "name": "A", "retrievedAt": "x"}]),
    _mutated(contentHash="sha256:" + "0" * 64),
    _mutated(contentHash="md5:abc"),
    _mutated(data=[1, 2]),
    _mutated(data=KeyError),
    _mutated(stale=False),
])
def test_validate_envelope_rejects(env):
    with pytest.raises(vp.EnvelopeError):
        vp.validate_envelope(env)


def test_validate_detects_tampered_data():
    env = _env({"ratio": 1.5})
    env["data"] = {"ratio": 1.6}
    with pytest.raises(vp.EnvelopeError, match="contentHash mismatch"):
        vp.validate_envelope(env)


# ---------------------------------------------------------------------------
# write_if_changed / write_version
# ---------------------------------------------------------------------------

def test_write_if_changed_noop_on_same_content(tmp_path):
    path = tmp_path / "out" / "summary.json"
    first = _env({"v": 1})
    assert vp.write_if_changed(path, first) is True
    text = path.read_text(encoding="utf-8")
    assert text == vp.dumps_compact(first) + "\n"
    assert json.loads(text) == first
    mtime = path.stat().st_mtime_ns

    # generatedAt / sources churn alone never rewrites.
    later = _env({"v": 1}, generated_at="2026-09-30T10:00:00+09:00",
                 sources=[{"id": "other", "name": "Other"}])
    assert vp.write_if_changed(path, later) is False
    assert path.read_text(encoding="utf-8") == text
    assert path.stat().st_mtime_ns == mtime


def test_write_if_changed_rewrites_on_data_asof_or_corruption(tmp_path):
    path = tmp_path / "summary.json"
    assert vp.write_if_changed(path, _env({"v": 1})) is True
    assert vp.write_if_changed(path, _env({"v": 2})) is True
    assert json.loads(path.read_text())["data"] == {"v": 2}
    assert vp.write_if_changed(path, _env({"v": 2}, as_of="2026-10-01")) is True
    path.write_text("{not json", encoding="utf-8")
    assert vp.write_if_changed(path, _env({"v": 2})) is True
    stale = json.loads(path.read_text())
    stale["schemaVersion"] = 0
    path.write_text(json.dumps(stale), encoding="utf-8")
    assert vp.write_if_changed(path, _env({"v": 2})) is True


def test_write_if_changed_refuses_invalid_envelope(tmp_path):
    env = _env()
    env["contentHash"] = "sha256:" + "1" * 64
    with pytest.raises(vp.EnvelopeError):
        vp.write_if_changed(tmp_path / "s.json", env)
    assert not (tmp_path / "s.json").exists()


def test_write_is_atomic_on_failure(tmp_path, monkeypatch):
    path = tmp_path / "summary.json"
    vp.write_if_changed(path, _env({"v": 1}))
    before = path.read_bytes()

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(vp.os, "replace", boom)
    with pytest.raises(OSError):
        vp.write_if_changed(path, _env({"v": 2}))
    assert path.read_bytes() == before
    assert sorted(os.listdir(tmp_path)) == ["summary.json"]  # temp file cleaned up


def test_write_version_noop_and_update(tmp_path):
    path = tmp_path / "version.json"
    env = _env({"v": 1})
    assert vp.write_version(path, {"summary.json": env}, generated_at=GEN) is True
    doc = json.loads(path.read_text())
    assert doc == {"schemaVersion": 1, "tool": "holding_value", "generatedAt": GEN,
                   "files": {"summary.json": env["contentHash"]}}
    assert vp.write_version(path, {"summary.json": env["contentHash"]}, tool="holding_value") is False
    legacy = tmp_path / "current.json"
    legacy.write_bytes(b'{"x":1}')
    files = {"summary.json": env, "current.json": vp.file_hash(legacy)}
    assert vp.write_version(path, files) is True
    assert json.loads(path.read_text())["files"]["current.json"] == (
        "sha256:5041bf1f713df204784353e82f6a4a535931cb64f1f4b4a5aeaffcb720918b22"
    )
    with pytest.raises(vp.EnvelopeError):
        vp.write_version(path, {"summary.json": "nope"}, tool="holding_value")
    with pytest.raises(vp.EnvelopeError):
        vp.write_version(path, {"summary.json": env["contentHash"]})  # tool unknown


def test_cli_validate_and_hash(tmp_path, capsys):
    path = tmp_path / "s.json"
    vp.write_if_changed(path, _env({"v": 1}))
    assert vp.main(["validate", str(path)]) == 0
    assert vp.main(["hash", str(path)]) == 0
    bad = json.loads(path.read_text())
    bad["data"]["v"] = 9
    path.write_text(json.dumps(bad))
    assert vp.main(["validate", str(path)]) == 1
    assert vp.main([]) == 2


def test_module_is_stdlib_only_and_py39_friendly():
    src = (ROOT / "ecosystem" / "python" / "vc_publish.py").read_text(encoding="utf-8")
    assert src.startswith("# vendored from value-invest ecosystem/python/vc_publish.py")
    imports = set(re.findall(r"^(?:from|import) ([a-zA-Z_][\w.]*)", src, re.M))
    assert imports <= {"__future__", "hashlib", "json", "os", "re", "sys", "tempfile", "datetime", "typing"}
    ast.parse(src, feature_version=(3, 9))  # siblings may run 3.9+


# ---------------------------------------------------------------------------
# schemas + fixtures
# ---------------------------------------------------------------------------

_ANNOTATIONS = {"$schema", "$id", "$comment", "title", "description", "examples", "default", "format", "$defs"}


def _type_ok(value, name):
    if name == "null":
        return value is None
    if name == "boolean":
        return isinstance(value, bool)
    if name == "integer":
        return isinstance(value, int) and not isinstance(value, bool) or (
            isinstance(value, float) and value.is_integer())
    if name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if name == "string":
        return isinstance(value, str)
    if name == "array":
        return isinstance(value, list)
    if name == "object":
        return isinstance(value, dict)
    raise AssertionError(f"unknown type {name}")


def _mini_validate(value, schema, root, path="$"):
    """Draft 2020-12 subset. Raises AssertionError on unknown keywords."""
    errors = []
    if schema is True:
        return errors
    if schema is False:
        return [f"{path}: not allowed"]
    for key in schema:
        assert key in _ANNOTATIONS or key in {
            "$ref", "type", "properties", "required", "additionalProperties", "items", "prefixItems", "enum",
            "const", "pattern", "minimum", "maximum", "minItems", "maxItems", "minLength", "uniqueItems",
            "anyOf", "propertyNames",
        }, f"unsupported schema keyword {key} at {path}"
    if "$ref" in schema:
        ref = schema["$ref"]
        assert ref.startswith("#/$defs/"), ref
        errors += _mini_validate(value, root["$defs"][ref.split("/")[-1]], root, path)
    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_type_ok(value, t) for t in types):
            return errors + [f"{path}: expected {types}, got {type(value).__name__}"]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: expected const {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} not in enum")
    if "anyOf" in schema and not any(not _mini_validate(value, s, root, path) for s in schema["anyOf"]):
        errors.append(f"{path}: no anyOf branch matched")
    if isinstance(value, str):
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errors.append(f"{path}: {value!r} !~ {schema['pattern']}")
        if len(value) < schema.get("minLength", 0):
            errors.append(f"{path}: too short")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: < minimum")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: > maximum")
    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0) or len(value) > schema.get("maxItems", 1 << 30):
            errors.append(f"{path}: item count {len(value)} out of range")
        if schema.get("uniqueItems") and len({json.dumps(v, sort_keys=True) for v in value}) != len(value):
            errors.append(f"{path}: items not unique")
        prefix = schema.get("prefixItems", [])
        for i, item in enumerate(value):
            sub = prefix[i] if i < len(prefix) else schema.get("items", True)
            errors += _mini_validate(item, sub, root, f"{path}[{i}]")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{path}: missing {key}")
        props = schema.get("properties", {})
        for key, item in value.items():
            if "propertyNames" in schema:
                errors += _mini_validate(key, schema["propertyNames"], root, f"{path}.<{key}>")
            if key in props:
                errors += _mini_validate(item, props[key], root, f"{path}.{key}")
            elif "additionalProperties" in schema:
                errors += _mini_validate(item, schema["additionalProperties"], root, f"{path}.{key}")
    return errors


def _schema_errors(value, schema):
    try:
        import jsonschema  # type: ignore[import-not-found]
    except ImportError:
        return _mini_validate(value, schema, schema)
    validator = jsonschema.Draft202012Validator(schema)
    return [f"{'/'.join(map(str, e.path))}: {e.message}" for e in validator.iter_errors(value)]


def _load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


ENVELOPE_SCHEMA = _load(SCHEMAS / "vc-envelope.schema.json")
FIXTURE_FILES = sorted(FIXTURES.glob("*.summary.json"))


def test_every_known_tool_has_schema_and_fixture():
    schema_tools = {p.name.removesuffix(".schema.json") for p in (SCHEMAS / "summary").glob("*.schema.json")}
    fixture_tools = {p.name.removesuffix(".summary.json") for p in FIXTURE_FILES}
    assert schema_tools == set(vp.KNOWN_TOOLS)
    assert fixture_tools == set(vp.KNOWN_TOOLS)


@pytest.mark.parametrize("schema_path", sorted(SCHEMAS.rglob("*.schema.json")), ids=lambda p: p.name)
def test_schema_files_are_draft_2020_12(schema_path):
    schema = _load(schema_path)
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["$id"].startswith("urn:value-compass:schema:")
    assert schema["type"] == "object"
    _mini_validate({}, schema, schema)  # keyword whitelist check (raises on unsupported keywords)


@pytest.mark.parametrize("fixture", FIXTURE_FILES, ids=lambda p: p.name)
def test_fixture_is_valid_published_file(fixture):
    raw = fixture.read_text(encoding="utf-8")
    env = json.loads(raw)
    tool = fixture.name.removesuffix(".summary.json")
    assert env["tool"] == tool
    vp.validate_envelope(env)
    # Published files are byte-exact canonical JSON + newline.
    assert raw == vp.dumps_compact(env) + "\n"
    assert len(raw.encode("utf-8")) < 16 * 1024
    assert _schema_errors(env, ENVELOPE_SCHEMA) == []
    schema = _load(SCHEMAS / "summary" / f"{tool}.schema.json")
    assert _schema_errors(env["data"], schema) == []


def test_mini_validator_catches_contract_breaks():
    schema = _load(SCHEMAS / "summary" / "holding_value.schema.json")
    data = _load(FIXTURES / "holding_value.summary.json")["data"]
    broken = json.loads(json.dumps(data))
    broken["pairs"][0]["code"] = "000670.KS"
    del broken["pairs"][1]["ratio"]
    broken["averageRatio"] = "183"
    errors = _mini_validate(broken, schema, schema)
    assert any("000670.KS" in e for e in errors)
    assert any("missing ratio" in e for e in errors)
    assert any("averageRatio" in e for e in errors)
    bb = _load(SCHEMAS / "summary" / "buybacks.schema.json")
    bdata = _load(FIXTURES / "buybacks.summary.json")["data"]
    bdata["ratios"]["005930.KS"] = 1.0
    bdata["ratioAsOf"]["005930"] = 20251231
    errs = _mini_validate(bdata, bb, bb)
    assert any("005930.KS" in e for e in errs) and any("ratioAsOf.005930" in e for e in errs)


def test_spac_hunter_schema_accepts_optional_liquidation_fields():
    # spac-hunter 가 추가로 발행하는 선택 필드(valuationDate · currentLiquidationValue ·
    # liquidationDiscountPct)는 스키마에 적혀 있고, 없어도·null 이어도 통과한다(additive).
    schema = _load(SCHEMAS / "summary" / "spac-hunter.schema.json")
    data = _load(FIXTURES / "spac-hunter.summary.json")["data"]
    assert _schema_errors(data, schema) == []  # 필드 없는 옛 발행분
    item = schema["properties"]["spacs"]["items"]
    for field in ("currentLiquidationValue", "liquidationDiscountPct"):
        assert field in item["properties"] and field not in item["required"]
    assert "valuationDate" in schema["properties"] and "valuationDate" not in schema["required"]

    extended = json.loads(json.dumps(data))
    extended["valuationDate"] = "2026-09-28"
    extended["spacs"][0].update(currentLiquidationValue=2137.86, liquidationDiscountPct=8.79)
    extended["spacs"][1].update(currentLiquidationValue=None, liquidationDiscountPct=None)
    assert _schema_errors(extended, schema) == []

    broken = json.loads(json.dumps(extended))
    broken["valuationDate"] = "20260928"
    broken["spacs"][0]["liquidationDiscountPct"] = "8.79"
    errors = _schema_errors(broken, schema)
    assert any("valuationDate" in e for e in errors)
    assert any("liquidationDiscountPct" in e for e in errors)
