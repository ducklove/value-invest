// Behaviour tests for ecosystem/js/vc-publish.mjs (published-data envelope v1)
// + cross-language parity with ecosystem/python/vc_publish.py.
//
// The fixtures in tests/fixtures/ecosystem/*.summary.json were written by the
// Python implementation, so re-deriving their bytes and contentHash here proves
// both languages agree. When a Python interpreter is available we also compare
// canonical output for tricky numbers/strings directly.

import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  EnvelopeError,
  KNOWN_TOOLS,
  buildEnvelope,
  canonicalJson,
  contentHash,
  dumpsCompact,
  fileHash,
  nowKstIso,
  validateEnvelope,
  writeIfChanged,
  writeVersion,
} from '../../ecosystem/js/vc-publish.mjs';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const FIXTURES = join(ROOT, 'tests', 'fixtures', 'ecosystem');
const PY_MODULE_DIR = join(ROOT, 'ecosystem', 'python');
const GEN = '2026-09-30T09:00:00+09:00';
const SRC = [{ id: 'unit', name: 'unit test' }];

const env = (data = { a: 1 }, opts = {}) => buildEnvelope('holding_value', data, {
  asOf: '2026-09-30', sources: SRC, generatedAt: GEN, ...opts,
});

function withTmp(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'vc-publish-'));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function findPython() {
  const candidates = [join(ROOT, '.venv', 'bin', 'python'), 'python3', 'python'];
  for (const cmd of candidates) {
    if (cmd.includes('/') && !existsSync(cmd)) continue;
    const r = spawnSync(cmd, ['-c', 'import sys; print(sys.version_info >= (3, 9))'], { encoding: 'utf8' });
    if (r.status === 0 && r.stdout.trim() === 'True') return cmd;
  }
  return null;
}

// --------------------------------------------------------------------------
// canonical JSON / hash
// --------------------------------------------------------------------------

test('canonicalJson sorts keys, is compact and keeps unicode', () => {
  assert.equal(
    canonicalJson({ b: 1, a: [true, null, '한글'], A: { z: 0, y: 'x' } }),
    '{"A":{"y":"x","z":0},"a":[true,null,"한글"],"b":1}',
  );
  assert.equal(dumpsCompact({ b: [1.0, 2], a: 'x' }), '{"a":"x","b":[1,2]}');
});

test('keys are ordered by code point like Python sorted()', () => {
  // U+FF21 (fullwidth A) vs U+1F600 (emoji, a surrogate pair in UTF-16):
  // UTF-16 order would put the emoji first; code-point order puts it last.
  assert.equal(canonicalJson({ '\u{1F600}': 1, 'Ａ': 2 }), '{"Ａ":2,"\u{1F600}":1}');
});

test('numbers use ECMAScript formatting; -0 becomes 0', () => {
  assert.equal(canonicalJson([1.0, -0, 0.1, 1e-7, 1.5e-7, 0.000001, 1e15, 760.56]),
    '[1,0,0.1,1e-7,1.5e-7,0.000001,1000000000000000,760.56]');
});

test('contentHash is deterministic and matches a known vector', () => {
  assert.equal(contentHash({}), 'sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a');
  assert.equal(contentHash({ x: [1, 2.5], y: null }), contentHash({ y: null, x: [1, 2.5] }));
  assert.notEqual(contentHash({ x: 1 }), contentHash({ x: 1.5 }));
});

test('rejects NaN/Infinity/unsafe integers/lone surrogates/non-JSON types', () => {
  for (const bad of [{ x: NaN }, { x: Infinity }, { x: -Infinity }, { x: 2 ** 53 }, { x: '\uD800' },
    { x: undefined }, { x: new Date() }, { x: new Map() }, { x: () => 1 }]) {
    assert.throws(() => contentHash(bad), EnvelopeError);
  }
});

// --------------------------------------------------------------------------
// envelope
// --------------------------------------------------------------------------

test('buildEnvelope fills every v1 field', () => {
  const e = env({ k: 1 }, { asOf: new Date('2026-09-26T22:11:51Z') });
  assert.equal(e.schemaVersion, 1);
  assert.equal(e.kind, 'summary');
  assert.equal(e.asOf, '2026-09-27T07:11:51+09:00');
  assert.equal(e.generatedAt, GEN);
  assert.equal(e.contentHash, contentHash({ k: 1 }));
  assert.deepEqual(e.sources, SRC);
  assert.equal(validateEnvelope(e), e);
});

test('nowKstIso is +09:00 wall-clock time', () => {
  assert.equal(nowKstIso(new Date('2026-09-30T15:00:00Z')), '2026-10-01T00:00:00+09:00');
  assert.match(buildEnvelope('eiayn', { a: 1 }, { asOf: '2026-09-30', sources: SRC }).generatedAt,
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+09:00$/);
});

test('validateEnvelope rejects contract violations', () => {
  const mutations = [
    { schemaVersion: 2 }, { tool: 'Holding Value' }, { kind: 'full' },
    { generatedAt: '2026-09-30T00:00:00Z' }, { asOf: '20260930' }, { sources: [] },
    { sources: [{ id: 'a', name: 'A' }, { id: 'a', name: 'B' }] },
    { sources: [{ id: 'a', name: 'A', url: 'ftp://x' }] },
    { sources: [{ id: 'a', name: 'A', retrievedAt: 'x' }] },
    { contentHash: 'sha256:' + '0'.repeat(64) }, { data: [1] }, { stale: false },
  ];
  for (const change of mutations) {
    assert.throws(() => validateEnvelope({ ...env(), ...change }), EnvelopeError, JSON.stringify(change));
  }
  const missing = { ...env() };
  delete missing.data;
  assert.throws(() => validateEnvelope(missing), EnvelopeError);
  const tampered = env({ ratio: 1.5 });
  tampered.data = { ratio: 1.6 };
  assert.throws(() => validateEnvelope(tampered), /contentHash mismatch/);
});

// --------------------------------------------------------------------------
// writeIfChanged / writeVersion
// --------------------------------------------------------------------------

test('writeIfChanged is a no-op when only generatedAt/sources change', () => withTmp((dir) => {
  const path = join(dir, 'nested', 'summary.json');
  const first = env({ v: 1 });
  assert.equal(writeIfChanged(path, first), true);
  const text = readFileSync(path, 'utf8');
  assert.equal(text, dumpsCompact(first) + '\n');
  const mtime = statSync(path).mtimeMs;
  const later = env({ v: 1 }, { generatedAt: '2026-09-30T10:00:00+09:00', sources: [{ id: 'x', name: 'X' }] });
  assert.equal(writeIfChanged(path, later), false);
  assert.equal(readFileSync(path, 'utf8'), text);
  assert.equal(statSync(path).mtimeMs, mtime);
}));

test('writeIfChanged rewrites on data/asOf change or unreadable file', () => withTmp((dir) => {
  const path = join(dir, 'summary.json');
  assert.equal(writeIfChanged(path, env({ v: 1 })), true);
  assert.equal(writeIfChanged(path, env({ v: 2 })), true);
  assert.deepEqual(JSON.parse(readFileSync(path, 'utf8')).data, { v: 2 });
  assert.equal(writeIfChanged(path, env({ v: 2 }, { asOf: '2026-10-01' })), true);
  writeFileSync(path, '{broken');
  assert.equal(writeIfChanged(path, env({ v: 2 })), true);
  const bad = env();
  bad.contentHash = 'sha256:' + '1'.repeat(64);
  assert.throws(() => writeIfChanged(join(dir, 'other.json'), bad), EnvelopeError);
  assert.equal(existsSync(join(dir, 'other.json')), false);
  assert.deepEqual(readdirSync(dir).sort(), ['summary.json']); // no temp files left
}));

test('writeVersion writes once, skips unchanged, accepts envelopes', () => withTmp((dir) => {
  const path = join(dir, 'version.json');
  const e = env({ v: 1 });
  assert.equal(writeVersion(path, { 'summary.json': e }, { generatedAt: GEN }), true);
  assert.deepEqual(JSON.parse(readFileSync(path, 'utf8')), {
    schemaVersion: 1, tool: 'holding_value', generatedAt: GEN, files: { 'summary.json': e.contentHash },
  });
  assert.equal(writeVersion(path, { 'summary.json': e.contentHash }, { tool: 'holding_value' }), false);
  const legacy = join(dir, 'current.json');
  writeFileSync(legacy, '{"x":1}');
  assert.equal(fileHash(legacy), 'sha256:5041bf1f713df204784353e82f6a4a535931cb64f1f4b4a5aeaffcb720918b22');
  assert.equal(writeVersion(path, { 'summary.json': e, 'current.json': fileHash(legacy) }), true);
  assert.throws(() => writeVersion(path, { 'summary.json': 'nope' }, { tool: 'holding_value' }), EnvelopeError);
  assert.throws(() => writeVersion(path, { 'summary.json': e.contentHash }), EnvelopeError);
}));

// --------------------------------------------------------------------------
// fixtures + cross-language parity
// --------------------------------------------------------------------------

const fixtureFiles = readdirSync(FIXTURES).filter((n) => n.endsWith('.summary.json')).sort();

test('there is one fixture per known tool', () => {
  assert.deepEqual(fixtureFiles.map((n) => n.replace('.summary.json', '')).sort(), [...KNOWN_TOOLS].sort());
});

for (const name of fixtureFiles) {
  test(`fixture ${name}: Python-written bytes and hash reproduce in JS`, () => {
    const raw = readFileSync(join(FIXTURES, name), 'utf8');
    const parsed = JSON.parse(raw);
    assert.equal(parsed.tool, name.replace('.summary.json', ''));
    validateEnvelope(parsed); // recomputes contentHash in JS == hash stored by Python
    assert.equal(contentHash(parsed.data), parsed.contentHash);
    assert.equal(dumpsCompact(parsed) + '\n', raw);
  });
}

test('canonical JSON is byte-identical to Python for tricky values', (t) => {
  const python = findPython();
  if (!python) {
    t.skip('python >= 3.9 not available');
    return;
  }
  const sample = {
    nums: [0, -0, 1, -1, 0.1, 0.2, 1 / 3, 2 / 3, 1e-7, 1.5e-7, 123e-20, 5e-324, 0.000001, 1e15, 9007199254740991,
      4.35, 760.56, 25000000000, 0.30000000000000004, -1.07, 3.0e13, 1234.5678e-3],
    strs: ['', 'a"b', 'back\\slash', 'ctl\u0001\u001f\n\r\t\b\f', '  ', '\u007f', '한글 ¥ €', '\u{1F600}'],
    keys: { b: 1, a: 2, 'Ａ': 3, '\u{1F600}': 4, 'é': 5, Z: 6 },
    nested: [{ z: null, y: [true, false] }],
  };
  const script = [
    'import json, sys',
    `sys.path.insert(0, ${JSON.stringify(PY_MODULE_DIR)})`,
    'import vc_publish as vp',
    'obj = json.loads(sys.stdin.read())',
    'sys.stdout.write(json.dumps({"canon": vp.canonical_json(obj), "hash": vp.content_hash(obj)}))',
  ].join('\n');
  const r = spawnSync(python, ['-c', script], { input: JSON.stringify(sample), encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);
  const out = JSON.parse(r.stdout);
  assert.equal(out.canon, canonicalJson(sample));
  assert.equal(out.hash, contentHash(sample));
});
