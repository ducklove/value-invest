// vendored from value-invest ecosystem/js/vc-publish.mjs — do not edit in sibling repos; run node scripts/sync-ecosystem.mjs --write
//
// Value Compass published-data envelope v1 — dependency-free Node ESM
// (node:crypto, node:fs, node:path only). Twin of ecosystem/python/vc_publish.py:
// canonicalJson() yields the SAME bytes as the Python canonical_json(), so
// contentHash is identical across languages. Contract:
// value-invest docs/ecosystem/data-contract.md
//
// Canonical JSON: keys sorted by Unicode code point, separators "," ":",
// JSON.stringify string escaping, ECMAScript Number#toString numbers,
// NaN/Infinity rejected, |n| <= 2^53-1, UTF-8. Published files = canonical
// form of the whole envelope + "\n".

import { createHash } from 'node:crypto';
import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeSync, chmodSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';

export const SCHEMA_VERSION = 1;
export const KINDS = Object.freeze(['summary']);
export const KNOWN_TOOLS = Object.freeze([
  'holding_value',
  'common_preferred_spread',
  'spac-hunter',
  'buybacks',
  'eiayn',
  'gold_gap',
  'all-about-gold',
  'nps-tracker',
  'bond-mate',
]);
export const ENVELOPE_KEYS = Object.freeze(['schemaVersion', 'tool', 'kind', 'generatedAt', 'asOf', 'sources', 'contentHash', 'data']);

const TOOL_RE = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const SOURCE_ID_RE = /^[a-z0-9][a-z0-9_.-]{0,63}$/;
const HASH_RE = /^sha256:[0-9a-f]{64}$/;
const GENERATED_AT_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?\+09:00$/;
const AS_OF_RE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?\+09:00)?$/;
const URL_RE = /^https?:\/\/\S+$/;
const LONE_SURROGATE_RE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

export class EnvelopeError extends Error {
  constructor(message) {
    super(message);
    this.name = 'EnvelopeError';
  }
}

// ---------------------------------------------------------------------------
// canonical JSON
// ---------------------------------------------------------------------------

/** Compare strings by Unicode code point (Python's sorted() order). */
function compareCodePoints(a, b) {
  const ia = a[Symbol.iterator]();
  const ib = b[Symbol.iterator]();
  for (;;) {
    const x = ia.next();
    const y = ib.next();
    if (x.done || y.done) return x.done === y.done ? 0 : (x.done ? -1 : 1);
    const cx = x.value.codePointAt(0);
    const cy = y.value.codePointAt(0);
    if (cx !== cy) return cx < cy ? -1 : 1;
  }
}

/** Plain JSON object (not an Array/Date/Map/class instance). */
function isRecord(value) {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function encode(value, path, out) {
  if (value === null) {
    out.push('null');
  } else if (value === true || value === false) {
    out.push(value ? 'true' : 'false');
  } else if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new EnvelopeError(`${path}: NaN/Infinity is not allowed (use null for unknown)`);
    if (Math.abs(value) > Number.MAX_SAFE_INTEGER) throw new EnvelopeError(`${path}: |number| exceeds 2**53-1`);
    out.push(String(value)); // -0 -> "0"
  } else if (typeof value === 'string') {
    if (LONE_SURROGATE_RE.test(value)) throw new EnvelopeError(`${path}: lone surrogate in string`);
    out.push(JSON.stringify(value));
  } else if (Array.isArray(value)) {
    out.push('[');
    value.forEach((item, i) => {
      if (i) out.push(',');
      encode(item, `${path}[${i}]`, out);
    });
    out.push(']');
  } else if (isRecord(value)) {
    const keys = Object.keys(value).sort(compareCodePoints);
    out.push('{');
    keys.forEach((key, i) => {
      if (i) out.push(',');
      encode(key, path, out);
      out.push(':');
      encode(value[key], `${path}.${key}`, out);
    });
    out.push('}');
  } else {
    const type = value === undefined ? 'undefined' : (value?.constructor?.name || typeof value);
    throw new EnvelopeError(`${path}: unsupported type ${type}`);
  }
}

/** Canonical compact JSON text — byte-identical to Python canonical_json(). */
export function canonicalJson(obj) {
  const out = [];
  encode(obj, '$', out);
  return out.join('');
}

/** Compact JSON for published files (== canonical form, no newline). */
export const dumpsCompact = canonicalJson;

/** "sha256:<hex>" of the canonical UTF-8 JSON of data. */
export function contentHash(data) {
  return 'sha256:' + createHash('sha256').update(canonicalJson(data), 'utf8').digest('hex');
}

/** "sha256:<hex>" of a file's raw bytes (legacy files in version.json). */
export function fileHash(path) {
  return 'sha256:' + createHash('sha256').update(readFileSync(path)).digest('hex');
}

// ---------------------------------------------------------------------------
// envelope
// ---------------------------------------------------------------------------

/** Current time as "YYYY-MM-DDTHH:MM:SS+09:00". */
export function nowKstIso(now = new Date()) {
  return new Date(now.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 19) + '+09:00';
}

function kstText(value, field) {
  if (value instanceof Date) return nowKstIso(value);
  if (typeof value === 'string') return value;
  throw new EnvelopeError(`${field}: expected string or Date`);
}

function normalizeSources(sources) {
  return (sources || []).map((src) => {
    if (!src || typeof src !== 'object') throw new EnvelopeError('sources: each item must be an object');
    const item = { id: src.id, name: src.name };
    if (src.url != null) item.url = src.url;
    return item;
  });
}

/**
 * Build and validate a v1 envelope around `data`.
 * asOf: KST "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM[:SS]+09:00" (a Date is formatted as KST datetime).
 */
export function buildEnvelope(tool, data, { asOf, sources, generatedAt = null, kind = 'summary' } = {}) {
  const envelope = {
    schemaVersion: SCHEMA_VERSION,
    tool,
    kind,
    generatedAt: generatedAt ? kstText(generatedAt, 'generatedAt') : nowKstIso(),
    asOf: kstText(asOf, 'asOf'),
    sources: normalizeSources(sources),
    contentHash: contentHash(data),
    data,
  };
  validateEnvelope(envelope);
  return envelope;
}

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

/** Structural v1 checks (no schema library). Returns obj or throws EnvelopeError. */
export function validateEnvelope(obj) {
  if (!isPlainObject(obj)) throw new EnvelopeError('envelope must be an object');
  const missing = ENVELOPE_KEYS.filter((k) => !(k in obj));
  if (missing.length) throw new EnvelopeError(`envelope missing keys: ${missing.join(', ')}`);
  const extra = Object.keys(obj).filter((k) => !ENVELOPE_KEYS.includes(k)).sort();
  if (extra.length) throw new EnvelopeError(`envelope has unknown keys: ${extra.join(', ')}`);
  if (obj.schemaVersion !== SCHEMA_VERSION) throw new EnvelopeError(`schemaVersion must be ${SCHEMA_VERSION}`);
  if (typeof obj.tool !== 'string' || !TOOL_RE.test(obj.tool)) {
    throw new EnvelopeError('tool must be a registry id ([a-z0-9][a-z0-9_-]*)');
  }
  if (!KINDS.includes(obj.kind)) throw new EnvelopeError(`kind must be one of ${KINDS.join(', ')}`);
  if (typeof obj.generatedAt !== 'string' || !GENERATED_AT_RE.test(obj.generatedAt)) {
    throw new EnvelopeError('generatedAt must be ISO8601 with +09:00 (YYYY-MM-DDTHH:MM:SS+09:00)');
  }
  if (typeof obj.asOf !== 'string' || !AS_OF_RE.test(obj.asOf)) {
    throw new EnvelopeError('asOf must be a KST date (YYYY-MM-DD) or datetime with +09:00');
  }
  if (!Array.isArray(obj.sources) || !obj.sources.length) throw new EnvelopeError('sources must be a non-empty list');
  const seen = new Set();
  obj.sources.forEach((src, i) => {
    if (!isPlainObject(src)) throw new EnvelopeError(`sources[${i}] must be an object`);
    const extraSrc = Object.keys(src).filter((k) => !['id', 'name', 'url'].includes(k)).sort();
    if (extraSrc.length) throw new EnvelopeError(`sources[${i}] has unknown keys: ${extraSrc.join(', ')}`);
    if (typeof src.id !== 'string' || !SOURCE_ID_RE.test(src.id)) {
      throw new EnvelopeError(`sources[${i}].id must match [a-z0-9][a-z0-9_.-]*`);
    }
    if (seen.has(src.id)) throw new EnvelopeError(`sources[${i}].id duplicated: ${src.id}`);
    seen.add(src.id);
    if (typeof src.name !== 'string' || !src.name.trim()) throw new EnvelopeError(`sources[${i}].name must be a non-empty string`);
    if ('url' in src && (typeof src.url !== 'string' || !URL_RE.test(src.url))) {
      throw new EnvelopeError(`sources[${i}].url must be an http(s) URL`);
    }
  });
  if (!isPlainObject(obj.data)) throw new EnvelopeError('data must be an object');
  if (typeof obj.contentHash !== 'string' || !HASH_RE.test(obj.contentHash)) {
    throw new EnvelopeError('contentHash must be sha256:<64 hex>');
  }
  const actual = contentHash(obj.data);
  if (actual !== obj.contentHash) {
    throw new EnvelopeError(`contentHash mismatch: declared ${obj.contentHash}, actual ${actual}`);
  }
  return obj;
}

// ---------------------------------------------------------------------------
// writing (atomic + no-op rule)
// ---------------------------------------------------------------------------

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}

function atomicWrite(path, text) {
  const target = resolve(path);
  const dir = dirname(target);
  mkdirSync(dir, { recursive: true });
  const tmp = join(dir, `.vc-${basename(target)}.${process.pid}.${Date.now()}.tmp`);
  let fd = null;
  try {
    fd = openSync(tmp, 'w', 0o644);
    writeSync(fd, text, null, 'utf8');
    fsyncSync(fd);
    closeSync(fd);
    fd = null;
    chmodSync(tmp, 0o644);
    renameSync(tmp, target);
  } catch (err) {
    if (fd !== null) {
      try { closeSync(fd); } catch { /* ignore */ }
    }
    try { unlinkSync(tmp); } catch { /* ignore */ }
    throw err;
  }
}

function sameContent(existing, envelope) {
  if (!isPlainObject(existing)) return false;
  return ['schemaVersion', 'tool', 'kind', 'asOf', 'contentHash'].every((k) => existing[k] === envelope[k]);
}

/**
 * Write envelope unless the file already holds the same content
 * (same schemaVersion, tool, kind, asOf, contentHash — generatedAt/sources ignored).
 * Returns true when the file was (re)written.
 */
export function writeIfChanged(path, envelope) {
  validateEnvelope(envelope);
  if (sameContent(readJson(path), envelope)) return false;
  atomicWrite(path, dumpsCompact(envelope) + '\n');
  return true;
}

function sameFiles(a, b) {
  if (!isPlainObject(a)) return false;
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  return ka.length === kb.length && kb.every((k) => a[k] === b[k]);
}

/**
 * Write version.json = {schemaVersion, tool, generatedAt, files}.
 * files: { "summary.json": "sha256:…" | envelope, … }. Returns false (no write)
 * when tool and the files map are unchanged.
 */
export function writeVersion(path, files, { tool = null, generatedAt = null } = {}) {
  const normalized = {};
  let resolvedTool = tool;
  for (const [name, raw] of Object.entries(files || {})) {
    let value = raw;
    if (isPlainObject(raw)) {
      resolvedTool = resolvedTool || raw.tool;
      value = raw.contentHash;
    }
    if (!name || typeof value !== 'string' || !HASH_RE.test(value)) {
      throw new EnvelopeError(`version files[${JSON.stringify(name)}] must be sha256:<64 hex>`);
    }
    normalized[name] = value;
  }
  if (!Object.keys(normalized).length) throw new EnvelopeError('version files must not be empty');
  if (typeof resolvedTool !== 'string' || !TOOL_RE.test(resolvedTool)) {
    throw new EnvelopeError('version tool must be a registry id');
  }
  const existing = readJson(path);
  if (isPlainObject(existing) && existing.schemaVersion === SCHEMA_VERSION
      && existing.tool === resolvedTool && sameFiles(existing.files, normalized)) {
    return false;
  }
  const doc = {
    schemaVersion: SCHEMA_VERSION,
    tool: resolvedTool,
    generatedAt: generatedAt ? kstText(generatedAt, 'generatedAt') : nowKstIso(),
    files: normalized,
  };
  if (!GENERATED_AT_RE.test(doc.generatedAt)) throw new EnvelopeError('generatedAt must be ISO8601 with +09:00');
  atomicWrite(path, dumpsCompact(doc) + '\n');
  return true;
}
