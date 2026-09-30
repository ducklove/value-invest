// Value Compass ecosystem sync — generalises scripts/sync-analytics.mjs.
//
//   node scripts/sync-ecosystem.mjs                 verify (default; exit 1 on any FAIL)
//   node scripts/sync-ecosystem.mjs --write         write generated/vendored files
//   node scripts/sync-ecosystem.mjs --only a,b      limit sibling steps to these tool ids
//   node scripts/sync-ecosystem.mjs --hub-only      only the value-invest checks (CI has no siblings)
//   node scripts/sync-ecosystem.mjs --workspace DIR sibling checkouts root (default: ..)
//
// Source of truth: config/ecosystem.json. Steps:
//   1. validate the registry (+ config/analytics-projects.json consistency)
//   2. regenerate the public registry block inside static/ecosystem/vc-shell.js
//   3. check vc-tokens.css: the prefers-color-scheme block mirrors [data-theme="dark"]
//   3b. the hub's own static/index.html must carry an up-to-date theme-boot block
//   4. per public sibling checkout: vendored vc-shell.js / vc-tokens.css (byte-identical),
//      inline theme-boot block between <!-- vc:theme-boot --> markers, <vc-shell> adoption,
//      held-badges ?v= tag, vendored publish helpers (vc_publish.py / vc-publish.mjs)
// --write only writes files in working trees; it never runs git.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, posix, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const argv = process.argv.slice(2);
const write = argv.includes('--write');
const hubOnly = argv.includes('--hub-only');
const argValue = name => { const i = argv.indexOf(name); return i >= 0 ? argv[i + 1] : undefined; };
const only = argValue('--only') ? new Set(argValue('--only').split(',').map(s => s.trim()).filter(Boolean)) : null;
const workspace = resolve(argValue('--workspace') || resolve(root, '..'));

const HUB_ID = 'value-invest';
const ICONS = new Set(['building', 'split', 'rocket', 'buyback', 'etf', 'gold', 'coin', 'pension', 'bond', 'chart',
  'compass', 'grid', 'gauge', 'server']);
const INTERNAL_PORTS = [3288, 8400, 8765, 8790, 8801];
// Must match core/ecosystem.py PROJECTION_FIELDS (tests/test_ecosystem_registry.py compares the output).
export const PROJECTION_FIELDS = ['id', 'integrationKey', 'name', 'description', 'category', 'icon', 'accent', 'url',
  'deploy', 'stockLink', 'viewLink', 'assetLink', 'hubView', 'embed', 'themeParam', 'handoff', 'heldBadges'];
const LINK_FIELDS = ['stockLink', 'viewLink', 'assetLink'];

const paths = {
  registry: resolve(root, 'config/ecosystem.json'),
  analytics: resolve(root, 'config/analytics-projects.json'),
  shell: resolve(root, 'static/ecosystem/vc-shell.js'),
  tokens: resolve(root, 'static/ecosystem/vc-tokens.css'),
  boot: resolve(root, 'static/ecosystem/vc-theme-boot.js'),
  pyHelper: resolve(root, 'ecosystem/python/vc_publish.py'),
  jsHelper: resolve(root, 'ecosystem/js/vc-publish.mjs'),
};

let failures = 0;
let warnings = 0;
const log = {
  ok: (scope, msg) => console.log(`OK    ${scope}: ${msg}`),
  info: (scope, msg) => console.log(`INFO  ${scope}: ${msg}`),
  warn: (scope, msg) => { warnings++; console.warn(`WARN  ${scope}: ${msg}`); },
  fail: (scope, msg) => { failures++; console.error(`FAIL  ${scope}: ${msg}`); },
  wrote: (scope, msg) => console.log(`WROTE ${scope}: ${msg}`),
};

// ---- 1. registry validation ------------------------------------------------
function privateHost(host) {
  if (host === 'localhost' || host.endsWith('.local') || host.endsWith('.lan')) return true;
  return /^(10|127)\.|^192\.168\.|^172\.(1[6-9]|2\d|3[01])\.|^169\.254\./.test(host);
}
function checkPublicUrl(where, url, errors) {
  let u;
  try { u = new URL(url); } catch { errors.push(`${where}: invalid URL ${url}`); return; }
  if (u.protocol !== 'https:') errors.push(`${where}: public URL must be https (${url})`);
  if (privateHost(u.hostname)) errors.push(`${where}: private host in public item (${url})`);
  if (u.port && INTERNAL_PORTS.includes(Number(u.port))) errors.push(`${where}: internal port in public item (${url})`);
  if (u.username || u.password || u.search) errors.push(`${where}: no credentials/query in URLs (${url})`);
}
export function validateRegistry(reg) {
  const errors = [];
  if (reg.version !== 1) errors.push('version must be 1');
  if (typeof reg.hub !== 'string') errors.push('hub URL missing'); else checkPublicUrl('hub', reg.hub, errors);
  if (!reg.heldBadges || typeof reg.heldBadges.version !== 'string' || typeof reg.heldBadges.path !== 'string') {
    errors.push('heldBadges {version, path} required');
  }
  const categories = (reg.categories || []).map(c => c.id);
  if (!categories.length || new Set(categories).size !== categories.length) errors.push('categories empty or duplicated');
  const ids = new Set();
  const keys = new Set();
  for (const [index, tool] of (reg.tools || []).entries()) {
    const where = `tools[${tool.id || index}]`;
    if (!/^[a-z0-9][a-z0-9_:-]*$/.test(tool.id || '')) errors.push(`${where}: bad id`);
    else if (ids.has(tool.id)) errors.push(`${where}: duplicate id`);
    ids.add(tool.id);
    if (tool.integrationKey != null) {
      if (keys.has(tool.integrationKey)) errors.push(`${where}: duplicate integrationKey`);
      keys.add(tool.integrationKey);
    }
    if (!tool.name || !tool.description) errors.push(`${where}: name/description required`);
    if (!categories.includes(tool.category)) errors.push(`${where}: unknown category ${tool.category}`);
    if (!ICONS.has(tool.icon)) errors.push(`${where}: unknown icon ${tool.icon}`);
    if (!/^#[0-9a-fA-F]{6}$/.test(tool.accent || '')) errors.push(`${where}: accent must be #rrggbb`);
    if (!['public', 'internal'].includes(tool.visibility)) errors.push(`${where}: visibility must be public|internal`);
    if (tool.visibility === 'public') {
      if (typeof tool.url !== 'string') errors.push(`${where}: public tools need a url`);
      else checkPublicUrl(where, tool.url, errors);
      const text = JSON.stringify(tool);
      if (/\b(?:10|127|192\.168|172\.(?:1[6-9]|2\d|3[01]))\.\d{1,3}\.\d{1,3}/.test(text)) errors.push(`${where}: private IP in public item`);
      for (const port of INTERNAL_PORTS) if (new RegExp(`:${port}\\b`).test(text)) errors.push(`${where}: internal port :${port} in public item`);
    } else if (tool.vendor) {
      errors.push(`${where}: internal tools are never vendored`);
    }
    for (const field of LINK_FIELDS) {
      const link = tool[field];
      if (link == null) continue;
      if (typeof link.template !== 'string') errors.push(`${where}: ${field}.template required`);
      try { new RegExp(link.accepts); } catch (error) { errors.push(`${where}: ${field}.accepts ${error.message}`); }
    }
    for (const flag of ['themeParam', 'handoff', 'heldBadges']) {
      if (typeof tool[flag] !== 'boolean') errors.push(`${where}: ${flag} must be boolean`);
    }
    if (tool.handoff && !tool.integrationKey) errors.push(`${where}: handoff tools need an integrationKey`);
    if (!Array.isArray(tool.data)) errors.push(`${where}: data must be an array`);
    const v = tool.vendor;
    if (v != null) {
      if (!['html', 'dir', 'src'].every(k => typeof v[k] === 'string')) errors.push(`${where}: vendor {html, dir, src} required`);
      if (!['shell', 'themeBoot'].every(k => typeof v[k] === 'boolean')) errors.push(`${where}: vendor.shell/themeBoot must be boolean`);
      for (const k of ['python', 'js']) if (v[k] != null && typeof v[k] !== 'string') errors.push(`${where}: vendor.${k} must be a path or null`);
    }
  }
  return errors;
}

export function publicProjection(reg) {
  const pub = reg.tools.filter(t => t.visibility === 'public');
  const used = new Set(pub.map(t => t.category));
  return {
    version: reg.version,
    hub: reg.hub,
    categories: reg.categories.filter(c => used.has(c.id)),
    tools: pub.map(t => {
      const out = {};
      for (const field of PROJECTION_FIELDS) if (t[field] !== null && t[field] !== undefined) out[field] = t[field];
      return out;
    }),
  };
}

function analyticsConsistency(reg, entries) {
  const byId = new Map(reg.tools.map(t => [t.id, t]));
  const seen = new Set();
  for (const entry of entries) {
    const tool = byId.get(entry.project);
    const scope = `analytics-projects[${entry.project}]`;
    seen.add(entry.project);
    if (!tool) { log.fail(scope, 'no registry tool with this id'); continue; }
    if (!tool.vendor) { log.fail(scope, 'registry tool has no vendor block'); continue; }
    if (tool.vendor.html !== entry.html) log.fail(scope, `html ${entry.html} != vendor.html ${tool.vendor.html}`);
    if (tool.id === HUB_ID) continue; // the hub is the source, its copies live in static/ecosystem
    const assetDir = posix.dirname(entry.asset);
    if (assetDir !== tool.vendor.dir) log.fail(scope, `analytics dir ${assetDir} != vendor.dir ${tool.vendor.dir}`);
    if (!entry.src.startsWith(tool.vendor.src)) log.fail(scope, `analytics src ${entry.src} not under vendor.src ${tool.vendor.src}`);
  }
  for (const tool of reg.tools) {
    if (tool.vendor && !seen.has(tool.id)) log.fail(`registry[${tool.id}]`, 'vendored tool missing from config/analytics-projects.json');
  }
}

// ---- 2/3. hub generated assets -------------------------------------------------
const REGISTRY_BLOCK = /\/\* vc:registry:start \*\/ [\s\S]*? \/\* vc:registry:end \*\//;
export function renderShell(shellSource, reg) {
  if (!REGISTRY_BLOCK.test(shellSource)) throw new Error('vc:registry markers missing in vc-shell.js');
  const block = `/* vc:registry:start */ ${JSON.stringify(publicProjection(reg))} /* vc:registry:end */`;
  return shellSource.replace(REGISTRY_BLOCK, () => block);
}
function declarations(css, selectorPattern) {
  const match = css.match(selectorPattern);
  if (!match) return null;
  return match[1].split(';').map(s => s.trim().replace(/\s+/g, ' ')).filter(Boolean).join(';');
}
function checkTokens(tokens) {
  const dark = declarations(tokens, /:root\[data-theme="dark"\]\s*\{([^}]*)\}/);
  const auto = declarations(tokens, /@media \(prefers-color-scheme: dark\)\s*\{\s*:root:not\(\[data-theme="light"\]\):not\(\[data-theme="dark"\]\)\s*\{([^}]*)\}/);
  if (!dark || !auto) log.fail('vc-tokens.css', 'dark or prefers-color-scheme block missing');
  else if (dark !== auto) log.fail('vc-tokens.css', 'prefers-color-scheme block differs from [data-theme="dark"]');
  else log.ok('vc-tokens.css', 'auto-dark block mirrors [data-theme="dark"]');
}

const BOOT_BLOCK = /<!-- vc:theme-boot -->[\s\S]*?<!-- \/vc:theme-boot -->/g;
export function bootBlock(bootSource) {
  return `<!-- vc:theme-boot --><script>\n${bootSource.replace(/\s+$/, '')}\n</script><!-- /vc:theme-boot -->`;
}

// ---- helpers -------------------------------------------------------------------
function read(file) { return existsSync(file) ? readFileSync(file, 'utf8') : null; }
function syncCopy(scope, source, target, { missingIsFailure }) {
  const current = read(target);
  if (current === source) { log.ok(scope, 'up to date'); return; }
  if (write) {
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, source);
    log.wrote(scope, target);
    return;
  }
  if (current === null) (missingIsFailure ? log.fail : log.info)(scope, 'not vendored yet');
  else log.fail(scope, 'differs from the hub copy (run npm run sync:ecosystem:write)');
}
function syncBootBlock(scope, htmlPath, html, block, required) {
  const count = (html.match(BOOT_BLOCK) || []).length;
  if (count === 0) {
    (required ? log.fail : log.info)(scope, 'theme-boot markers not adopted');
    return html;
  }
  if (count > 1) { log.fail(scope, 'theme-boot markers appear more than once'); return html; }
  if (html.includes(block)) { log.ok(scope, 'theme-boot block up to date'); return html; }
  if (!write) { log.fail(scope, 'theme-boot block is stale'); return html; }
  const next = html.replace(BOOT_BLOCK, () => block);
  writeFileSync(htmlPath, next);
  log.wrote(scope, `${htmlPath} (theme-boot block)`);
  return next;
}

// ---- main -------------------------------------------------------------------
function main() {
  const reg = JSON.parse(readFileSync(paths.registry, 'utf8'));
  const errors = validateRegistry(reg);
  errors.forEach(error => log.fail('registry', error));
  if (errors.length) return;
  log.ok('registry', `${reg.tools.length} tools (${reg.tools.filter(t => t.visibility === 'public').length} public)`);
  analyticsConsistency(reg, JSON.parse(readFileSync(paths.analytics, 'utf8')));

  const shellSource = readFileSync(paths.shell, 'utf8');
  const shell = renderShell(shellSource, reg);
  if (shell === shellSource) log.ok('static/ecosystem/vc-shell.js', 'registry block up to date');
  else if (write) { writeFileSync(paths.shell, shell); log.wrote('static/ecosystem/vc-shell.js', 'registry block regenerated'); }
  else log.fail('static/ecosystem/vc-shell.js', 'registry block is stale (run npm run sync:ecosystem:write)');
  const tokens = readFileSync(paths.tokens, 'utf8');
  checkTokens(tokens);
  const block = bootBlock(readFileSync(paths.boot, 'utf8'));

  const hub = reg.tools.find(t => t.id === HUB_ID);
  if (hub && hub.vendor) {
    const htmlPath = resolve(root, hub.vendor.html);
    // The hub's own page always carries the boot block (vendor.themeBoot only gates siblings' adoption).
    syncBootBlock(`${HUB_ID} ${hub.vendor.html}`, htmlPath, readFileSync(htmlPath, 'utf8'), block, true);
  }
  if (hubOnly) return;

  const pySource = read(paths.pyHelper);
  const jsSource = read(paths.jsHelper);
  for (const tool of reg.tools) {
    if (tool.visibility !== 'public' || !tool.vendor || tool.id === HUB_ID) continue;
    if (only && !only.has(tool.id)) continue;
    const repo = resolve(workspace, tool.id);
    if (!existsSync(repo)) { log.warn(tool.id, `checkout not found at ${repo} — skipped`); continue; }
    const v = tool.vendor;
    const dir = resolve(repo, v.dir);
    syncCopy(`${tool.id} ${posix.join(v.dir, 'vc-shell.js')}`, shell, resolve(dir, 'vc-shell.js'), { missingIsFailure: v.shell });
    syncCopy(`${tool.id} ${posix.join(v.dir, 'vc-tokens.css')}`, tokens, resolve(dir, 'vc-tokens.css'), { missingIsFailure: v.shell });

    const htmlPath = resolve(repo, v.html);
    let html = read(htmlPath);
    if (html === null) {
      // Only an adopting sibling must have its page; otherwise a moved/renamed HTML is a warning.
      (v.shell || v.themeBoot ? log.fail : log.warn)(tool.id, `${v.html} not found`);
      continue;
    }
    html = syncBootBlock(`${tool.id} ${v.html}`, htmlPath, html, block, v.themeBoot);

    if (v.shell) {
      const missing = [];
      if (!html.includes(`<vc-shell tool="${tool.id}"`)) missing.push(`<vc-shell tool="${tool.id}">`);
      if (!html.includes(`href="${v.src}vc-tokens.css`)) missing.push(`${v.src}vc-tokens.css <link>`);
      if (!html.includes(`src="${v.src}vc-shell.js`)) missing.push(`${v.src}vc-shell.js <script>`);
      if (missing.length) log.fail(tool.id, `shell adoption incomplete: ${missing.join(', ')}`);
      else log.ok(tool.id, 'shell adopted');
    } else {
      log.info(tool.id, 'shell not adopted (vendor.shell=false)');
    }

    if (tool.heldBadges) {
      const tags = [...html.matchAll(/portfolio-held-badges\.js\?v=([^"'&\s>]+)/g)].map(m => m[1]);
      const want = reg.heldBadges.version;
      if (!tags.length) (v.shell ? log.fail : log.warn)(tool.id, 'held-badges script tag missing');
      else if (tags.some(tag => tag !== want)) (v.shell ? log.fail : log.warn)(tool.id, `held-badges ?v=${tags.join(',')} (registry ${want})`);
      else log.ok(tool.id, `held-badges ?v=${want}`);
    }

    for (const [kind, source, name] of [['python', pySource, 'vc_publish.py'], ['js', jsSource, 'vc-publish.mjs']]) {
      if (!v[kind]) continue;
      const scope = `${tool.id} ${posix.join(v[kind], name)}`;
      if (source === null) { log.warn(scope, `hub source ecosystem/${kind}/${name} missing — skipped`); continue; }
      syncCopy(scope, source, resolve(repo, v[kind], name), { missingIsFailure: false });
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
  console.log(`\n${failures ? 'FAILED' : 'PASSED'} — ${failures} failure(s), ${warnings} warning(s)${write ? ' [write]' : ' [verify]'}`);
  process.exitCode = failures ? 1 : 0;
}
