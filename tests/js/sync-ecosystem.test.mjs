// scripts/sync-ecosystem.mjs — vc-shell.js / vc-tokens.css ?v= cache labels in adopting siblings
// must equal the canonical VCShell.version (verify fails on a stale label, --write relabels it).
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assetVersionLabels, bootBlock, relabelAssets, shellVersion } from '../../scripts/sync-ecosystem.mjs';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const SCRIPT = fileURLToPath(new URL('../../scripts/sync-ecosystem.mjs', import.meta.url));
const SHELL = read('static/ecosystem/vc-shell.js');
const TOKENS = read('static/ecosystem/vc-tokens.css');
const VERSION = shellVersion(SHELL);

test('shellVersion reads the VERSION constant that VCShell.version exposes', () => {
  assert.match(VERSION, /^\d+\.\d+\.\d+$/);
  assert.match(SHELL, new RegExp(`vc-shell\\.js v${VERSION.replace(/\./g, '\\.')} `), 'header comment matches VERSION');
  assert.equal(shellVersion('/* no constant */'), null);
});

test('assetVersionLabels / relabelAssets touch only vc-shell.js and vc-tokens.css ?v= labels', () => {
  const html = [
    '<link rel="stylesheet" href="./static/vc-tokens.css?v=1.0.0">',
    '<link rel="stylesheet" href="static/style.css?v=20260930-vc">',
    '<script defer src="%BASE_URL%vc-shell.js?v=1.0.0"></script>',
    '<script defer src="https://hub.test/js/portfolio-held-badges.js?v=20260930-vc"></script>',
    '<!-- vc-shell.js (no label) -->',
  ].join('\n');
  assert.deepEqual(assetVersionLabels(html), [
    { asset: 'vc-tokens.css', version: '1.0.0' },
    { asset: 'vc-shell.js', version: '1.0.0' },
  ]);
  const next = relabelAssets(html, '9.9.9');
  assert.ok(next.includes('href="./static/vc-tokens.css?v=9.9.9"'));
  assert.ok(next.includes('src="%BASE_URL%vc-shell.js?v=9.9.9"'));
  assert.ok(next.includes('static/style.css?v=20260930-vc'), 'other assets keep their own label');
  assert.ok(next.includes('portfolio-held-badges.js?v=20260930-vc'));
  assert.deepEqual(assetVersionLabels('<script defer src="%BASE_URL%vc-shell.js"></script>'), [], 'unlabelled is fine');
});

function fakeWorkspace(label) {
  const workspace = mkdtempSync(join(tmpdir(), 'sync-ecosystem-'));
  const repo = join(workspace, 'index-popup'); // registry vendor: html index.html, dir public, src %BASE_URL%
  mkdirSync(join(repo, 'public'), { recursive: true });
  writeFileSync(join(repo, 'public', 'vc-shell.js'), SHELL);
  writeFileSync(join(repo, 'public', 'vc-tokens.css'), TOKENS);
  const html = `<!doctype html><html><head>${bootBlock(read('static/ecosystem/vc-theme-boot.js'))}
<link rel="stylesheet" href="%BASE_URL%vc-tokens.css${label}" />
<script defer src="%BASE_URL%vc-shell.js${label}"></script>
</head><body><vc-shell tool="index-popup"><a class="hub-link" href="https://hub.test">Value Compass</a></vc-shell></body></html>\n`;
  writeFileSync(join(repo, 'index.html'), html);
  return { workspace, htmlPath: join(repo, 'index.html') };
}
const run = (workspace, ...flags) => spawnSync(process.execPath,
  [SCRIPT, '--workspace', workspace, '--only', 'index-popup', ...flags], { encoding: 'utf8' });

test('verify fails on a stale sibling ?v= label and --write relabels it to VCShell.version', () => {
  const { workspace, htmlPath } = fakeWorkspace('?v=1.0.0');
  try {
    const verify = run(workspace);
    assert.equal(verify.status, 1, verify.stdout + verify.stderr);
    assert.match(verify.stderr, new RegExp(`stale \\?v= label\\(s\\) vc-tokens\\.css\\?v=1\\.0\\.0, vc-shell\\.js\\?v=1\\.0\\.0 \\(VCShell\\.version ${VERSION.replace(/\./g, '\\.')}\\)`));

    const fixed = run(workspace, '--write');
    assert.equal(fixed.status, 0, fixed.stdout + fixed.stderr);
    const html = readFileSync(htmlPath, 'utf8');
    assert.ok(html.includes(`href="%BASE_URL%vc-tokens.css?v=${VERSION}"`));
    assert.ok(html.includes(`src="%BASE_URL%vc-shell.js?v=${VERSION}"`));

    const again = run(workspace);
    assert.equal(again.status, 0, again.stdout + again.stderr);
  } finally {
    rmSync(workspace, { recursive: true, force: true });
  }
});

test('verify accepts unlabelled sibling tags (the ?v= label is optional)', () => {
  const { workspace } = fakeWorkspace('');
  try {
    const verify = run(workspace);
    assert.equal(verify.status, 0, verify.stdout + verify.stderr);
  } finally {
    rmSync(workspace, { recursive: true, force: true });
  }
});
