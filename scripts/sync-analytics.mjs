// Run from any directory. Sibling checkouts live alongside value-invest.
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const projects = JSON.parse(readFileSync(resolve(root, 'config/analytics-projects.json')));
const source = readFileSync(resolve(root, 'static/js/analytics.js'), 'utf8');
const write = process.argv.includes('--write');
let failures = 0;
for (const entry of projects) {
  const repo = resolve(root, '..', entry.project);
  const target = resolve(repo, entry.asset);
  if (write) {
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, source);
  }
  try {
    const html = readFileSync(resolve(repo, entry.html), 'utf8');
    if (readFileSync(target, 'utf8') !== source
      || !html.includes(`src="${entry.src}" data-project="${entry.project}"`)) {
      throw new Error('Analytics asset or entrypoint differs');
    }
    console.log(`OK ${entry.project}`);
  } catch (error) {
    console.error(`${entry.project}: ${error.message}`);
    failures++;
  }
}
process.exitCode = failures ? 1 : 0;
