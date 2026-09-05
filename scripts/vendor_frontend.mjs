import { mkdirSync, copyFileSync } from 'node:fs';
import { resolve } from 'node:path';

const destination = resolve('static/js/vendor');
mkdirSync(destination, { recursive: true });
for (const [source, name] of [
  ['marked/lib/marked.umd.js', 'marked.js'],
  ['marked/LICENSE', 'marked.LICENSE'],
  ['dompurify/dist/purify.min.js', 'purify.js'],
  ['dompurify/LICENSE', 'dompurify.LICENSE'],
]) copyFileSync(resolve('node_modules', source), resolve(destination, name));
