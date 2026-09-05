import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

test('배포되는 Markdown 라이브러리는 잠금 파일로 설치한 원본과 일치한다', () => {
  assert.deepEqual(readFileSync('static/js/vendor/marked.js'), readFileSync('node_modules/marked/lib/marked.umd.js'));
  assert.deepEqual(readFileSync('static/js/vendor/purify.js'), readFileSync('node_modules/dompurify/dist/purify.min.js'));
});
