/* vc-theme-boot v1 (value-invest static/ecosystem/vc-theme-boot.js) — pre-paint theme; inlined by sync-ecosystem, do not edit copies. */
(function () {
  var d = document.documentElement, p = null, s = null, t, v, i;
  var legacy = ['bondmate.theme', 'eiayn:theme:v1', 'spac-hunter-theme', 'preferred-theme', 'nps-theme'];
  try { p = new URLSearchParams(location.search); } catch (e) { p = null; }
  try {
    s = localStorage.getItem('theme');
    if (s === null) {
      for (i = 0; i < legacy.length && s === null; i++) {
        v = localStorage.getItem(legacy[i]);
        if (v && v.charAt(0) === '"') { try { v = JSON.parse(v); } catch (e) { v = null; } }
        if (v === 'light' || v === 'dark') { s = v; localStorage.setItem('theme', v); }
      }
    }
  } catch (e) { s = null; }
  t = p && p.get('theme');
  if (t !== 'light' && t !== 'dark') t = s === 'light' || s === 'dark' ? s : null;
  if (!t) t = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  d.setAttribute('data-theme', t);
  v = p && p.get('embed');
  if ((v !== null && v !== undefined && v !== '0' && v !== 'false') || (p && p.get('headless') === '1')) d.setAttribute('data-embed', v || '');
})();
