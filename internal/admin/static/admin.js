// Minimal progressive enhancement for the control panel (no inline JS: strict CSP).
document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll('form[data-confirm]').forEach(function (f) {
    f.addEventListener('submit', function (e) { if (!confirm(f.dataset.confirm)) e.preventDefault(); });
  });
  document.querySelectorAll('a[data-back]').forEach(function (a) {
    a.addEventListener('click', function (e) { if (history.length > 1) { e.preventDefault(); history.back(); } });
  });
  // "all in this group" toggles
  document.querySelectorAll('input[data-all]').forEach(function (cb) {
    cb.addEventListener('change', function () {
      cb.closest('details').querySelectorAll('input[name=components]').forEach(function (x) { x.checked = cb.checked; });
    });
  });
  // Show the local-time equivalent next to UTC inputs.
  function hint(inp) {
    var s = inp.parentNode.querySelector('.localhint');
    if (!s) { s = document.createElement('span'); s.className = 'localhint muted'; inp.parentNode.appendChild(s); }
    if (!inp.value) { s.textContent = ''; return; }
    var d = new Date(inp.value + ':00Z');
    s.textContent = isNaN(d) ? '' : 'Your local time: ' + d.toLocaleString();
  }
  document.querySelectorAll('input[data-utc]').forEach(function (i) { hint(i); i.addEventListener('input', function () { hint(i); }); });
  var n = document.querySelector('[data-now]');
  if (n) n.textContent = 'Your time zone: ' + Intl.DateTimeFormat().resolvedOptions().timeZone;
});
