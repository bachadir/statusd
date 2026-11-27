// Public status page. Plain JS, no dependencies. All API text goes in via textContent.
(function () {
  'use strict';
  var CFG = window.STATUS_CONFIG || {};
  var API = (CFG.apiBase || '').replace(/\/$/, '');
  var REFRESH = (CFG.refreshSeconds || 60) * 1000;
  var STALE_AFTER = (CFG.staleAfterSeconds || 300) * 1000;

  var ICONS = { operational: '✓', degraded_performance: '!', partial_outage: '!', major_outage: '✕', under_maintenance: '⚙' };
  var LABELS = {
    operational: 'Operational', degraded_performance: 'Degraded performance', partial_outage: 'Partial outage',
    major_outage: 'Major outage', under_maintenance: 'Under maintenance'
  };

  function h(tag, attrs) {
    var el = document.createElement(tag);
    if (attrs) for (var k in attrs) {
      if (attrs[k] == null || attrs[k] === false) continue;
      if (k === 'class') el.className = attrs[k];
      else if (k === 'text') el.textContent = attrs[k];
      else el.setAttribute(k, attrs[k]);
    }
    for (var i = 2; i < arguments.length; i++) {
      var c = arguments[i];
      if (c == null || c === false) continue;
      if (Array.isArray(c)) c.forEach(function (x) { if (x != null) el.appendChild(typeof x === 'string' ? document.createTextNode(x) : x); });
      else el.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    }
    return el;
  }
  function clear(el) { while (el.firstChild) el.removeChild(el.firstChild); return el; }
  function $(id) { return document.getElementById(id); }

  function fmtTime(iso) {
    if (!iso) return '';
    var d = new Date(iso);
    return d.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  }
  function ago(ms) {
    var s = Math.max(0, Math.round(ms / 1000));
    if (s < 45) return 'a few seconds ago';
    if (s < 90) return 'a minute ago';
    var m = Math.round(s / 60); if (m < 60) return m + ' minutes ago';
    var hr = Math.round(m / 60); if (hr < 36) return hr + (hr === 1 ? ' hour ago' : ' hours ago');
    var d = Math.round(hr / 24); return d + ' days ago';
  }
  function statusBadge(st) {
    return h('span', { class: 'status s-' + st }, h('span', { class: 'icon', 'aria-hidden': 'true', text: ICONS[st] || '?' }), LABELS[st] || st);
  }

  function getJSON(path) {
    return fetch(API + path, { headers: { Accept: 'application/json' }, cache: 'no-cache' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  // ---------- status page ----------
  var last = null;       // last good summary
  var lastFetchOk = 0;   // when we last got a good answer
  var uptime = null;
  var openGroups = {};   // remember groups the user expanded

  function renderStale(fetchFailed) {
    var el = $('stale');
    var asOf = last && last.data_as_of ? new Date(last.data_as_of).getTime() : 0;
    var tooOld = !asOf || Date.now() - asOf > STALE_AFTER || (last && last.stale);
    if (!fetchFailed && !tooOld) { el.hidden = true; return; }
    el.hidden = false;
    clear(el).appendChild(h('b', { text: 'Status data may be out of date.' }));
    el.appendChild(h('p', {
      text: asOf ? 'The information below was last updated ' + ago(Date.now() - asOf) + ' (' + fmtTime(last.data_as_of) + '). We are working to restore live updates.'
                 : 'We could not load the current status. Please try again shortly.'
    }));
  }

  function renderOverall(s) {
    var el = $('overall');
    el.className = 'overall s-' + s.overall.status;
    clear(el);
    el.appendChild(h('span', { class: 'icon', 'aria-hidden': 'true', text: ICONS[s.overall.status] || '' }));
    var asOf = s.data_as_of ? new Date(s.data_as_of).getTime() : 0;
    el.appendChild(h('div', null, h('h1', { text: s.overall.description }),
      h('p', { class: 'updated', text: asOf ? 'Updated ' + ago(Date.now() - asOf) : '' })));
  }

  function renderAnnouncements(s) {
    var el = clear($('announcements'));
    s.announcements.forEach(function (a) {
      el.appendChild(h('div', { class: 'notice ' + a.level, role: a.level === 'critical' ? 'alert' : null },
        h('b', { text: a.title }), a.body ? h('p', { text: a.body }) : null));
    });
  }

  function incidentCard(i, showAll) {
    var ups = showAll ? i.updates : i.updates.slice(0, 3);
    return h('article', { class: 'card inc ' + i.impact },
      h('h3', { text: i.title }),
      h('div', { class: 'meta', text: (i.state.charAt(0).toUpperCase() + i.state.slice(1)) + ' · started ' + fmtTime(i.created) + (i.resolved ? ' · resolved ' + fmtTime(i.resolved) : '') }),
      h('ul', { class: 'updates' }, ups.map(function (u) {
        return h('li', null, h('b', { text: u.state }), ' ', h('span', { class: 'muted', text: '— ' + fmtTime(u.ts) }), h('p', { text: u.body }));
      })),
      i.components.length ? h('div', { class: 'affected', text: 'Affected: ' + i.components.map(function (c) { return c.group + ' › ' + c.name; }).join(', ') }) : null,
      i.postmortem_url ? h('div', { class: 'affected' }, h('a', { href: i.postmortem_url, rel: 'noopener', text: 'Post-incident report' })) : null);
  }

  function maintCard(m) {
    return h('article', { class: 'card mnt' },
      h('h3', { text: m.title }),
      h('div', { class: 'meta', text: (m.state === 'in_progress' ? 'In progress' : m.state === 'completed' ? 'Completed' : 'Scheduled') + ' · ' + fmtTime(m.starts) + ' – ' + fmtTime(m.ends) }),
      m.body ? h('p', { text: m.body }) : null,
      m.components.length ? h('div', { class: 'affected', text: 'Affected: ' + m.components.map(function (c) { return c.group + ' › ' + c.name; }).join(', ') }) : null);
  }

  function renderIncidents(s) {
    var el = clear($('incidents'));
    if (!s.incidents.active.length) { el.className = ''; return; }
    el.className = 'block';
    el.appendChild(h('h2', { text: 'Active incidents' }));
    s.incidents.active.forEach(function (i) { el.appendChild(incidentCard(i, false)); });
  }

  function renderMaintenance(s) {
    var el = clear($('maintenance'));
    var a = s.maintenances.active, u = s.maintenances.upcoming;
    if (!a.length && !u.length) { el.className = ''; return; }
    el.className = 'block';
    if (a.length) { el.appendChild(h('h2', { text: 'Maintenance in progress' })); a.forEach(function (m) { el.appendChild(maintCard(m)); }); }
    if (u.length) { el.appendChild(h('h2', { text: 'Upcoming maintenance' })); u.forEach(function (m) { el.appendChild(maintCard(m)); }); }
  }

  function freshText(f) {
    if (!f) return null;
    return h('span', { class: 'fresh' + (f.stale ? ' stale' : ''), title: 'Data last updated ' + fmtTime(f.updated_at),
      text: 'Data updated ' + ago(f.age_seconds * 1000 + (Date.now() - lastFetchOk)) });
  }

  function uptimeFor(id) {
    if (!uptime) return null;
    for (var g = 0; g < uptime.groups.length; g++)
      for (var c = 0; c < uptime.groups[g].components.length; c++)
        if (uptime.groups[g].components[c].id === id) return uptime.groups[g].components[c];
    return null;
  }

  function bars(id) {
    var u = uptimeFor(id);
    if (!u || u.uptime_pct == null) return null;
    var wrap = h('div', null);
    var b = h('div', { class: 'bars', role: 'img', 'aria-label': uptime.days + '-day uptime ' + u.uptime_pct.toFixed(2) + '%' });
    u.days.forEach(function (d) {
      var t = d.date + ': ' + (d.uptime_pct == null ? 'no data' : d.uptime_pct.toFixed(2) + '% uptime' + (d.worst && d.worst !== 'operational' ? ', ' + LABELS[d.worst] : ''));
      b.appendChild(h('i', { class: d.worst ? 's-' + d.worst : '', title: t }));
    });
    wrap.appendChild(b);
    wrap.appendChild(h('div', { class: 'barlegend' }, h('span', { text: uptime.days + ' days ago' }), h('span', { text: u.uptime_pct.toFixed(2) + '% uptime' }), h('span', { text: 'Today' })));
    return wrap;
  }

  function renderGroups(s) {
    var el = clear($('groups'));
    s.groups.forEach(function (g) {
      var open = openGroups[g.slug] !== undefined ? openGroups[g.slug] : g.status !== 'operational';
      var d = h('details', { class: 'group', open: open ? 'open' : null },
        h('summary', null, h('span', { class: 'gname', text: g.name }), statusBadge(g.status)),
        h('ul', { class: 'components' }, g.components.map(function (c) {
          return h('li', null,
            h('div', { class: 'crow' },
              h('span', null, h('span', { class: 'cname', text: c.name }), freshText(c.freshness)),
              statusBadge(c.status)),
            bars(c.id));
        })));
      d.addEventListener('toggle', function () { openGroups[g.slug] = d.open; });
      el.appendChild(d);
    });
  }

  function renderRecent(s) {
    var el = clear($('recent'));
    el.appendChild(h('h2', { text: 'Past incidents' }));
    if (!s.incidents.recent.length) el.appendChild(h('p', { class: 'empty', text: 'No incidents in the last 7 days.' }));
    s.incidents.recent.forEach(function (i) { el.appendChild(incidentCard(i, false)); });
    if (s.maintenances.recent.length) {
      el.appendChild(h('h2', { text: 'Recently completed maintenance' }));
      s.maintenances.recent.forEach(function (m) { el.appendChild(maintCard(m)); });
    }
    el.appendChild(h('p', { class: 'muted', text: s.incidents.count_30d + ' incident(s) and ' + s.maintenances.count_30d + ' maintenance window(s) in the last 30 days. ' },
      h('a', { href: 'history.html', text: 'View history' })));
  }

  function render(s) {
    document.title = (s.overall.status === 'operational' ? '' : '⚠ ') + (s.page.name || 'Status');
    renderOverall(s); renderAnnouncements(s); renderIncidents(s); renderMaintenance(s); renderGroups(s); renderRecent(s);
  }

  function refresh() {
    getJSON('/api/v1/status.json').then(function (s) {
      last = s; lastFetchOk = Date.now(); render(s); renderStale(false);
    }).catch(function () {
      if (last) render(last);
      renderStale(true);
    });
  }

  function refreshUptime() {
    getJSON('/api/v1/uptime.json').then(function (u) { uptime = u; if (last) renderGroups(last); }).catch(function () {});
  }

  // ---------- history page ----------
  function historyPage() {
    var params = new URLSearchParams(location.search);
    var month = params.get('month') || '';
    var q = month ? '?month=' + encodeURIComponent(month) : '';
    Promise.all([getJSON('/api/v1/incidents.json' + q), getJSON('/api/v1/maintenances.json' + q)]).then(function (r) {
      var inc = r[0], mnt = r[1];
      var d = new Date(inc.month + '-01T00:00:00Z');
      $('month').textContent = d.toLocaleString(undefined, { month: 'long', year: 'numeric', timeZone: 'UTC' });
      $('prev').href = '?month=' + inc.prev;
      if (inc.next) { $('next').hidden = false; $('next').href = '?month=' + inc.next; }
      var il = clear(document.querySelector('#hist-incidents .list'));
      if (!inc.items.length) il.appendChild(h('p', { class: 'empty', text: 'No incidents reported this month.' }));
      inc.items.forEach(function (i) { il.appendChild(incidentCard(i, true)); });
      var ml = clear(document.querySelector('#hist-maint .list'));
      if (!mnt.items.length) ml.appendChild(h('p', { class: 'empty', text: 'No maintenance this month.' }));
      mnt.items.forEach(function (m) { ml.appendChild(maintCard(m)); });
    }).catch(function () {
      clear(document.querySelector('#hist-incidents .list')).appendChild(h('div', { class: 'notice warn', text: 'History could not be loaded. Please try again shortly.' }));
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    var tz = $('tz');
    if (tz) try { tz.textContent = Intl.DateTimeFormat().resolvedOptions().timeZone; } catch (e) { tz.textContent = 'local'; }
    if (document.body.dataset.page === 'history') { historyPage(); return; }
    refresh(); refreshUptime();
    setInterval(refresh, REFRESH);
    setInterval(refreshUptime, 10 * REFRESH);
    // Keep "updated X ago" honest between refreshes, and refresh when the tab returns.
    setInterval(function () { if (last) { renderOverall(last); renderStale(Date.now() - lastFetchOk > 2 * REFRESH); } }, 15000);
    document.addEventListener('visibilitychange', function () { if (!document.hidden) refresh(); });
  });
})();
