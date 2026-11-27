package store

// The SQLite database is a materialised view of the raft log: it is rebuilt
// from the latest raft snapshot plus the log on every start, so the schema can
// change freely between releases (bump schemaVersion; snapshots from an older
// schema are migrated in migrate()).
const schemaVersion = 1

const schema = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS components (
  id               TEXT PRIMARY KEY,
  raw_level        INTEGER NOT NULL DEFAULT -1,
  auto_level       INTEGER NOT NULL DEFAULT 2,
  auto_status      TEXT    NOT NULL DEFAULT 'operational',
  auto_since       INTEGER NOT NULL DEFAULT 0,
  automated        INTEGER NOT NULL DEFAULT 0,
  effective_status TEXT    NOT NULL DEFAULT 'operational',
  effective_source TEXT    NOT NULL DEFAULT 'auto',
  effective_since  INTEGER NOT NULL DEFAULT 0,
  signals_json     TEXT,
  fresh_age        REAL,
  fresh_level      INTEGER,
  fresh_ts         INTEGER,
  last_tick        INTEGER NOT NULL DEFAULT 0,
  override_status  TEXT,
  override_reason  TEXT,
  override_by      TEXT,
  override_set     INTEGER,
  override_expires INTEGER
);

-- Effective status history: one row per change (exact uptime, tiny table).
CREATE TABLE IF NOT EXISTS status_changes (
  id        INTEGER PRIMARY KEY,
  component TEXT    NOT NULL,
  ts        INTEGER NOT NULL,
  status    TEXT    NOT NULL,
  source    TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS status_changes_comp_ts ON status_changes(component, ts);

-- Seconds spent in each effective status per UTC day (uptime bars).
-- status 'no_data' accounts for evaluated-but-unknown time.
CREATE TABLE IF NOT EXISTS daily (
  component TEXT NOT NULL,
  day       TEXT NOT NULL,
  status    TEXT NOT NULL,
  seconds   REAL NOT NULL,
  PRIMARY KEY (component, day, status)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS incidents (
  id             TEXT PRIMARY KEY,
  title          TEXT    NOT NULL,
  impact         TEXT    NOT NULL,
  state          TEXT    NOT NULL,
  created        INTEGER NOT NULL,
  updated        INTEGER NOT NULL,
  resolved       INTEGER NOT NULL DEFAULT 0,
  postmortem_url TEXT    NOT NULL DEFAULT '',
  author         TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS incidents_created ON incidents(created);

CREATE TABLE IF NOT EXISTS incident_updates (
  id          TEXT PRIMARY KEY,
  incident_id TEXT    NOT NULL,
  ts          INTEGER NOT NULL,
  state       TEXT    NOT NULL,
  body        TEXT    NOT NULL,
  author      TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS incident_updates_inc ON incident_updates(incident_id, ts);

CREATE TABLE IF NOT EXISTS incident_components (
  incident_id TEXT NOT NULL,
  component   TEXT NOT NULL,
  PRIMARY KEY (incident_id, component)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS maintenances (
  id        TEXT PRIMARY KEY,
  title     TEXT    NOT NULL,
  body      TEXT    NOT NULL DEFAULT '',
  starts    INTEGER NOT NULL,
  ends      INTEGER NOT NULL,
  cancelled INTEGER NOT NULL DEFAULT 0,
  created   INTEGER NOT NULL,
  updated   INTEGER NOT NULL,
  author    TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS maintenances_window ON maintenances(starts, ends);

CREATE TABLE IF NOT EXISTS maintenance_components (
  maintenance_id TEXT NOT NULL,
  component      TEXT NOT NULL,
  PRIMARY KEY (maintenance_id, component)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS announcements (
  id      TEXT PRIMARY KEY,
  level   TEXT    NOT NULL,
  title   TEXT    NOT NULL,
  body    TEXT    NOT NULL DEFAULT '',
  starts  INTEGER NOT NULL,
  ends    INTEGER NOT NULL DEFAULT 0,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  author  TEXT    NOT NULL DEFAULT ''
);

-- Last status each webhook was told about, per component (written by the leader
-- after a successful delivery, so a new leader continues where the old one stopped).
CREATE TABLE IF NOT EXISTS webhook_state (
  webhook   TEXT    NOT NULL,
  component TEXT    NOT NULL,
  status    TEXT    NOT NULL,
  ts        INTEGER NOT NULL,
  PRIMARY KEY (webhook, component)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS audit (
  id     INTEGER PRIMARY KEY,
  ts     INTEGER NOT NULL,
  actor  TEXT    NOT NULL,
  action TEXT    NOT NULL,
  target TEXT    NOT NULL,
  detail TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_ts ON audit(ts);
`
