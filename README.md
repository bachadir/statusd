# statusd

A self-hosted status page whose component statuses come **automatically from Prometheus**: blackbox probes
voted across regions, PromQL thresholds, data freshness and DNS zone age, all with hysteresis. Staff handle
incidents, maintenances, announcements and overrides in a control panel behind OIDC.

It is built to stay up when your infrastructure doesn't: two full nodes in different datacenters plus a small
witness form a Raft cluster, and the public page is a static site behind a CDN.

```
            DC 1                       DC 2                    3rd site (small VM)
  statusd eu1 (full node)  ◀─raft─▶  statusd us1 (full node)  ◀─raft─▶  statusd witness (vote only)
   collector · evaluator              collector · evaluator
   SQLite (raft FSM)                  SQLite (raft FSM)
   public API · control panel         public API · control panel
        │ tunnel / LB                        │ tunnel / LB
        └──────────── CDN (LB + cache) ──────┘ ◀── static page (web/public)
```

- **One Go binary.** On the **leader**, it queries Prometheus every 60 s, evaluates every catalogue component
  (probe voting across regions, thresholds, freshness, hysteresis), and commits one raft entry per tick.
- **Replication.** Every write is replicated with **Raft** (`hashicorp/raft`) to an SQLite state machine on each
  node. Both full nodes serve the public API and the control panel; followers forward writes to the leader. The
  **witness** gives the two datacenters a quorum and never evaluates.
- **Source-aware leadership.** If the leader can't reach its Prometheus sources but another node can, leadership
  moves to the node that can.
- **Honest failure modes.** A node without quorum, or with old data, fails `/healthz` (so the load balancer stops
  routing to it). It keeps serving its last state with `stale: true`, and the page shows a stale-data banner.
  "No data" is never published as green or red.
- **Stateless frontend.** Plain HTML/CSS/JS (`web/public`, no build step) that fetches `/api/v1/status.json`. A
  CDN only proxies and caches it, with `stale-if-error` as a last resort.
- **Replay.** Run a catalogue change against past Prometheus data and see what the page would have published
  before you ship it.
- **Optional Status.io feed.** Mirror the published statuses to an existing Status.io page.

You don't need the full cluster to start: a single node (`raft.bootstrap: true`, no peers) works fine.

## Quick start

```sh
make demo        # docker: one node + simulated Prometheus; page http://localhost:8090, panel http://localhost:8091
```

The demo runs the example catalogue (`catalog/`) against `fakeprom`, a tiny Prometheus stand-in. Break things to
see the page react within a minute:

```sh
echo 0 > deploy/demo/state/probe          # every probe region fails
echo 0 > deploy/demo/state/value          # every promql signal goes bad
echo 7200 > deploy/demo/state/serial_lag  # the DNS zone is two hours old
rm deploy/demo/state/probe deploy/demo/state/value deploy/demo/state/serial_lag   # back to normal
make demo-down
```

The demo control panel has authentication turned off (`dev_insecure_user`). Never use that outside your machine.

## Development

```sh
make test        # go vet + unit tests
make fakeprom    # Prometheus stand-in on :9090 (or point statusd.dev.yaml at your own Prometheus)
make dev         # one node: page + API on http://127.0.0.1:8080, control panel on :8081
make check       # validate config + catalogue without starting
make ha-test     # 3-node docker cluster (mTLS) + failover / partition / source-loss scenarios
make ha-down
```

| Path | What |
|---|---|
| `cmd/statusd` | main, config, replay |
| `cmd/fakeprom` | Prometheus stand-in for the demo and the HA test |
| `internal/catalog` | catalogue loader and validation |
| `internal/collector` | Prometheus client, endpoint fallback (e.g. direct → Grafana datasource proxy) |
| `internal/eval` | signals → levels, region voting, hysteresis |
| `internal/store` | raft node (mTLS transport), FSM over SQLite, commands, queries |
| `internal/node` | leader loop, write forwarding, leadership transfer, metrics |
| `internal/view` | public documents (summary, history, uptime) |
| `internal/api` | public read-only JSON API + `/healthz` |
| `internal/admin` | control panel (OIDC, roles, CSRF, audit log) |
| `internal/statusio` | optional: push published statuses to a Status.io page |
| `catalog/*.yml` | example catalogue: groups, components and their signals |
| `web/public` | public status page |
| `deploy/` | Helm chart, systemd unit, Cloudflare setup, example configs, demo, HA test |

## The catalogue

One YAML file per group. A component's automatic status is the worst of its signals, after hysteresis. The
example catalogue in `catalog/` uses every feature; replace it with your own services.

```yaml
group: {slug: api, name: "API", order: 20}
defaults:
  hysteresis: {down_after: 3, up_after: 5}     # 3 bad ticks to publish worse, 5 good ticks to recover
components:
  - slug: public
    name: "Public API"
    signals:
      - {name: api-probes, kind: probe_vote, source: prom, check: 'api_.*'}   # 0 regions failing ok, 1 degraded, ≥2 down
      - name: success-ratio
        kind: promql
        source: prom
        expr: 'sum(rate(http_requests_total{job="api",code!~"5.."}[5m])) / sum(rate(http_requests_total{job="api"}[5m]))'
        ok: ">=0.99"
        degraded: ">=0.95"
        empty: ok                               # no traffic is not an outage
  - slug: dns-zone
    name: "DNS Zone"
    signals:
      - name: zone-age                          # oldest SOA serial the probes get
        kind: serial_age
        source: prom
        expr: 'probe_dns_serial{check="dns_soa"}'
        serial_format: unix                     # or yymmddhhmm
        aggregate: oldest                       # oldest (default) | median | freshest
        degraded_after: 20m
        down_after: 1h
        max_level: degraded                     # early warning: never reports down on its own
    freshness:                                  # the data age shown on the page
      {source: prom, expr: 'probe_dns_serial{check="dns_soa"}', serial_format: unix, aggregate: median, degraded_after: 30m, down_after: 3h}
```

**Signal kinds:**
- **`probe_vote`** reads `probe_success` series that carry a `check` label (what is probed) and a `probe_region`
  label (where from), for example blackbox_exporter run in several regions with relabelling. Each region is rated
  on its worst target over a window (`window`, default `10m`); a region fails below `fail_below` (default 0.5).
  One failing region means degraded, two or more mean down, and fewer than `min_regions` (default 2) reporting
  means no data. Short windows turn single-target blips from one region into public episodes.
- **`promql`** is any instant query compared with `ok` and `degraded` thresholds (`>=0.99`, `<2`, …). Anything
  else is down. `empty` decides what "no series" means: `no_data` (default), `ok`, `degraded` or `down`.
- **`serial_age`** turns DNS SOA serials (one series per server or probe target) into ages and aggregates them.

**Other options:**
- **`freshness`** is the data age shown next to a component. Its `expr` returns ages in seconds, or with
  `serial_format`, serials. `affects_status: false` makes it display-only; `public: false` hides it from the
  public page.
- **`max_level: degraded`** on any signal makes it a softer signal: it can degrade a component but never take it down.
- **`degraded_as` / `down_as`** map levels to public statuses. The defaults are degraded performance and major outage.
- **`note`** is an internal note shown in the control panel only. `hidden: true` keeps a component off the page.
- **A component without signals** is set by hand only. It shows as operational until someone changes it.
- `link` on a signal adds a link (e.g. to a Grafana panel) in the control panel.

`statusd -check` validates the catalogue against the config's `sources:`, and `go test ./internal/catalog` guards
the shipped catalogue in CI.

**Status precedence:** a manual override (with or without an expiry) wins. An active maintenance window comes
next, then the automatic status. **Pause automation** in Settings freezes every automatic status.

### Replay before shipping a signal change

`statusd -replay-from` runs the catalogue against past Prometheus data, hysteresis included, and prints what the
page would have published. Each query is fetched once as a range query, so a week takes seconds:

```sh
statusd -config statusd.yaml -replay-from 2026-01-01T00:00:00Z [-replay-to …] \
        -replay-match '^(api|data)/' -replay-signals [-replay-catalog ./my-catalog] [-replay-step 60s]
```

Check both that known incidents publish and that quiet periods stay quiet.

## Managing the catalogue

Keep the catalogue in git and ship it through review and CI.

statusd reads the catalogue **once, at startup**. There is no hot reload, so every change needs a restart.

| Change | Effect |
|---|---|
| Rename `name`, change `order`, edit signals or thresholds | Safe. History and overrides are kept. |
| Add a component | It appears after the restart and is automated from the next tick. |
| Remove a component | It disappears from the page. Its old data stays in the store but is no longer shown. |
| **Change a `slug`** | **It becomes a new component.** Uptime history, open overrides and incident or maintenance links stay on the old ID. Avoid this after go-live. |

- **Both full nodes must have the same catalogue.** Each node builds the public page and the control panel from its
  own copy, so if they differ, the two datacenters show different pages. The witness doesn't need a catalogue.
- **A broken catalogue stops statusd from starting.** That covers unknown keys, duplicate slugs, a `source` missing
  from the config's `sources:`, a bad regex or a bad threshold. Always validate (`statusd -check`) before you deploy.
- **Roll out one node at a time.** Restart the follower first, check it's healthy, then restart the leader. The
  follower takes over in about 10 s. The public page keeps serving throughout, because both nodes serve reads and
  the CDN caches the JSON.

### VMs (systemd)

`deploy/systemd/statusd.service` expects the catalogue in `/etc/statusd/catalog/` (`catalog_dir` in the config).
On each full node, one at a time, follower first:

1. Copy `catalog/*.yml` to `/etc/statusd/catalog/`. Remove files that no longer exist; every `*.yml` in the
   directory is loaded.
2. Validate: `statusd -config /etc/statusd/statusd.yaml -check`.
3. If validation passes, `systemctl restart statusd`. If it fails, restore the previous files and don't restart.
4. Wait for `/healthz` to return 200 before moving to the next node.

### Kubernetes (Helm)

The chart (`deploy/helm/statusd`) takes the catalogue as a value and mounts it from a ConfigMap. Set at most one
of these:

```yaml
catalog:
  files: {}              # 1) the chart creates "<release>-catalog" from these
  existingConfigMap: ""  # 2) a ConfigMap you manage elsewhere (CI, ArgoCD, kustomize)
                         # 3) neither set: the catalogue built into the image (Dockerfile COPY)
```

**Option 1: the chart creates the ConfigMap.**

```sh
make helm-catalog        # validates the catalogue, then writes catalog-values.yaml from catalog/
helm upgrade --install statusd-eu1 deploy/helm/statusd -f deploy/examples/helm-values-eu1.yaml -f catalog-values.yaml
```

**Option 2: you manage the ConfigMap.** It needs one key per `*.yml` file:

```sh
kubectl create configmap statusd-catalog --from-file=catalog/ --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install statusd-eu1 deploy/helm/statusd -f deploy/examples/helm-values-eu1.yaml --set catalog.existingConfigMap=statusd-catalog
```

- The ConfigMap is mounted read-only at `/etc/statusd/catalog`. The chart sets `config.catalog_dir` itself, and
  fails with a clear error if your config points somewhere else.
- **Restarts on change:** the pod carries a hash of the catalogue (`checksum/catalog`), so `helm upgrade` restarts
  it whenever the catalogue changes. If you change a self-managed ConfigMap without `helm upgrade`, run
  `kubectl rollout restart statefulset/<release>`.
- ConfigMaps are limited to 1 MiB, far more than a catalogue needs.

## Deploying

Examples: `deploy/examples/` (systemd configs and Helm values for a full node and the witness),
`deploy/helm/statusd`, `deploy/systemd/statusd.service` and `deploy/cloudflare/README.md`. You need:

- **Raft certificates:** an mTLS CA with node certs carrying SAN `statusd`. `deploy/certs/gen-raft-certs.sh <dir>`
  creates them offline (`NODES="eu1 us1 witness"` by default). Keep the CA key offline.
- **Shared keys:** a cluster key (all members) and a session key (≥ 32 bytes, both full nodes), e.g.
  `openssl rand -base64 32`.
- **An OIDC client** for the control panel (Keycloak, authentik, Dex, Okta, …) with redirect URI
  `https://<panel>/auth/callback`, and a `groups` claim. Full nodes don't start until OIDC discovery works.
- **Network:** raft (TCP 7000) and the internal API (TCP 8443) reachable between all members. Only the public port
  (8080) should be exposed to the internet; keep the control panel (8081) internal.
- **The catalogue** on both full nodes.

In Kubernetes, one Helm release per member. Every statusd pod has a required pod anti-affinity on
`kubernetes.io/hostname` (`antiAffinity` in the values), so two members sharing a cluster never share a node.
`clusterService.loadBalancerSourceRanges` restricts who can reach raft; remember any NAT between datacenters when
you fill it in (a blocked source shows up as `dial tcp …:7000: i/o timeout` in the raft log).

**Where to put the witness:** in a third location if you can. If it shares a site with a full node, losing that
site loses quorum; the other full node then keeps serving its last state read-only, with the stale banner, until
the site is back.

**Public page in the cluster (optional):** `publicUI` in the chart runs a small nginx Deployment (image built from
`web/`) that serves `web/public` and passes `/api/` and `/healthz` to the release's statusd. Same-origin, so no
CORS; useful as a preview before a CDN setup, or instead of one.

### CI

`.github/workflows/ci.yml` runs the tests, validates the example catalogue, lints and renders the chart, and on
pushes to `main` and tags builds `ghcr.io/<owner>/<repo>` and `ghcr.io/<owner>/<repo>/web`.

## Control panel roles

| Role | Default group | Can |
|---|---|---|
| viewer | `status-viewer` | see everything, including the "why" signal breakdown |
| editor | `status-editor` | overrides, incidents, maintenances, announcements |
| admin | `status-admin` | plus deleting incidents and maintenances, and pausing automation |

Group names are configurable (`admin.oidc.*_groups`).

## Public API

| Endpoint | Cache | Content |
|---|---|---|
| `GET /api/v1/status.json` | 30 s | overall status, groups and components (status, source, freshness), announcements, active and recent incidents and maintenances, `data_as_of`, `stale` |
| `GET /api/v1/incidents.json?month=YYYY-MM` | 60 s | incident history |
| `GET /api/v1/incidents/{id}` | 60 s | one incident |
| `GET /api/v1/maintenances.json?month=YYYY-MM` | 60 s | maintenance history |
| `GET /api/v1/uptime.json?days=90` | 300 s | daily uptime per component (major outage counts 100 %, partial 30 %, maintenance excluded) |
| `GET /healthz` | none | 200 only with a raft leader and fresh data (use it as the load balancer health check) |

## Feeding Status.io (optional)

statusd can keep an existing Status.io page in step with its own published statuses: use it instead of the
built-in page, or alongside it while subscribers move over. It's off unless `statusio.enabled` is true.

- **What gets pushed:** for every mapped catalogue component, the raft leader pushes the same status that
  `status.json` publishes (automatic status after hysteresis, overrides, maintenance windows) to one Status.io
  (component, container) pair with `POST /component/status/update`.
- **Status codes:** operational 100, under_maintenance 200, degraded_performance 300, partial_outage 400,
  major_outage 500.
- **The text** is one fixed public sentence per status. Override reasons and signal details are never sent.
- **Incidents and maintenances are not mirrored.** During a maintenance window the components show "Planned
  maintenance" on Status.io, but no incident or maintenance entry is created there.

### Setup

1. **The page ID** is the 24-character hex string in the page's dashboard URL.
2. **API credentials:** the *API ID* and *API key* from the Status.io dashboard. **The key is account-wide**: it can
   change every page in the account. Keep it in a secret store and never in git.
3. **Structure:** Status.io components are statusd groups; containers are statusd components. Statuses are set per
   (component, container) pair, and containers can be shared between components, so the mapping stores both IDs.
4. **Leave the mapped pairs alone in the Status.io dashboard.** statusd owns them while the feed is on; a manual
   change is overwritten at the next reconcile (≤ 10 min) and counted as drift.

```yaml
statusio:
  enabled: true                       # default false: nothing runs
  dry_run: true                       # default true: log and show pushes, send nothing
  statuspage_id: 000000000000000000000000
  api_id_file: /etc/statusd/secrets/statusio-api-id     # or api_id_env: STATUSIO_API_ID
  api_key_file: /etc/statusd/secrets/statusio-api-key   # or api_key_env: STATUSIO_API_KEY
  # Optional, with defaults:
  # base_url: https://api.status.io/v2
  # min_gap: 1s            # Status.io: one request at a time, >= 1 s apart
  # sync_every: 15s        # compare published statuses with Status.io's
  # reconcile_every: 10m   # re-read Status.io's summary (lost pushes, hand edits)
  # timeout: 20s
  # details:               # public text per status; these replace the defaults
  #   major_outage: "We are seeing a service disruption and are investigating."
```

Set the same block on both full nodes. Credential files hold the bare value (`chmod 0400`); they're required only
with `dry_run: false`. statusd refuses to start with a malformed page ID, `dry_run: false` without credentials, or
an unknown `details` key.

### Mapping the catalogue

Each catalogue component carries its pair:

```yaml
  - slug: public
    name: "Public API"
    statusio: {component: 0123456789abcdef01234567, container: fedcba9876543210fedcba98}
```

Unmapped and hidden components are never pushed. To generate the mapping, run `-statusio-map` against a config
with `statuspage_id` and credentials:

```sh
make build
./bin/statusd -config my-statusio.yaml -statusio-map
```

It reads `GET /component/list` and matches by name (groups to components, then components to containers),
ignoring case, punctuation, a plural "s" and `(formerly …)`, and also matching a legacy name in brackets. It
prints the `statusio:` lines to paste into the catalogue, then what it couldn't match on either side.

### Rolling it out

1. Try it on a **test page** in the same account first: point one or two components at it, run with
   `dry_run: false`, and set an override in the control panel.
2. On the real page, start with **`dry_run: true`**: the leader logs what it *would* push
   (`status.io status pushed … dry_run=true`) and the control panel lists it. Compare with `status.json` for a while.
3. Check whether status changes notify your subscribers, then go live with `dry_run: false`. The first sync
   pushes every pair that differs (1 request per second); after that only changes.

### Operating it

- **Control panel overview** shows the page, the mapped components, whether this node publishes (only the leader
  does), pending pushes, the last read, the last error and the latest pushes.
- **Failover:** a new leader reads Status.io's summary before pushing, so nothing is double-posted or missed.
- **Errors:** network errors and 5xx back off (15 s, doubling to 5 min). A 4xx is not retried until that
  component's status changes again. "Mapped pair is not on the page" means the Status.io structure changed:
  regenerate the mapping.
- **Metrics:**

  | Metric | Meaning |
  |---|---|
  | `statusd_statusio_requests_total{endpoint,result}` | `result` = ok, error, dry_run |
  | `statusd_statusio_pending` | pairs that differ from Status.io (leader only) |
  | `statusd_statusio_last_success_timestamp_seconds` | last successful request |
  | `statusd_statusio_drift_total` | hand edits found on Status.io |
  | `statusd_statusio_dry_run` | 1 while only logging |

- **Suggested alerts:**

  ```promql
  # pushes stuck (for: 15m)
  max(statusd_statusio_pending) > 0 and min(statusd_statusio_dry_run) == 0
  # no successful request for 30 min: Status.io unreachable or credentials broken
  time() - max(statusd_statusio_last_success_timestamp_seconds) > 1800 and min(statusd_statusio_dry_run) == 0
  # someone edited a statusd-managed pair in the Status.io dashboard
  increase(statusd_statusio_drift_total[1h]) > 0
  ```

## License

MIT. See [LICENSE](LICENSE).
