#!/bin/bash
# HA scenarios against the local docker-compose cluster. Each step prints PASS/FAIL.
set -u
cd "$(dirname "$0")"
EU=localhost:18080; US=localhost:28080; EUA=localhost:18081; USA=localhost:28081
fails=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; fails=$((fails+1)); }
field() { curl -s -m 3 "$1/healthz" | python3 -c "import json,sys
try: print(json.load(sys.stdin)['detail']['$2'])
except Exception: print('')"; }
leader() { local l; l=$(field $EU leader); [ -z "$l" ] && l=$(field $US leader); echo "$l"; }
api_of() { case $1 in eu1) echo $EU;; us1) echo $US;; esac; }
adm_of() { case $1 in eu1) echo $EUA;; us1) echo $USA;; esac; }
wait_for() { # wait_for <seconds> <description> <command...>
  local t=$1 d=$2; shift 2; local s=$(date +%s)
  while ! "$@" >/dev/null 2>&1; do
    if [ $(( $(date +%s) - s )) -ge $t ]; then fail "$d (timeout ${t}s)"; return 1; fi; sleep 1; done
  pass "$d ($(( $(date +%s) - s ))s)"; }
healthy() { curl -sf -m 3 "$1/healthz" >/dev/null; }
status_of() { curl -s -m 3 "$1/api/v1/status.json" | python3 -c "import json,sys
d=json.load(sys.stdin); print([c['status'] for g in d['groups'] for c in g['components'] if c['id']=='$2'][0])"; }
csrf() { curl -s "$1/" | grep -o 'name="csrf" value="[^"]*"' | head -1 | sed 's/.*value="//;s/"$//'; }
# Query a node from inside the public network (Docker Desktop's host port forwarding
# is unreliable for a container that has had a network detached).
incurl() { docker run --rm --network ha-test_public curlimages/curl:8.11.1 -s -m 3 "$@"; }
has_announcement() { curl -s "$1/api/v1/status.json" | grep -q "$2"; }

echo "== 0. baseline"
L=$(leader); echo "leader: $L"
[ "$L" = eu1 ] || [ "$L" = us1 ] && pass "a full node leads" || fail "leader is '$L'"
wait_for 20 "both full nodes healthy" bash -c "curl -sf $EU/healthz && curl -sf $US/healthz"

echo "== 1. write through the follower is forwarded to the leader"
F=us1; [ "$L" = us1 ] && F=eu1
FA=$(adm_of $F); C=$(csrf $FA)
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST $FA/announcements --data-urlencode csrf=$C -d level=warning -d title=forwarded-write-$$ -d starts=2020-01-01T00:00)
[ "$code" = 303 ] && pass "follower $F accepted write (HTTP $code)" || fail "follower write HTTP $code"
wait_for 10 "write visible on eu1" has_announcement $EU forwarded-write-$$
wait_for 10 "write visible on us1" has_announcement $US forwarded-write-$$

echo "== 2. automatic detection with hysteresis (probes fail in both DCs)"
echo 0 > state/eu/probe; echo 0 > state/us/probe
wait_for 40 "probed service goes major_outage" bash -c "[ \"\$(curl -s $EU/api/v1/status.json | python3 -c \"import json,sys; d=json.load(sys.stdin); print([c['status'] for g in d['groups'] for c in g['components'] if c['id']=='test/probed'][0])\")\" = major_outage ]"
echo 1 > state/eu/probe; echo 1 > state/us/probe
wait_for 40 "probed service recovers" bash -c "[ \"\$(curl -s $US/api/v1/status.json | python3 -c \"import json,sys; d=json.load(sys.stdin); print([c['status'] for g in d['groups'] for c in g['components'] if c['id']=='test/probed'][0])\")\" = operational ]"

echo "== 3. kill the leader"
L=$(leader); S=us1; [ "$L" = us1 ] && S=eu1
t0=$(date +%s); docker compose stop -t 1 $L >/dev/null 2>&1
wait_for 30 "$S takes over leadership" bash -c "[ \"\$(curl -s -m 2 $(api_of $S)/healthz | grep -o '\"raft_state\":\"Leader\"')\" != '' ]"
wait_for 30 "$S healthy (ticking) after failover" healthy $(api_of $S)
[ "$(field $(api_of $S) leader)" != witness ] && pass "witness did not keep leadership" || fail "witness is leader"
docker compose start $L >/dev/null 2>&1
wait_for 40 "$L rejoins and is healthy" healthy $(api_of $L)
wait_for 10 "$L has replicated state" has_announcement $(api_of $L) forwarded-write-$$

echo "== 4. partition a full node from the cluster"
L=$(leader); P=us1; [ "$L" = us1 ] && P=eu1   # partition the follower
docker network disconnect ha-test_cluster ha-test-$P-1
export -f incurl
wait_for 60 "partitioned $P reports unhealthy (no fresh data)" bash -c "incurl -o /dev/null -w '%{http_code}' http://$P:8080/healthz | grep -q 503"
wait_for 60 "partitioned $P still serves the last state, flagged stale" bash -c "incurl http://$P:8080/api/v1/status.json | grep -q '\"stale\":true'"
healthy $(api_of $L) && pass "majority side ($L) stays healthy" || fail "majority side unhealthy"
docker network connect --alias $P-raft ha-test_cluster ha-test-$P-1
wait_for 40 "$P healthy again after heal" healthy $(api_of $P)

echo "== 5. leader loses its Prometheus source -> leadership moves"
L=$(leader); O=us1; [ "$L" = us1 ] && O=eu1
DC=${L%1}; docker compose stop -t 1 prom-$DC >/dev/null 2>&1
wait_for 90 "leadership moves from $L to $O" bash -c "[ \"\$(curl -s -m 2 $(api_of $O)/healthz | grep -o '\"raft_state\":\"Leader\"')\" != '' ]"
docker compose start prom-$DC >/dev/null 2>&1

echo "== 6. witness loss keeps quorum (2 of 3)"
docker compose stop -t 1 witness >/dev/null 2>&1
L=$(leader); FA=$(adm_of $L); C=$(csrf $FA)
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST $FA/announcements --data-urlencode csrf=$C -d level=info -d title=no-witness-$$ -d starts=2020-01-01T00:00)
[ "$code" = 303 ] && pass "writes still work without the witness" || fail "write without witness HTTP $code"
docker compose start witness >/dev/null 2>&1

echo
[ $fails -eq 0 ] && echo "ALL SCENARIOS PASSED" || echo "$fails FAILURE(S)"
exit $fails
