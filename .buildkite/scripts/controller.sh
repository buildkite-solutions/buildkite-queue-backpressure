#!/usr/bin/env bash
# Capacity controller: unblock block-gated builds while the target cluster queue has room.
#
# Every POLL_SECONDS:
#   1. in-flight = running + queued (scheduled/reserved/assigned/accepted) jobs on the queue
#   2. list blocked builds in TARGET_PIPELINES, oldest first
#   3. unblock builds while in-flight + JOBS_PER_BUILD <= MAX_RUNNING
# A single controller hands out capacity, so builds never race each other.
# Runs for RUN_MINUTES, then exits 0. API errors are logged and retried.
#
# Env:
#   GRAPHQL_API_TOKEN  Buildkite API token: GraphQL access, read_builds, write_builds
#   BK_ORG_SLUG        Organization slug
#   BK_CLUSTER_ID      Cluster GraphQL ID
#   BK_QUEUE_ID        Cluster queue GraphQL ID
#   TARGET_PIPELINES   Space-separated pipeline slugs whose blocked builds to manage
#   MAX_RUNNING        Max in-flight jobs on the queue (default 10)
#   JOBS_PER_BUILD     Jobs each unblocked build adds to the queue (default 2)
#   POLL_SECONDS       Seconds between checks (default 10)
#   RUN_MINUTES        How long to run before exiting (default 30)
set -uo pipefail

MAX_RUNNING="${MAX_RUNNING:-10}"
JOBS_PER_BUILD="${JOBS_PER_BUILD:-2}"
POLL_SECONDS="${POLL_SECONDS:-10}"
RUN_MINUTES="${RUN_MINUTES:-30}"

# A build bigger than the limit can never fit; let it run once the queue is empty.
NEED=$(( JOBS_PER_BUILD < MAX_RUNNING ? JOBS_PER_BUILD : MAX_RUNNING ))
REST="https://api.buildkite.com/v2/organizations/$BK_ORG_SLUG"
AUTH=(-H "Authorization: Bearer $GRAPHQL_API_TOKEN")

command -v jq >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq jq; }

QUERY='query InFlight($queue: ID!, $cluster: ID!, $org: ID!) {
  node(id: $queue) {
    ... on ClusterQueue { metrics { runningJobsCount } }
  }
  organization(slug: $org) {
    running: jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [RUNNING], type: [COMMAND]) { count }
    queued: jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [SCHEDULED, RESERVED, ASSIGNED, ACCEPTED], type: [COMMAND]) { count }
  }
}'
PAYLOAD=$(jq -n --arg q "$QUERY" --arg queue "$BK_QUEUE_ID" --arg cluster "$BK_CLUSTER_ID" --arg org "$BK_ORG_SLUG" \
  '{query: $q, variables: {queue: $queue, cluster: $cluster, org: $org}}')

# Prints the in-flight job count, or nothing on API error.
in_flight() {
  local resp metric live queued
  resp=$(curl -sS -X POST https://graphql.buildkite.com/v1 "${AUTH[@]}" \
    -H "Content-Type: application/json" -d "$PAYLOAD" || true)
  metric=$(jq -r '.data.node.metrics.runningJobsCount // 0' <<<"$resp" 2>/dev/null || true)
  live=$(jq -r '.data.organization.running.count // empty' <<<"$resp" 2>/dev/null || true)
  queued=$(jq -r '.data.organization.queued.count // empty' <<<"$resp" 2>/dev/null || true)
  if [[ -z "$live" || -z "$queued" ]]; then
    echo "$(date -u +%T) API error (in-flight): $(head -c 300 <<<"$resp" | tr -d '\n')" >&2
    return
  fi
  # Running: higher of the bucketed queue metric and the live count (conservative).
  echo $(( (${metric:-0} > live ? ${metric:-0} : live) + queued ))
}

# Prints "created_at pipeline build_number unblock_url" for each unblockable block job, oldest first.
blocked_builds() {
  local p resp
  for p in $TARGET_PIPELINES; do
    resp=$(curl -sS "${AUTH[@]}" "$REST/pipelines/$p/builds?state=blocked&per_page=100" || true)
    jq -r --arg p "$p" '.[] | . as $b | .jobs[]
      | select(.type == "manual" and .state == "blocked" and .unblockable == true)
      | "\($b.created_at) \($p) \($b.number) \(.unblock_url)"' <<<"$resp" 2>/dev/null \
      || echo "$(date -u +%T) API error (builds for $p): $(head -c 300 <<<"$resp" | tr -d '\n')" >&2
  done | sort
}

DEADLINE=$(( $(date +%s) + RUN_MINUTES * 60 ))
UNBLOCKED_TOTAL=0
echo "Managing: $TARGET_PIPELINES | limit $MAX_RUNNING in-flight, $JOBS_PER_BUILD jobs/build | running ${RUN_MINUTES}m"

while (( $(date +%s) < DEADLINE )); do
  INFLIGHT=$(in_flight)
  if [[ -z "$INFLIGHT" ]]; then sleep "$POLL_SECONDS"; continue; fi

  mapfile -t BLOCKED < <(blocked_builds)
  RELEASED=0
  for entry in "${BLOCKED[@]}"; do
    (( INFLIGHT + NEED <= MAX_RUNNING )) || break
    read -r _ pipeline number url <<<"$entry"
    code=$(curl -sS -o /tmp/unblock.json -w '%{http_code}' -X PUT "${AUTH[@]}" \
      -H "Content-Type: application/json" -d '{}' "$url" || true)
    if [[ "$code" == 200 ]]; then
      echo "$(date -u +%T) unblocked $pipeline #$number (in-flight $INFLIGHT -> $(( INFLIGHT + JOBS_PER_BUILD )))"
      INFLIGHT=$(( INFLIGHT + JOBS_PER_BUILD ))
      RELEASED=$(( RELEASED + 1 ))
    else
      echo "$(date -u +%T) failed to unblock $pipeline #$number (HTTP $code): $(head -c 300 /tmp/unblock.json | tr -d '\n')"
    fi
  done
  UNBLOCKED_TOTAL=$(( UNBLOCKED_TOTAL + RELEASED ))

  WAITING=$(( ${#BLOCKED[@]} - RELEASED ))
  if (( RELEASED > 0 || WAITING > 0 )); then
    echo "$(date -u +%T) in-flight $INFLIGHT/$MAX_RUNNING | released $RELEASED | still blocked $WAITING"
  fi
  sleep "$POLL_SECONDS"
done

echo "Run window over. Unblocked $UNBLOCKED_TOTAL build(s)."
buildkite-agent annotate --style info --context controller \
  "Capacity controller ran ${RUN_MINUTES}m and unblocked **$UNBLOCKED_TOTAL** build(s) (limit $MAX_RUNNING in-flight on kube_local)." 2>/dev/null || true
