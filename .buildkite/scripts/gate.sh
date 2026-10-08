#!/usr/bin/env bash
# Back-pressure gate: block until the target cluster queue has room for this build's jobs.
#
# Releases when  in-flight + JOBS_PER_BUILD <= MAX_RUNNING, where in-flight is
# running jobs plus jobs already released but not yet running (scheduled/reserved/
# assigned/accepted). Gates run one at a time (concurrency group in pipeline.yml),
# so a just-released build's jobs are always counted by the next gate.
# Never fails on API errors; it logs and keeps polling.
#
# Env:
#   GRAPHQL_API_TOKEN  Buildkite API token with GraphQL access (from Buildkite secrets)
#   BK_ORG_SLUG        Organization slug
#   BK_CLUSTER_ID      Cluster GraphQL ID
#   BK_QUEUE_ID        Cluster queue GraphQL ID
#   MAX_RUNNING        Max in-flight jobs on the queue (default 10)
#   JOBS_PER_BUILD     Jobs this build will add to the queue (default 2)
#   POLL_SECONDS       Seconds between checks (default 15)
set -uo pipefail

MAX_RUNNING="${MAX_RUNNING:-10}"
JOBS_PER_BUILD="${JOBS_PER_BUILD:-2}"
POLL_SECONDS="${POLL_SECONDS:-15}"

# A build bigger than the limit can never fit; let it run once the queue is empty.
NEED=$(( JOBS_PER_BUILD < MAX_RUNNING ? JOBS_PER_BUILD : MAX_RUNNING ))

command -v jq >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq jq; }

QUERY='query GateCheck($queue: ID!, $cluster: ID!, $org: ID!) {
  node(id: $queue) {
    ... on ClusterQueue { key metrics { runningJobsCount timestamp } }
  }
  organization(slug: $org) {
    running: jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [RUNNING], type: [COMMAND]) { count }
    queued: jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [SCHEDULED, RESERVED, ASSIGNED, ACCEPTED], type: [COMMAND]) { count }
  }
}'

PAYLOAD=$(jq -n --arg q "$QUERY" --arg queue "$BK_QUEUE_ID" --arg cluster "$BK_CLUSTER_ID" --arg org "$BK_ORG_SLUG" \
  '{query: $q, variables: {queue: $queue, cluster: $cluster, org: $org}}')

while true; do
  RESP=$(curl -sS -X POST https://graphql.buildkite.com/v1 \
    -H "Authorization: Bearer $GRAPHQL_API_TOKEN" -H "Content-Type: application/json" \
    -d "$PAYLOAD" || true)

  METRIC=$(jq -r '.data.node.metrics.runningJobsCount // empty' <<<"$RESP" 2>/dev/null || true)
  LIVE=$(jq -r '.data.organization.running.count // empty' <<<"$RESP" 2>/dev/null || true)
  QUEUED=$(jq -r '.data.organization.queued.count // empty' <<<"$RESP" 2>/dev/null || true)

  if [[ -z "$LIVE" || -z "$QUEUED" ]]; then
    echo "$(date -u +%T) API error, retrying in ${POLL_SECONDS}s: $(head -c 300 <<<"$RESP" | tr -d '\n')"
    sleep "$POLL_SECONDS"; continue
  fi

  # Running: higher of the bucketed queue metric and the live count (conservative).
  RUNNING=$(( ${METRIC:-0} > LIVE ? ${METRIC:-0} : LIVE ))
  INFLIGHT=$(( RUNNING + QUEUED ))
  echo "$(date -u +%T) in-flight: $INFLIGHT (running=$RUNNING [metrics=${METRIC:-n/a} live=$LIVE], queued=$QUEUED) + this build $JOBS_PER_BUILD vs limit $MAX_RUNNING"

  if (( INFLIGHT + NEED <= MAX_RUNNING )); then
    echo "Capacity available, releasing build."
    buildkite-agent annotate --style success --context gate \
      "Gate released at $(date -u +%T) UTC: **$INFLIGHT** in-flight on the target queue + $JOBS_PER_BUILD from this build (limit $MAX_RUNNING)." 2>/dev/null || true
    break
  fi
  echo "At capacity, waiting ${POLL_SECONDS}s..."
  sleep "$POLL_SECONDS"
done
