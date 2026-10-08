#!/usr/bin/env bash
# Back-pressure gate: block until the target cluster queue has fewer than MAX_RUNNING running jobs.
# Never fails on API errors; it logs and keeps polling.
#
# Env:
#   GRAPHQL_API_TOKEN  Buildkite API token with GraphQL access (from Buildkite secrets)
#   BK_ORG_SLUG        Organization slug
#   BK_CLUSTER_ID      Cluster GraphQL ID
#   BK_QUEUE_ID        Cluster queue GraphQL ID
#   MAX_RUNNING        Release when running jobs < this (default 10)
#   POLL_SECONDS       Seconds between checks (default 15)
set -uo pipefail

MAX_RUNNING="${MAX_RUNNING:-10}"
POLL_SECONDS="${POLL_SECONDS:-15}"

command -v jq >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq jq; }

QUERY='query GateCheck($queue: ID!, $cluster: ID!, $org: ID!) {
  node(id: $queue) {
    ... on ClusterQueue { key metrics { runningJobsCount timestamp } }
  }
  organization(slug: $org) {
    jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [RUNNING], type: [COMMAND]) { count }
  }
}'

PAYLOAD=$(jq -n --arg q "$QUERY" --arg queue "$BK_QUEUE_ID" --arg cluster "$BK_CLUSTER_ID" --arg org "$BK_ORG_SLUG" \
  '{query: $q, variables: {queue: $queue, cluster: $cluster, org: $org}}')

while true; do
  RESP=$(curl -sS -X POST https://graphql.buildkite.com/v1 \
    -H "Authorization: Bearer $GRAPHQL_API_TOKEN" -H "Content-Type: application/json" \
    -d "$PAYLOAD" || true)

  METRIC=$(jq -r '.data.node.metrics.runningJobsCount // empty' <<<"$RESP" 2>/dev/null || true)
  LIVE=$(jq -r '.data.organization.jobs.count // empty' <<<"$RESP" 2>/dev/null || true)

  if [[ -z "$METRIC" && -z "$LIVE" ]]; then
    echo "$(date -u +%T) API error, retrying in ${POLL_SECONDS}s: $(head -c 300 <<<"$RESP" | tr -d '\n')"
    sleep "$POLL_SECONDS"; continue
  fi

  # Take the higher of the bucketed queue metric and the live job count (conservative).
  RUNNING=$(( ${METRIC:-0} > ${LIVE:-0} ? ${METRIC:-0} : ${LIVE:-0} ))
  echo "$(date -u +%T) running jobs: $RUNNING (metrics=${METRIC:-n/a}, live=${LIVE:-n/a}, limit=$MAX_RUNNING)"

  if (( RUNNING < MAX_RUNNING )); then
    echo "Capacity available, releasing build."
    buildkite-agent annotate --style success --context gate \
      "Gate released at $(date -u +%T) UTC with **$RUNNING** running jobs on kube_local (limit $MAX_RUNNING)." 2>/dev/null || true
    break
  fi
  echo "At capacity, waiting ${POLL_SECONDS}s..."
  sleep "$POLL_SECONDS"
done
