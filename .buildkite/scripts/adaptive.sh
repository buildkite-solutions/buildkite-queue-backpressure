#!/usr/bin/env bash
# Adaptive parallelism: pick how many test shards to run from free capacity on the target queue.
#
#   in-flight vCPU = (running + queued jobs on the target queue) x QUEUE_VCPU
#   headroom       = MAX_VCPU - in-flight vCPU
#   N              = headroom / QUEUE_VCPU, clamped to [MIN_SHARDS, MAX_SHARDS]
#
# Then uploads one test step with `parallelism: N` on the target queue.
# If headroom can't fit even MIN_SHARDS, waits and re-checks rather than going over MAX_VCPU.
# Runs in a concurrency group (pipeline.adaptive.yml) so two builds never read the same headroom.
#
# Env:
#   GRAPHQL_API_TOKEN  Buildkite API token with GraphQL access (from Buildkite secrets)
#   BK_ORG_SLUG        Organization slug
#   BK_CLUSTER_ID      Cluster GraphQL ID
#   BK_QUEUE_ID        Target cluster queue GraphQL ID
#   TARGET_QUEUE_KEY   Target queue key, for the uploaded test step
#   QUEUE_VCPU         vCPU per job on the target queue (default 4)
#   MAX_VCPU           vCPU budget for the target queue (default 40)
#   MIN_SHARDS         Fewest shards to run (default 1)
#   MAX_SHARDS         Most shards to run (default 8)
#   POLL_SECONDS       Seconds between checks while waiting for MIN_SHARDS to fit (default 15)
set -uo pipefail

QUEUE_VCPU="${QUEUE_VCPU:-4}"
MAX_VCPU="${MAX_VCPU:-40}"
MIN_SHARDS="${MIN_SHARDS:-1}"
MAX_SHARDS="${MAX_SHARDS:-8}"
POLL_SECONDS="${POLL_SECONDS:-15}"

command -v jq >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq jq; }

# Same query as gate.sh.
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
  INFLIGHT_VCPU=$(( INFLIGHT * QUEUE_VCPU ))
  HEADROOM=$(( MAX_VCPU - INFLIGHT_VCPU ))
  (( HEADROOM < 0 )) && HEADROOM=0
  FIT=$(( HEADROOM / QUEUE_VCPU ))
  echo "$(date -u +%T) in-flight: $INFLIGHT jobs = ${INFLIGHT_VCPU} vCPU (running=$RUNNING, queued=$QUEUED) | headroom ${HEADROOM}/${MAX_VCPU} vCPU = $FIT shard(s)"

  if (( FIT >= MIN_SHARDS )); then
    break
  fi
  echo "Not enough headroom for MIN_SHARDS=$MIN_SHARDS, waiting ${POLL_SECONDS}s..."
  sleep "$POLL_SECONDS"
done

N=$(( FIT < MAX_SHARDS ? FIT : MAX_SHARDS ))
SHARD_SECONDS=$(( 20 + 120 / N ))   # fake per-shard startup cost + an even split of 120s of work
echo "Chose $N shard(s) of ${SHARD_SECONDS}s each."

# \$\$ becomes $$ in the uploaded YAML, which Buildkite turns into a runtime $.
buildkite-agent pipeline upload <<YAML
steps:
  - label: ":test_tube: Tests"
    agents:
      queue: "${TARGET_QUEUE_KEY}"
    parallelism: ${N}
    command: 'echo "Shard \$\$((BUILDKITE_PARALLEL_JOB + 1)) of \$\$BUILDKITE_PARALLEL_JOB_COUNT"; sleep ${SHARD_SECONDS}'
YAML

buildkite-agent meta-data set "adaptive-shards" "$N"
buildkite-agent annotate --style info --context adaptive \
  "**${N} shard(s)** chosen at $(date -u +%T) UTC. In-flight on ${TARGET_QUEUE_KEY}: ${INFLIGHT} jobs = **${INFLIGHT_VCPU} vCPU**. Headroom: **${HEADROOM} of ${MAX_VCPU} vCPU**. ${QUEUE_VCPU} vCPU per shard, shards clamped to ${MIN_SHARDS}-${MAX_SHARDS}."
