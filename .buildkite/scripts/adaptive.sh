#!/usr/bin/env bash
# Adaptive parallelism: pick how many test shards to run from free vCPU across the whole org.
#
# Once per run (cached):
#   list every cluster and queue in the org (REST), and each queue's vCPU:
#   hosted queues from their instance shape, self-hosted queues from VCPU_OVERRIDES (else skipped).
# Every poll (job counts only):
#   in-flight vCPU = sum over queues of (running + queued jobs) x queue vCPU
#   util           = in-flight vCPU / MAX_VCPU
#   FIT            = (MAX_VCPU - in-flight vCPU) / target queue vCPU
#   util >= PEAK_UTIL  ->  N = MIN_SHARDS
#   otherwise          ->  N = min(FIT / 2, MAX_SHARDS), at least MIN_SHARDS
# If FIT < MIN_SHARDS, waits and re-polls rather than going over MAX_VCPU.
# Then uploads one test step with `parallelism: N` to TARGET_QUEUE_KEY.
# Runs in a concurrency group (pipeline.adaptive.yml) so two builds never read the same headroom.
#
# Env:
#   GRAPHQL_API_TOKEN  Buildkite API token: GraphQL access, read_builds, read_clusters
#   BK_ORG_SLUG        Organization slug
#   BK_CLUSTER_ID      GraphQL ID of the cluster this pipeline runs in (where TARGET_QUEUE_KEY lives)
#   TARGET_QUEUE_KEY   Queue the test shards run on
#   MAX_VCPU           vCPU budget across the whole org (default 20)
#   PEAK_UTIL          Utilization at or above which builds get MIN_SHARDS (default 0.7)
#   MIN_SHARDS         Fewest shards to run (default 1)
#   MAX_SHARDS         Most shards to run (default 5)
#   VCPU_OVERRIDES     vCPU for self-hosted queues, "key:vcpu key:vcpu ..." (default none: skipped)
#   POLL_SECONDS       Seconds between polls while waiting for MIN_SHARDS to fit (default 15)
set -uo pipefail

MAX_VCPU="${MAX_VCPU:-20}"
PEAK_UTIL="${PEAK_UTIL:-0.7}"
MIN_SHARDS="${MIN_SHARDS:-1}"
MAX_SHARDS="${MAX_SHARDS:-5}"
VCPU_OVERRIDES="${VCPU_OVERRIDES:-}"
POLL_SECONDS="${POLL_SECONDS:-15}"
CHUNK=20   # queues per GraphQL count query

REST="https://api.buildkite.com/v2/organizations/$BK_ORG_SLUG"
AUTH=(-H "Authorization: Bearer $GRAPHQL_API_TOKEN")
WORK=$(mktemp -d)

command -v jq >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq jq; }

# GET every page of a REST list endpoint, printed as one JSON array.
rest_list() {
  local page=1 resp all='[]'
  while :; do
    resp=$(curl -sS "${AUTH[@]}" "$1?per_page=100&page=$page") || return 1
    jq -e 'type == "array"' >/dev/null 2>&1 <<<"$resp" || { echo "REST error on $1: $(head -c 300 <<<"$resp")" >&2; return 1; }
    [[ $(jq length <<<"$resp") == 0 ]] && break
    all=$(jq -s 'add' <(echo "$all") <(echo "$resp"))
    page=$(( page + 1 ))
  done
  echo "$all"
}

# ---- 1. Discover clusters, queues and vCPU (cached for this run) -------------------------------
echo "--- :mag: Discovering clusters and queues"
until CLUSTERS=$(rest_list "$REST/clusters"); do echo "Retrying in ${POLL_SECONDS}s..."; sleep "$POLL_SECONDS"; done

: > "$WORK/queues.tsv"   # cluster_name  cluster_gid  key  queue_gid  vcpu  source
RAW_SHOWN=0
while IFS=$'\t' read -r cid cgid cname; do
  until QUEUES=$(rest_list "$REST/clusters/$cid/queues"); do sleep "$POLL_SECONDS"; done

  if (( ! RAW_SHOWN )) && jq -e 'any(.[]; .hosted)' >/dev/null <<<"$QUEUES"; then
    echo "Raw hosted queue response (vCPU is read from .hosted_agents.instance_shape.cpu):"
    jq '[.[] | select(.hosted)][0] | {key, hosted, hosted_agents}' <<<"$QUEUES"
    RAW_SHOWN=1
  fi

  # \x1f-separated: unlike tabs, empty fields (no cpu) aren't collapsed by read.
  while IFS=$'\x1f' read -r key qgid hosted cpu shape; do
    vcpu="" source=""
    if [[ $hosted == true ]]; then
      if [[ $cpu =~ ^[0-9]+$ ]]; then
        vcpu=$cpu source="instance_shape.cpu"
      elif [[ $shape =~ _([0-9]+)X[0-9]+$ ]]; then   # e.g. LINUX_AMD64_16X64 -> 16
        vcpu=${BASH_REMATCH[1]} source="shape name"
      fi
    else
      for o in $VCPU_OVERRIDES; do [[ ${o%%:*} == "$key" ]] && vcpu=${o#*:} source="VCPU_OVERRIDES"; done
    fi
    if [[ -n $vcpu ]]; then
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$cname" "$cgid" "$key" "$qgid" "$vcpu" "$source" >> "$WORK/queues.tsv"
    else
      echo "Skipping $cname / $key: $([[ $hosted == true ]] && echo "hosted, unknown shape '$shape'" || echo "self-hosted, not in VCPU_OVERRIDES")"
    fi
  done < <(jq -r '.[] | [.key, .graphql_id, (.hosted|tostring), (.hosted_agents.instance_shape.cpu // "" | tostring), (.hosted_agents.instance_shape.name // "")] | join("\u001f")' <<<"$QUEUES")
done < <(jq -r '.[] | [.id, .graphql_id, .name] | @tsv' <<<"$CLUSTERS")

COUNTED=$(wc -l < "$WORK/queues.tsv" | tr -d ' ')
echo "Counting $COUNTED queue(s) across $(jq length <<<"$CLUSTERS") cluster(s)."

TARGET_VCPU=$(awk -F'\t' -v c="$BK_CLUSTER_ID" -v k="$TARGET_QUEUE_KEY" '$2==c && $3==k {print $5}' "$WORK/queues.tsv")
if [[ -z "$TARGET_VCPU" ]]; then
  echo "Target queue '$TARGET_QUEUE_KEY' not found in this cluster, or its vCPU is unknown (self-hosted? add it to VCPU_OVERRIDES)." >&2
  exit 1
fi
echo "Target queue $TARGET_QUEUE_KEY: $TARGET_VCPU vCPU per shard."

# The queue this planner job runs on, so its own job isn't counted as load.
SELF_QUEUE="${BUILDKITE_AGENT_META_DATA_QUEUE:-}"
[[ -n $SELF_QUEUE ]] && echo "Excluding this planner job (queue $SELF_QUEUE) from the count."

# ---- 2. Poll job counts until at least MIN_SHARDS fit ------------------------------------------
# Prints "index running queued" per counted queue, or fails on API error.
poll_counts() {
  local start=0 q resp
  while (( start < COUNTED )); do
    q=$(awk -F'\t' -v s="$start" -v n="$CHUNK" 'NR>s && NR<=s+n {
          i=NR-1
          printf "r%d: jobs(first: 1, cluster: \"%s\", clusterQueue: [\"%s\"], state: [RUNNING], type: [COMMAND]) { count }\n", i, $2, $4
          printf "q%d: jobs(first: 1, cluster: \"%s\", clusterQueue: [\"%s\"], state: [SCHEDULED, RESERVED, ASSIGNED, ACCEPTED], type: [COMMAND]) { count }\n", i, $2, $4
        }' "$WORK/queues.tsv")
    resp=$(curl -sS -X POST https://graphql.buildkite.com/v1 "${AUTH[@]}" -H "Content-Type: application/json" \
      -d "$(jq -n --arg q "query Counts { organization(slug: \"$BK_ORG_SLUG\") { $q } }" '{query: $q}')") || return 1
    jq -e '.data.organization' >/dev/null 2>&1 <<<"$resp" || { echo "API error: $(head -c 300 <<<"$resp" | tr -d '\n')" >&2; return 1; }
    jq -r '.data.organization | to_entries[] | "\(.key[1:]) \(.key[0:1]) \(.value.count)"' <<<"$resp"
    start=$(( start + CHUNK ))
  done | awk '{ if ($2=="r") r[$1]=$3; else q[$1]=$3 } END { for (i in r) print i, r[i], q[i] }' | sort -n
}

while :; do
  if ! poll_counts > "$WORK/counts.txt"; then echo "Retrying in ${POLL_SECONDS}s..."; sleep "$POLL_SECONDS"; continue; fi

  # Join counts onto the cached queue table: cluster key vcpu running queued vcpu_used.
  # This planner job is running too; it exits right after the upload, so leave it out.
  awk -F'\t' -v sc="$BK_CLUSTER_ID" -v sq="$SELF_QUEUE" \
      'NR==FNR { split($0, c, " "); run[c[1]]=c[2]; que[c[1]]=c[3]; next }
       { i=FNR-1; r=run[i]; if ($2==sc && $3==sq && r>0) r--
         printf "%s\t%s\t%s\t%d\t%d\t%d\n", $1, $3, $5, r, que[i], (r+que[i])*$5 }' \
    "$WORK/counts.txt" "$WORK/queues.tsv" > "$WORK/usage.tsv"

  INFLIGHT_VCPU=$(awk -F'\t' '{s+=$6} END {print s+0}' "$WORK/usage.tsv")
  HEADROOM=$(( MAX_VCPU - INFLIGHT_VCPU )); (( HEADROOM < 0 )) && HEADROOM=0
  FIT=$(( HEADROOM / TARGET_VCPU ))
  UTIL=$(awk -v a="$INFLIGHT_VCPU" -v b="$MAX_VCPU" 'BEGIN { printf "%.2f", a / b }')
  AT_PEAK=$(awk -v u="$UTIL" -v p="$PEAK_UTIL" 'BEGIN { print (u >= p) ? 1 : 0 }')

  echo "$(date -u +%T) org in-flight ${INFLIGHT_VCPU}/${MAX_VCPU} vCPU (util $UTIL) | headroom $HEADROOM vCPU = $FIT shard(s) of $TARGET_VCPU vCPU"
  awk -F'\t' '$4+$5 > 0 { printf "    %s / %s: %d running, %d queued x %d vCPU = %d vCPU\n", $1, $2, $4, $5, $3, $6 }' "$WORK/usage.tsv"

  (( FIT >= MIN_SHARDS )) && break
  echo "Not enough headroom for MIN_SHARDS=$MIN_SHARDS, waiting ${POLL_SECONDS}s..."
  sleep "$POLL_SECONDS"
done

# ---- 3. Choose N --------------------------------------------------------------------------------
if (( AT_PEAK )); then
  N=$MIN_SHARDS
  RULE="util $UTIL >= PEAK_UTIL $PEAK_UTIL, so MIN_SHARDS = $MIN_SHARDS"
else
  HALF=$(( FIT / 2 ))
  N=$(( HALF < MAX_SHARDS ? HALF : MAX_SHARDS ))
  (( N < MIN_SHARDS )) && N=$MIN_SHARDS
  RULE="util $UTIL < PEAK_UTIL $PEAK_UTIL, so min(FIT/2 = $HALF, MAX_SHARDS = $MAX_SHARDS), at least MIN_SHARDS = $MIN_SHARDS"
fi
SHARD_SECONDS=$(( 20 + 120 / N ))   # fake per-shard startup cost + an even split of 120s of work
echo "Chose $N shard(s) of ${SHARD_SECONDS}s each: $RULE"

# ---- 4. Upload the test step and annotate -------------------------------------------------------
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
buildkite-agent meta-data set "adaptive-inflight-vcpu" "$INFLIGHT_VCPU"

{
  echo "### :abacus: ${N} shard(s) on \`${TARGET_QUEUE_KEY}\`"
  echo
  echo "Org in-flight: **${INFLIGHT_VCPU} / ${MAX_VCPU} vCPU** (util **${UTIL}**). Headroom: ${HEADROOM} vCPU = ${FIT} shard(s) of ${TARGET_VCPU} vCPU."
  echo
  echo "Rule: ${RULE}."
  echo
  [[ -n $SELF_QUEUE ]] && echo "Counts exclude this planner job on \`${SELF_QUEUE}\`." && echo
  echo
  echo "| Cluster | Queue | vCPU | Running | Queued | vCPU used |"
  echo "|---|---|---|---|---|---|"
  awk -F'\t' -v t="$TARGET_QUEUE_KEY" '$4+$5 > 0 || $2 == t { printf "| %s | %s | %d | %d | %d | %d |\n", $1, $2, $3, $4, $5, $6 }' "$WORK/usage.tsv"
  echo "| **Org total** | | | | | **${INFLIGHT_VCPU}** |"
  echo
  echo "$(awk -F'\t' '$4+$5 == 0 && $2 != "'"$TARGET_QUEUE_KEY"'"' "$WORK/usage.tsv" | wc -l | tr -d ' ') other counted queue(s) were idle. Measured at $(date -u +%T) UTC."
} | buildkite-agent annotate --style info --context adaptive
