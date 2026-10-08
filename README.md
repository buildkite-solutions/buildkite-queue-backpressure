# Hosted agents back-pressure demo

Keep Buildkite hosted agents usage within a limit, either by holding new builds back until there's room, or by sizing each build to the capacity that's free right now.

This is useful when you want a ceiling on concurrent compute (a cost budget, or headroom kept free for other teams), or when a queue sits in front of something with limited capacity, such as a license pool, a shared test environment or a rate-limited downstream service. Builds wait, or shrink, in Buildkite rather than piling up downstream.

The examples run on Buildkite hosted agents. The same scripts work with self-hosted agents.

## Approaches

The repo has working examples of each approach, all built on the Buildkite APIs:

| | A. Self-gated | B. Block step | C. Adaptive parallelism |
|---|---|---|---|
| What it controls | When a build starts | When a build starts | How many test shards a build runs |
| How it works | First step polls the API until there's room | First step is a `block` step; a separate controller pipeline unblocks it | First step reads free capacity and uploads a test step with `parallelism: N` sized to fit |
| Limit expressed as | Jobs on one queue (`MAX_RUNNING`) | Jobs on one queue (`MAX_RUNNING`) | vCPU across the whole org (`MAX_VCPU`) |
| Who decides | Each build's own gate, one at a time | One controller for all builds | Each build's planner, one at a time |
| Pipelines | 1 | 2 (gated pipeline + controller) | 1 |
| When capacity is busy | Build waits | Build stays blocked | Build runs with fewer shards (`MIN_SHARDS` at peak); waits only if not even `MIN_SHARDS` fits |
| Needs something always running | No | Yes, the controller | No |

All three held their limit in testing (see [Test results](#test-results)).

## How capacity is measured

### A and B: one queue

On every check, the gate (A) or controller (B) asks the Buildkite GraphQL API for the target queue's:

- **running jobs**: the higher of `ClusterQueue.metrics.runningJobsCount` and a live count of jobs in state `RUNNING`. The queue metric is reported per time bucket and is only populated if advanced queue metrics are available; if it's missing, the live count is used alone.
- **queued jobs**: jobs that have been released but aren't running yet (states `SCHEDULED`, `RESERVED`, `ASSIGNED`, `ACCEPTED`). Counting these stops a build that was just released from being missed by the next check.

A build is released only if `running + queued + JOBS_PER_BUILD <= MAX_RUNNING`, so the build's own jobs fit under the cap.

The query, from [`.buildkite/scripts/gate.sh`](.buildkite/scripts/gate.sh):

```graphql
query GateCheck($queue: ID!, $cluster: ID!, $org: ID!) {
  node(id: $queue) {
    ... on ClusterQueue { key metrics { runningJobsCount timestamp } }
  }
  organization(slug: $org) {
    running: jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [RUNNING], type: [COMMAND]) { count }
    queued: jobs(first: 1, cluster: $cluster, clusterQueue: [$queue], state: [SCHEDULED, RESERVED, ASSIGNED, ACCEPTED], type: [COMMAND]) { count }
  }
}
```

### C: every queue in the org, in vCPU

Approach C budgets compute across the whole organization, not one queue. A job on any queue in any cluster uses some of the same budget.

**Once per build** (cached for the rest of the run), the planner discovers what to count:

1. Lists every cluster (`GET /v2/organizations/{org}/clusters`), then each cluster's queues (`GET .../clusters/{id}/queues`).
2. Reads each **hosted** queue's vCPU from its instance shape. The planner prints one raw queue response in its log so you can see the field:

   ```json
   "hosted_agents": {
     "instance_shape": { "architecture": "amd64", "cpu": 2, "machine_type": "linux", "memory": 4, "name": "LINUX_AMD64_2X4" }
   }
   ```

   It uses `instance_shape.cpu`. If that's missing, it falls back to the number in the shape name (`LINUX_AMD64_16X64` → 16).
3. **Self-hosted** queues have no instance shape. Give their vCPU in `VCPU_OVERRIDES` (`"key:vcpu key:vcpu"`), or they're skipped and listed as skipped in the log.

**On every check**, it re-queries only the job counts: running and queued (`SCHEDULED`, `RESERVED`, `ASSIGNED`, `ACCEPTED`) for every counted queue. Each queue is two aliased `jobs { count }` fields, batched 20 queues per GraphQL request. It leaves out its own job, since it exits right after uploading. Then:

```
in-flight vCPU = sum over all counted queues of (running + queued) x queue vCPU
util           = in-flight vCPU / MAX_VCPU
FIT            = (MAX_VCPU - in-flight vCPU) / target queue vCPU

if FIT < MIN_SHARDS:     wait POLL_SECONDS and check again (never go over MAX_VCPU)
elif util >= PEAK_UTIL:  N = MIN_SHARDS
else:                    N = min(FIT / 2, MAX_SHARDS), at least MIN_SHARDS
```

Taking half of what fits, rather than all of it, leaves room for the builds right behind this one. At peak, every build drops to the minimum, so many builds can still start without any of them waiting.

**Worked example.** `MAX_VCPU=1584`, three hosted queues of 16, 8 and 4 vCPU, shards going to the 16 vCPU queue, `PEAK_UTIL=0.7`, `MIN_SHARDS=2`, `MAX_SHARDS=32`:

| Jobs in flight (16 / 8 / 4 vCPU queues) | In-flight vCPU | Util | FIT (16 vCPU shards) | N |
|---|---|---|---|---|
| 20 / 30 / 40 | 320 + 240 + 160 = 720 | 0.45 | 864 / 16 = 54 | min(27, 32) = **27** |
| 35 / 40 / 50 | 560 + 320 + 200 = 1080 | 0.68 | 504 / 16 = 31 | min(15, 32) = **15** |
| 40 / 50 / 61 | 640 + 400 + 244 = 1284 | 0.81 | 300 / 16 = 18 | at peak: **2** |
| 54 / 60 / 60 | 864 + 480 + 240 = 1584 | 1.00 | 0 | waits |

## Repository layout

```
.buildkite/
  pipeline.yml             # A. Self-gated: gate + 2 work jobs
  pipeline.block.yml       # B. Block step: block step + 2 work jobs
  pipeline.controller.yml  # B. Block step: capacity controller
  pipeline.adaptive.yml    # C. Adaptive parallelism: planner step
  scripts/
    gate.sh                # A: gate logic
    controller.sh          # B: controller logic
    adaptive.sh            # C: counts org-wide vCPU, picks N, uploads the parallel test step
```

The work in each example is fake. A and B run two `sleep 60` steps on the capped queue. C uploads a test step with `parallelism: N` whose shards each run `sleep $(( 20 + 120 / N ))`: a fixed 20s start-up cost plus an even split of 120s of work. Replace these with your real steps.

## Prerequisites

- A Buildkite organization using **clusters**, with:
  - the queue you want to cap (the *target queue*). The examples use a Buildkite hosted agents queue, `hosted_backpressure`; any cluster queue works, hosted or self-hosted.
  - a separate queue for the gate, controller and planner jobs. They're lightweight (bash, `curl`, `jq`) and must **not** run on the target queue, or they'd count toward and take up the capacity they're managing. The examples use the cluster's default hosted queue.
- Agents for those jobs need `bash`, `curl` and `jq`. If `jq` is missing, the scripts install it with `apt-get` (Debian/Ubuntu with `sudo`). Buildkite hosted Linux agents work as-is.
- A Buildkite **API access token** ([create one](https://buildkite.com/user/api-access-tokens)):
  - **Organization access**: your organization
  - **GraphQL API access**: enabled
  - **REST scopes**: `read_builds`, `read_clusters` (C lists every cluster and queue), plus `write_builds` for approach B (to unblock builds)
- That token stored as a [Buildkite secret](https://buildkite.com/docs/pipelines/security/secrets/buildkite-secrets) named `GRAPHQL_API_TOKEN` in the cluster, readable by the pipelines below.

## Setup

### 1. Find your cluster and queue IDs

The scripts take GraphQL IDs. You can read them from the REST API with the same token:

```bash
ORG=your-org-slug
TOKEN=...   # your API token

# Clusters: note the "id" (UUID) and "graphql_id" of yours
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://api.buildkite.com/v2/organizations/$ORG/clusters" | jq '.[] | {name, id, graphql_id}'

# Queues in that cluster: note the "graphql_id" of the target queue
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://api.buildkite.com/v2/organizations/$ORG/clusters/<cluster-uuid>/queues" | jq '.[] | {key, graphql_id}'
```

### 2. Point the pipeline files at your org

In `.buildkite/pipeline.yml` and `.buildkite/pipeline.controller.yml` (A and B), update the `env` block:

```yaml
env:
  BK_ORG_SLUG: "your-org-slug"
  BK_CLUSTER_ID: "<cluster graphql_id>"
  BK_QUEUE_ID: "<target queue graphql_id>"
```

Then replace `queue: hosted_backpressure` in the work steps (`pipeline.yml`, `pipeline.block.yml`) with your target queue's key.

In `.buildkite/pipeline.adaptive.yml` (C), set `BK_ORG_SLUG`, `BK_CLUSTER_ID` (the cluster the pipeline runs in) and `TARGET_QUEUE_KEY` (the queue in that cluster the shards run on). C finds every other queue itself, so it doesn't need queue IDs.

### 3. Create the pipelines for your approach

Create each pipeline from this repo, in your cluster. Each pipeline's steps (in the pipeline settings) are a single upload step pointing at its file.

**A. Self-gated:** one pipeline.

```yaml
steps:
  - label: ":pipeline:"
    command: buildkite-agent pipeline upload
```

Every build runs the gate first; the work jobs start once the gate releases.

**B. Block step:** two pipelines.

| Pipeline | Upload command |
|---|---|
| Gated pipeline | `buildkite-agent pipeline upload .buildkite/pipeline.block.yml` |
| Controller | `buildkite-agent pipeline upload .buildkite/pipeline.controller.yml` |

Set `TARGET_PIPELINES` in `pipeline.controller.yml` to the gated pipeline's slug (space-separate several slugs to manage more than one).

The controller runs for `RUN_MINUTES` per build, then exits. To keep it always on, add a [pipeline schedule](https://buildkite.com/docs/pipelines/configure/workflows/scheduled-builds) to the controller pipeline at the same interval (for example `*/30 * * * *` with the default 30 minutes). Its concurrency group allows only one controller at a time, so an overlapping run waits instead of double-counting capacity.

**C. Adaptive parallelism:** one pipeline.

```yaml
steps:
  - label: ":pipeline:"
    command: buildkite-agent pipeline upload .buildkite/pipeline.adaptive.yml
```

Set `MAX_VCPU` to the vCPU budget for your whole org, and `MAX_SHARDS` to the most shards a build should ever run. If you use self-hosted agents too, list their queues' vCPU in `VCPU_OVERRIDES` so they count toward the budget.

With [Test Engine test splitting](https://buildkite.com/docs/test-engine/test-splitting), the varying N needs no extra work: the Test Engine client (`bktec`) reads `BUILDKITE_PARALLEL_JOB` and `BUILDKITE_PARALLEL_JOB_COUNT`, which Buildkite sets on each parallel job, so it splits the tests across however many shards the planner chose. Replace the example's `sleep` command with your `bktec run` command.

## Configuration

Set these in the pipeline YAML `env` block. Values written as `${VAR:-default}` can also be overridden per build (New Build → Environment Variables).

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `MAX_RUNNING` | A, B | `10` | Max in-flight (running + queued) jobs on the target queue |
| `JOBS_PER_BUILD` | A, B | `2` | Jobs each build adds to the target queue. Keep in step with your work steps |
| `POLL_SECONDS` | A, B, C | `15` (A, C), `10` (B) | Seconds between checks (C only re-checks while waiting for `MIN_SHARDS` to fit) |
| `TARGET_PIPELINES` | B | example slug | Space-separated slugs of pipelines whose blocked builds the controller manages |
| `RUN_MINUTES` | B | `30` | How long one controller build runs |
| `MAX_VCPU` | C | `20` | vCPU budget across every counted queue in the org |
| `PEAK_UTIL` | C | `0.7` | At or above this utilization (`in-flight vCPU / MAX_VCPU`), builds get `MIN_SHARDS` |
| `MIN_SHARDS` | C | `1` | Fewest shards a build runs. If not even this fits, the planner waits |
| `MAX_SHARDS` | C | `5` in the example pipeline (`8` in the script) | Most shards a build runs. With `MAX_VCPU=20` and 2 vCPU shards, an idle org fits 10, and half of that is 5 |
| `TARGET_QUEUE_KEY` | C | `hosted_backpressure` | Queue the shards run on; its vCPU sizes each shard |
| `VCPU_OVERRIDES` | C | empty | vCPU of self-hosted queues to count, as `"key:vcpu key:vcpu"`. Unlisted self-hosted queues are skipped |
| `BK_ORG_SLUG`, `BK_CLUSTER_ID` | A, B, C | example values | Your org and cluster (see [Setup](#setup)) |
| `BK_QUEUE_ID` | A, B | example value | Target queue GraphQL ID; can be overridden per build to watch a different queue |

## Behavior details

**A. Self-gated**
- The gate step uses `concurrency_group` with `concurrency: 1`, so gates across all builds check one at a time. Without this, gates that check at the same moment all see the same free capacity and release together. In testing, that overshot a cap of 10 to 16.
- API errors are logged and retried; the gate never fails because of them. It also has no timeout, so add `timeout_in_minutes` to the gate step if you want waiting builds to give up eventually.

**B. Block step**
- Blocked builds are released oldest first (by build creation time) across all `TARGET_PIPELINES`.
- Within one check, the controller releases as many builds as fit, counting each one's `JOBS_PER_BUILD` as it goes.
- Unblocking a build by hand still works and lets it skip the queue. The controller sees its jobs on the next check.
- If the controller isn't running, builds stay blocked until it is (or until someone unblocks them).

**C. Adaptive parallelism**
- The planner step uses `concurrency_group` with `concurrency: 1`, so planners across all builds read headroom one at a time. Its shards are queued as soon as it uploads them, so the next planner already counts them.
- `MAX_VCPU` covers every counted queue, including other teams' pipelines that don't use the planner. Those jobs shrink the headroom, but nothing stops them from starting, so org-wide usage can go over `MAX_VCPU` because of them. The planner never adds shards that would take it over.
- `MIN_SHARDS` is a floor, but never at the expense of `MAX_VCPU`. If the free headroom can't fit `MIN_SHARDS`, the planner waits and re-checks, like the self-gated approach, rather than going over budget.
- The shard count is fixed once uploaded. A build that started small stays small even if capacity frees up while it runs.
- Each build's annotation shows a per-queue table (cluster, queue, vCPU, running, queued, vCPU used) for every busy queue plus the target, the org total, utilization, which rule applied, and N. N and the in-flight vCPU are also saved as build meta-data (`adaptive-shards`, `adaptive-inflight-vcpu`).
- Discovery costs one REST call for the cluster list plus one per cluster, at the start of each build; the planner then only re-queries job counts. With 34 hosted queues, each check is two GraphQL requests.
- Test Engine's client can also choose parallelism itself, from a target run time ([dynamic parallelism](https://buildkite.com/docs/test-engine/bktec/configuring#dynamic-parallelism), `bktec` 2.0+). That sizes a build by how long its tests take; approach C sizes it by how much capacity is free. You could use both and run the smaller of the two.

**All approaches**
- A, B: if `JOBS_PER_BUILD` is larger than `MAX_RUNNING`, the build is released once the target queue is empty.
- The limit applies only to builds that go through the gate, controller or planner. Jobs from other pipelines on the same queue count toward in-flight, but nothing stops them from starting. A dedicated target queue keeps the count clean.
- The queue can never run more jobs than its agents allow. If agent capacity (your hosted agents concurrency, or the size of a self-hosted fleet) is lower than `MAX_RUNNING` (or `MAX_VCPU` for C), the agents become the limit and the gate rarely has to wait.

## Test results

The target queue was a Buildkite hosted agents queue (Linux, 2 vCPU / 4 GB); the gate, controller and planner ran on a separate hosted queue. No jobs were held back by hosted agents concurrency limits.

### A and B

Tested with `MAX_RUNNING=10`, `JOBS_PER_BUILD=2` and work jobs of `sleep 60`.

| Approach | Scenario | Peak running | Peak running + queued | Result |
|---|---|---|---|---|
| A. Self-gated | 10 builds started at once | **10** | **10** | Gates released builds one by one at 0, 2, 4, 6, 8 in flight; the 6th waited until capacity freed. All 10 passed in about 3 minutes |
| B. Block step | 10 builds blocked, then controller started | **10** | **10** | Controller released 5 builds at once, held the other 5 until the first wave finished. All 10 passed in about 3 minutes |
| B. Block step | Controller running, 15 builds arriving every 4 seconds | **10** | **10** | Builds admitted as capacity freed, oldest first. All 15 passed in about 4 minutes |

### C. Adaptive parallelism

Tested with the example pipeline's settings: `MAX_VCPU=20`, `PEAK_UTIL=0.7`, `MIN_SHARDS=1`, `MAX_SHARDS=5`, shards on a 2 vCPU hosted queue. The planner counted 34 hosted queues across 13 clusters and skipped 33 self-hosted ones.

The org is shared, so other people's builds were running too. Throughout testing, another team's job held 4 vCPU on a queue in a different cluster. "Org in-flight" below is everything the planner counted, theirs included.

| Scenario | Org in-flight seen | Shards chosen | Result |
|---|---|---|---|
| 1 build, nothing else of ours running | 4 vCPU (the other team's job) | **4** | Util 0.20, FIT 8, so 8 / 2 = 4. The other cluster's job cost this build one shard |
| 1 build, budget raised to `MAX_VCPU=24` to cancel out that job | 4 of 24 vCPU | **5** (`MAX_SHARDS`) | Headroom 20 vCPU, the same as an idle org with `MAX_VCPU=20`. FIT 10, so 10 / 2 = 5 |
| 5 builds started at once | 4 → 16 vCPU | **4, 2, 1, 2, 1** | N fell as utilization rose. See below |
| Load on a different queue (`hosted_tester`, 4 × 2 vCPU), then 1 build | 12 vCPU | **2** | The same build got 4 shards without that load |

How the 5-build burst played out, in planner order:

| Build | Org in-flight seen | Util | FIT | Rule | Shards |
|---|---|---|---|---|---|
| 1st | 4 vCPU | 0.20 | 8 | FIT / 2 | **4** |
| 2nd | 12 vCPU | 0.60 | 4 | FIT / 2 | **2** |
| 3rd | 16 vCPU | 0.80 | 2 | at peak | **1** |
| 4th | 10 vCPU | 0.50 | 5 | FIT / 2 | **2** |
| 5th | 14 vCPU | 0.70 | 3 | at peak | **1** |

The 4th build saw less load than the 3rd because the 1st build's shards had just finished. All 5 builds passed in about 4 minutes, and none had to wait.

**Never over budget:** sampling every hosted job in the org every few seconds during the burst, in-flight vCPU peaked at exactly **20** (`MAX_VCPU`), and that includes the planner jobs themselves. In an earlier run, another team's burst of 32 jobs pushed the org to 74 vCPU. The planner doesn't control other pipelines (see [Behavior details](#behavior-details)). It had already chosen its shards against the load at that moment (6 vCPU in flight, 3 shards), so its own shards never took the total over budget.

### Timing gap

Between a job finishing and its replacement running there's a short gap: up to one poll interval, plus agent start-up time. Lower `POLL_SECONDS` to shorten it, at the cost of more API calls.

## Try it

- **A. Self-gated:** start a build, then while its work jobs are running, start another with `MAX_RUNNING=2`. The second gate logs `At capacity, waiting...` until the first build's jobs finish.
- **B. Block step:** start several builds of the gated pipeline (they'll sit blocked), then start a controller build. Its log shows each unblock decision and the in-flight count.
- **C. Adaptive parallelism:** start one build and open its annotation: it lists every busy queue in the org and the N chosen. Start a few more while its shards are running and watch N drop to `MIN_SHARDS` once utilization passes `PEAK_UTIL`. To see a build wait, start one with a low budget, for example `MAX_VCPU=4`, while another build's shards are running.
