# Buildkite queue back-pressure

Keep a Buildkite cluster queue within a capacity limit, either by holding new builds back until the queue has room, or by sizing each build to the room that's free.

This is useful when a queue sits in front of something with limited capacity, such as a license pool, a shared test environment, a rate-limited downstream service, or a cost budget, and you want builds to wait in Buildkite rather than pile up downstream.

## Approaches

The repo has working examples of each approach, all built on the Buildkite APIs:

| | A. Self-gated | B. Block step | C. Adaptive parallelism |
|---|---|---|---|
| What it controls | When a build starts | When a build starts | How many test shards a build runs |
| How it works | First step polls the API until there's room | First step is a `block` step; a separate controller pipeline unblocks it | First step reads free capacity and uploads a test step with `parallelism: N` sized to fit |
| Limit expressed as | Jobs (`MAX_RUNNING`) | Jobs (`MAX_RUNNING`) | vCPU (`MAX_VCPU`) |
| Who decides | Each build's own gate, one at a time | One controller for all builds | Each build's planner, one at a time |
| Pipelines | 1 | 2 (gated pipeline + controller) | 1 |
| When the queue is busy | Build waits | Build stays blocked | Build runs with fewer shards; waits only if not even `MIN_SHARDS` fits |
| Needs something always running | No | Yes, the controller | No |

All three count jobs the same way, and all three held their limit exactly in testing (see [Test results](#test-results)).

## How capacity is measured

On every check, the gate (A), controller (B) or planner (C) asks the Buildkite GraphQL API for the target queue's:

- **running jobs**: the higher of `ClusterQueue.metrics.runningJobsCount` and a live count of jobs in state `RUNNING`. The queue metric is reported per time bucket and is only populated if advanced queue metrics are available; if it's missing, the live count is used alone.
- **queued jobs**: jobs that have been released but aren't running yet (states `SCHEDULED`, `RESERVED`, `ASSIGNED`, `ACCEPTED`). Counting these stops a build that was just released from being missed by the next check.

- **A and B** release a build only if `running + queued + JOBS_PER_BUILD <= MAX_RUNNING`, so the build's own jobs fit under the cap.
- **C** converts the count to vCPU and sizes the build to what's left:

  ```
  in-flight vCPU = (running + queued) x QUEUE_VCPU
  headroom       = MAX_VCPU - in-flight vCPU
  N              = headroom / QUEUE_VCPU, clamped to MIN_SHARDS..MAX_SHARDS
  ```

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
    adaptive.sh            # C: picks N and uploads the parallel test step
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
  - **REST scopes**: `read_builds`, `read_clusters`, plus `write_builds` for approach B (to unblock builds)
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

In `.buildkite/pipeline.yml`, `.buildkite/pipeline.controller.yml` and `.buildkite/pipeline.adaptive.yml`, update the `env` block:

```yaml
env:
  BK_ORG_SLUG: "your-org-slug"
  BK_CLUSTER_ID: "<cluster graphql_id>"
  BK_QUEUE_ID: "<target queue graphql_id>"
```

Then replace `queue: hosted_backpressure` in the work steps (`pipeline.yml`, `pipeline.block.yml`) and `TARGET_QUEUE_KEY` in `pipeline.adaptive.yml` with your target queue's key.

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

Set `QUEUE_VCPU` to the vCPU of one agent on the target queue, and `MAX_VCPU` to the budget you want the queue to stay within.

## Configuration

Set these in the pipeline YAML `env` block. Values written as `${VAR:-default}` can also be overridden per build (New Build → Environment Variables).

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `MAX_RUNNING` | A, B | `10` | Max in-flight (running + queued) jobs on the target queue |
| `JOBS_PER_BUILD` | A, B | `2` | Jobs each build adds to the target queue. Keep in step with your work steps |
| `POLL_SECONDS` | A, B, C | `15` (A, C), `10` (B) | Seconds between checks (C only re-checks while waiting for `MIN_SHARDS` to fit) |
| `TARGET_PIPELINES` | B | example slug | Space-separated slugs of pipelines whose blocked builds the controller manages |
| `RUN_MINUTES` | B | `30` | How long one controller build runs |
| `QUEUE_VCPU` | C | `4` | vCPU per job on the target queue. Match your agents' instance size |
| `MAX_VCPU` | C | `40` | vCPU budget for the target queue |
| `MIN_SHARDS` | C | `1` | Fewest shards a build runs. If not even this fits, the planner waits |
| `MAX_SHARDS` | C | `8` | Most shards a build runs, even on an idle queue |
| `TARGET_QUEUE_KEY` | C | `hosted_backpressure` | Queue key the test step is uploaded to |
| `BK_ORG_SLUG`, `BK_CLUSTER_ID` | A, B, C | example values | Your org and cluster (see [Setup](#setup)) |
| `BK_QUEUE_ID` | A, B, C | example value | Target queue GraphQL ID; can be overridden per build to watch a different queue |

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
- `MIN_SHARDS` is a floor, but never at the expense of `MAX_VCPU`. If the free headroom can't fit `MIN_SHARDS`, the planner waits and re-checks, like the self-gated approach, rather than going over budget.
- The shard count is fixed once uploaded. A build that started small stays small even if capacity frees up while it runs.
- The chosen N is shown in a build annotation and saved as build meta-data (`adaptive-shards`).

**All approaches**
- A, B: if `JOBS_PER_BUILD` is larger than `MAX_RUNNING`, the build is released once the target queue is empty.
- The limit applies only to builds that go through the gate, controller or planner. Jobs from other pipelines on the same queue count toward in-flight, but nothing stops them from starting. A dedicated target queue keeps the count clean.
- The queue can never run more jobs than its agents allow. If agent capacity (your hosted agents concurrency, or the size of a self-hosted fleet) is lower than `MAX_RUNNING` (or `MAX_VCPU / QUEUE_VCPU` for C), the agents become the limit and the gate rarely has to wait.

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

Tested with the defaults: `QUEUE_VCPU=4`, `MAX_VCPU=40` (room for 10 shards), `MIN_SHARDS=1`, `MAX_SHARDS=8`. The test queue's agents actually have 2 vCPU; `QUEUE_VCPU=4` was kept so the budget works out to 10 jobs, the same cap as A and B. vCPU figures below are in those budget units.

| Scenario | Shards chosen | Peak in-flight vCPU | Result |
|---|---|---|---|
| 1 build on an idle queue | **8** | 32 | Headroom was 40 vCPU (10 shards); clamped to `MAX_SHARDS`. 8 shards of 35s, build took about 1 minute |
| 5 builds started at once | **8, 2, 8, 8, 2** | **40** | See below. All 5 passed in about 4 minutes |

How the 5-build burst played out, in planner order:

| Build | Planner saw in-flight | Headroom | Shards | Notes |
|---|---|---|---|---|
| 1st | 0 vCPU | 40 | 8 | Clamped to `MAX_SHARDS` |
| 2nd | 32 vCPU | 8 | **2** | Fewer shards: 80s each instead of 35s |
| 3rd | 40 vCPU | 0 | 8 | Queue full; waited 30s until the 1st build's shards finished (headroom 32) |
| 4th | 40 vCPU | 0 | 8 | Waited 45s until the queue drained |
| 5th | 32 vCPU | 8 | **2** | Fewer shards, like the 2nd |

In-flight vCPU never went over `MAX_VCPU`: it peaked at exactly 40.

Later builds get fewer shards whenever they arrive while there's *some* room left. When they arrive to a full queue, they wait for `MIN_SHARDS` to fit; by the time a big build's shards finish, a large block of capacity frees at once, so the next build gets a large N again. Short shards (as here, 35s) make this more pronounced. The trade-off shows in build times: the 2-shard builds' shards ran 80 seconds each instead of 35, but those builds started immediately instead of waiting.

### Timing gap

Between a job finishing and its replacement running there's a short gap: up to one poll interval, plus agent start-up time. Lower `POLL_SECONDS` to shorten it, at the cost of more API calls.

## Try it

- **A. Self-gated:** start a build, then while its work jobs are running, start another with `MAX_RUNNING=2`. The second gate logs `At capacity, waiting...` until the first build's jobs finish.
- **B. Block step:** start several builds of the gated pipeline (they'll sit blocked), then start a controller build. Its log shows each unblock decision and the in-flight count.
- **C. Adaptive parallelism:** start one build on an idle queue (it gets `MAX_SHARDS`), then start another while the first build's shards are running. The second build's annotation shows the smaller headroom and N it chose. To see it wait, set `MAX_VCPU` low, for example `MAX_VCPU=8`.
