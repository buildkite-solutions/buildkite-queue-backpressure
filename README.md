# Buildkite queue back-pressure

Cap how many jobs run at once on a Buildkite cluster queue by holding new builds back until the queue has room.

This is useful when a queue sits in front of something with limited capacity, such as a Kubernetes cluster, a license pool, a shared test environment, or a rate-limited downstream service, and you want builds to wait in Buildkite rather than pile up downstream.

The repo has two working approaches, both built on the Buildkite APIs:

| | A. Polling gate | B. Block step + capacity controller |
|---|---|---|
| How a build waits | Its first step polls the API until there's room | Its first step is a `block` step; a separate controller unblocks it |
| Who decides | Each build's gate, one at a time | One controller for all builds |
| Pipelines | 1 | 2 (gated pipeline + controller) |
| Visible in the UI as | A running gate step | A blocked build (can be unblocked by hand to skip the queue) |
| Release speed under a burst | One build every few seconds (each gate job must start) | Several builds per poll |
| Needs something always running | No | Yes, the controller |

Both approaches count jobs the same way and both hold the cap exactly in testing (see [Test results](#test-results)).

## How capacity is measured

On every check, the gate or controller asks the Buildkite GraphQL API for the target queue's:

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

## Repository layout

```
.buildkite/
  pipeline.yml             # A: polling gate + 2 work jobs
  pipeline.block.yml       # B: block step + 2 work jobs
  pipeline.controller.yml  # B: capacity controller
  scripts/
    gate.sh                # A: gate logic
    controller.sh          # B: controller logic
```

The work jobs in both demos are two `sleep 60` steps on the capped queue. Replace them with your real steps.

## Prerequisites

- A Buildkite organization using **clusters**, with:
  - the queue you want to cap (called the *target queue* below), and
  - a queue for the gate/controller jobs. They're lightweight (bash, `curl`, `jq`) and should **not** run on the target queue, or they'd take up the capacity they're managing. In the demo they use the cluster's default queue.
- Agents for the gate/controller need `bash`, `curl` and `jq`. If `jq` is missing, the scripts install it with `apt-get` (Debian/Ubuntu with `sudo`).
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

In `.buildkite/pipeline.yml` and `.buildkite/pipeline.controller.yml`, update the `env` block:

```yaml
env:
  BK_ORG_SLUG: "your-org-slug"
  BK_CLUSTER_ID: "<cluster graphql_id>"
  BK_QUEUE_ID: "<target queue graphql_id>"
```

Then replace `queue: kube_local` in the work steps (`pipeline.yml`, `pipeline.block.yml`) with your target queue's key.

### 3a. Approach A: polling gate

Create one pipeline from this repo, in your cluster, with these steps:

```yaml
steps:
  - label: ":pipeline:"
    command: buildkite-agent pipeline upload
```

Every build runs the gate first; the work jobs start once the gate releases.

### 3b. Approach B: block step + capacity controller

Create two pipelines from this repo, in your cluster:

| Pipeline | Steps |
|---|---|
| Gated pipeline | `buildkite-agent pipeline upload .buildkite/pipeline.block.yml` |
| Controller | `buildkite-agent pipeline upload .buildkite/pipeline.controller.yml` |

Set `TARGET_PIPELINES` in `pipeline.controller.yml` to the gated pipeline's slug (space-separate several slugs to manage more than one).

The controller runs for `RUN_MINUTES` per build, then exits. To keep it always on, add a [pipeline schedule](https://buildkite.com/docs/pipelines/configure/workflows/scheduled-builds) to the controller pipeline at the same interval (for example `*/30 * * * *` with the default 30 minutes). Its concurrency group allows only one controller at a time, so an overlapping run waits instead of double-counting capacity.

## Configuration

Set these in the pipeline YAML `env` block. Values written as `${VAR:-default}` can also be overridden per build (New Build → Environment Variables).

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `MAX_RUNNING` | A, B | `10` | Max in-flight (running + queued) jobs on the target queue |
| `JOBS_PER_BUILD` | A, B | `2` | Jobs each build adds to the target queue. Keep in step with your work steps |
| `POLL_SECONDS` | A, B | `15` (A), `10` (B) | Seconds between checks |
| `TARGET_PIPELINES` | B | demo slug | Space-separated slugs of pipelines whose blocked builds the controller manages |
| `RUN_MINUTES` | B | `30` | How long one controller build runs |
| `BK_ORG_SLUG`, `BK_CLUSTER_ID`, `BK_QUEUE_ID` | A, B | demo values | Your org and target queue (see [Setup](#setup)) |

## Behavior details

**Approach A: polling gate**
- The gate step uses `concurrency_group` with `concurrency: 1`, so gates across all builds check one at a time. Without this, gates that check at the same moment all see the same free capacity and release together. In testing, that overshot a cap of 10 to 16.
- API errors are logged and retried; the gate never fails because of them. It also has no timeout, so add `timeout_in_minutes` to the gate step if you want waiting builds to give up eventually.

**Approach B: block step + capacity controller**
- Blocked builds are released oldest first (by build creation time) across all `TARGET_PIPELINES`.
- Within one check, the controller releases as many builds as fit, counting each one's `JOBS_PER_BUILD` as it goes.
- Unblocking a build by hand still works and lets it skip the queue. The controller sees its jobs on the next check.
- If the controller isn't running, builds stay blocked until it is (or until someone unblocks them).

**Both**
- If `JOBS_PER_BUILD` is larger than `MAX_RUNNING`, the build is released once the target queue is empty.
- The cap holds back only builds that go through the gate or controller. Jobs from other pipelines on the same queue count toward in-flight, but nothing stops them from starting.
- The queue can never run more jobs than its agents allow. If agent capacity (for example the Agent Stack for Kubernetes `max-in-flight` setting) is lower than `MAX_RUNNING`, the agents become the limit and the gate rarely has to wait.

## Test results

All tests used a cap of `MAX_RUNNING=10`, `JOBS_PER_BUILD=2`, and work jobs of `sleep 60`, on a Kubernetes-backed queue able to run 20 jobs at once.

| Scenario | Peak running | Peak running + queued | Result |
|---|---|---|---|
| A, 10 builds at once, gates **not** serialized (earlier version) | 16 | 16 | Cap exceeded: gates checked at the same moment |
| A, 10 builds at once, current version | **10** | **10** | Gates released builds one by one at 0, 2, 4, 6, 8 in flight; the 6th waited until capacity freed |
| B, 10 builds blocked, then controller started | **10** | **10** | Controller released 5 builds in 2 seconds, held the other 5, released them when the first wave finished |
| B, controller running, 15 builds arriving every 4 seconds | **10** | **10** | Builds admitted as capacity freed, oldest first; all 15 passed in about 4 minutes |

Between a job finishing and its replacement running there's a short gap: up to one poll interval, plus agent start-up time. Lower `POLL_SECONDS` to shorten it, at the cost of more API calls.

## Try it

- **Watch a build wait (A):** start a build, then while its work jobs are running, start another with `MAX_RUNNING=2`. The second gate logs `At capacity, waiting...` until the first build's jobs finish.
- **Watch the controller (B):** start several builds of the gated pipeline (they'll sit blocked), then start a controller build. Its log shows each unblock decision and the in-flight count.
