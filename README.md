# kube_local back-pressure gate demo

A Buildkite pipeline that keeps the number of running jobs on the `kube_local` queue (Golden Gate Cluster) at a configurable limit.

## How it works

1. **Gate step** (runs on the cluster default queue, `hosted_linux_small`) polls the Buildkite GraphQL API every `POLL_SECONDS` for jobs on `kube_local`:
   - **running**: the higher of `ClusterQueue.metrics.runningJobsCount` and a live count of `RUNNING` jobs
   - **queued**: jobs already released but not yet running (`SCHEDULED`, `RESERVED`, `ASSIGNED`, `ACCEPTED`)

   It releases when `running + queued + JOBS_PER_BUILD <= MAX_RUNNING`, otherwise it waits. API errors are logged and retried, never failed.
2. **One gate at a time**: the gate step is in concurrency group `kube-local-back-pressure-gate` with `concurrency: 1`, so the next gate only checks once the previous build's jobs are counted as queued. This makes `MAX_RUNNING` a hard cap even when many builds start at once.
3. **Work jobs**: two `sleep 60` jobs on `kube_local`, which `depends_on` the gate.

## Setup

- Buildkite secret `GRAPHQL_API_TOKEN` in Golden Gate Cluster: API token with GraphQL access to `buildkite-solutions`, scopes `read_builds` and `read_clusters`.
- Pipeline steps: `buildkite-agent pipeline upload` (reads `.buildkite/pipeline.yml`).

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `MAX_RUNNING` | `10` | Max in-flight (running + queued) jobs on `kube_local` |
| `JOBS_PER_BUILD` | `2` | Jobs each build adds to `kube_local` |
| `POLL_SECONDS` | `15` | Seconds between checks |

Set `MAX_RUNNING=1` on a new build while another build is running to watch the gate wait.

## Notes

- `kube_local` can only run as many jobs as the agent-stack-k8s `max-in-flight` allows (set to 20 in `buildkite-k8s-gitops`, `overlays/kube-local`). Keep it above `MAX_RUNNING` or the gate never needs to wait.
- If `JOBS_PER_BUILD` is larger than `MAX_RUNNING`, the gate waits for an empty queue, then lets the build through.
- The cap only covers builds of this pipeline (or anything else using the same gate). Other pipelines targeting `kube_local` still count toward in-flight but aren't held back.

## Variant: block step + capacity controller

Two more pipelines from this repo take a different approach. Builds don't poll; one controller decides for all of them.

| Pipeline | Steps file | What it does |
|---|---|---|
| `kube-local-block-gated-demo` | `.buildkite/pipeline.block.yml` | Starts with a `block` step, then two `sleep 60` jobs on `kube_local` |
| `kube-local-capacity-controller` | `.buildkite/pipeline.controller.yml` | Every `POLL_SECONDS`, counts in-flight jobs on `kube_local` and unblocks blocked builds (oldest first) while `in-flight + JOBS_PER_BUILD <= MAX_RUNNING` |

- The controller runs for `RUN_MINUTES` (default 30) and is in concurrency group `kube-local-capacity-controller` (1 at a time). Run it on a schedule to keep it always on.
- It manages the pipelines listed in `TARGET_PIPELINES`.
- Its token (`GRAPHQL_API_TOKEN`) also needs `write_builds` to unblock.
- You can still unblock a build by hand to let it skip the queue.
