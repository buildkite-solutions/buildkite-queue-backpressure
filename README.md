# kube_local back-pressure gate demo

A Buildkite pipeline that keeps the number of running jobs on the `kube_local` queue (Golden Gate Cluster) at a configurable limit.

## How it works

1. **Gate step** (runs on the cluster default queue, `hosted_linux_small`) polls the Buildkite GraphQL API every `POLL_SECONDS`:
   - `ClusterQueue.metrics.runningJobsCount` (requires advanced queue metrics)
   - live count of `RUNNING` command jobs on the queue (`organization.jobs(clusterQueue: ...)`)

   It takes the higher of the two. While it is `>= MAX_RUNNING`, it waits. Once it drops below, the gate finishes. API errors are logged and retried, never failed.
2. **Work jobs**: two `sleep 60` jobs on `kube_local`, which `depends_on` the gate.

## Setup

- Buildkite secret `GRAPHQL_API_TOKEN` in Golden Gate Cluster: API token with GraphQL access to `buildkite-solutions`, scopes `read_builds` and `read_clusters`.
- Pipeline steps: `buildkite-agent pipeline upload` (reads `.buildkite/pipeline.yml`).

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `MAX_RUNNING` | `10` | Release the gate when running jobs are below this |
| `POLL_SECONDS` | `15` | Seconds between checks |

Set `MAX_RUNNING=1` on a new build while another build is running to watch the gate wait.

## Limitations

- The gate checks before adding its own jobs, so the queue can briefly reach `MAX_RUNNING - 1 + 2`.
- Gates that check at the same moment can all release together. Use a `concurrency` group if you need a hard cap.
