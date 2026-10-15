#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

benchmark_output="${1:-$(mktemp -d)}"
mkdir -p "$benchmark_output"
benchmark_output="$(CDPATH= cd -- "$benchmark_output" && pwd)"
docker compose up -d dynamodb
for benchmark_workers in 4 8 12; do
  docker compose run --build --no-deps --rm \
    --volume "$benchmark_output:/evidence" verifier crawlLoadProbe "--args=$benchmark_workers /evidence/$benchmark_workers"
done

for benchmark_artifact in "$benchmark_output/4/artifacts/"*.txt; do
  for benchmark_workers in 8 12; do
    cmp "$benchmark_artifact" "$benchmark_output/$benchmark_workers/artifacts/$(basename "$benchmark_artifact")"
  done
done
printf '%s\n' "All three 100-job workloads passed with identical artifacts. Evidence: $benchmark_output"
