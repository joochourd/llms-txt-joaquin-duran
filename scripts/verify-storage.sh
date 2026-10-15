#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

docker compose up --build -d --wait --wait-timeout 180
# The build image contains Gradle outputs; force execution instead of reusing test results.
docker compose run --build --rm verifier test integrationTest --rerun-tasks
docker compose run --rm verifier restartProbe --args=write

# A normal restart preserves the named volume. No down -v or table deletion.
docker compose restart dynamodb app
docker compose up -d --wait --wait-timeout 180
docker compose run --rm verifier restartProbe --args=verify
docker compose run --rm verifier restartProbe --args=delete

printf '%s\n' 'Builds, backend tests, health checks and storage restart verification passed.'
