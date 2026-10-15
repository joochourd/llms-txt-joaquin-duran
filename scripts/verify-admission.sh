#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# Storage verification executes the backend suites and checks normal restart persistence.
./scripts/verify-storage.sh
docker compose run --rm verifier admissionRestartProbe --args=write
docker compose run --rm verifier bulkRestartProbe --args=write
docker compose restart dynamodb app
docker compose up -d --wait --wait-timeout 180
docker compose run --rm verifier admissionRestartProbe --args=verify
docker compose run --rm verifier admissionRestartProbe --args=delete
docker compose run --rm verifier bulkRestartProbe --args=verify
docker compose run --rm verifier bulkRestartProbe --args=delete

printf '%s\n' 'Storage, admission and bulk backlog restart verification passed.'
