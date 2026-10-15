#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
# Existing suites execute all current unit/integration cases and normal restart checks.
./scripts/verify-admission.sh
docker compose run --rm verifier crawlRestartProbe --args=write
docker compose restart dynamodb app
docker compose up -d --wait --wait-timeout 180
docker compose run --rm verifier crawlRestartProbe --args=verify
docker compose run --rm verifier crawlRestartProbe --args=delete
printf '%s\n' 'Admission, safe crawl fixtures, publication and restart recovery verification passed.'
