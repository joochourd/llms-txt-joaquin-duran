#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
./scripts/verify-crawling.sh
docker compose run --rm verifier monitoringRestartProbe --args=write
docker compose restart dynamodb app
docker compose up -d --wait --wait-timeout 180
docker compose run --rm verifier monitoringRestartProbe --args=verify
docker compose run --rm verifier monitoringRestartProbe --args=delete

verification_evidence_dir="${VERIFY_EVIDENCE_DIR:-$(mktemp -d)}"
mkdir -p "$verification_evidence_dir"
docker compose run --no-deps --rm \
  --volume "$verification_evidence_dir:/evidence" verifier monitoringUpdateProbe "--args=/evidence"
printf '%s\n' "All verification passed, including monitoring restart and automatic updates. Evidence: $verification_evidence_dir"
