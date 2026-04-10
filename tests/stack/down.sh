#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${OVERSYNC_TEST_STACK_COMPOSE_FILE:-tests/stack/docker-compose.yml}"
STACK_ENV_FILE="${OVERSYNC_TEST_STACK_ENV_FILE:-${TMPDIR:-/tmp}/oversync-test-stack-${GITHUB_RUN_ID:-local}-${GITHUB_JOB:-dev}.env}"

docker compose --env-file "$STACK_ENV_FILE" -f "$COMPOSE_FILE" down -v --remove-orphans
