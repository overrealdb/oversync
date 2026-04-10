#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${OVERSYNC_TEST_STACK_COMPOSE_FILE:-tests/stack/docker-compose.yml}"
STACK_ENV_FILE="${OVERSYNC_TEST_STACK_ENV_FILE:-${TMPDIR:-/tmp}/oversync-test-stack-${GITHUB_RUN_ID:-local}-${GITHUB_JOB:-dev}.env}"

wait_service() {
  local service="$1"
  local readiness="${2:-running}"
  local attempts="${3:-60}"

  for _ in $(seq 1 "$attempts"); do
    local row
    row="$(docker compose --env-file "$STACK_ENV_FILE" -f "$COMPOSE_FILE" ps --format json 2>/dev/null | grep "\"Service\":\"$service\"" || true)"
    if [[ -n "$row" ]]; then
      local state health status
      state="$(printf '%s\n' "$row" | sed -n 's/.*"State":"\([^"]*\)".*/\1/p')"
      health="$(printf '%s\n' "$row" | sed -n 's/.*"Health":"\([^"]*\)".*/\1/p')"
      status="$(printf '%s\n' "$row" | sed -n 's/.*"Status":"\([^"]*\)".*/\1/p')"

      if [[ "$state" == "running" ]]; then
        if [[ "$readiness" == "running" ]]; then
          echo "$service is running"
          return 0
        fi

        if [[ "$readiness" == "healthy" && "$health" == "healthy" ]]; then
          echo "$service is healthy"
          return 0
        fi
      fi

      echo "waiting for $service ($state/$health): $status"
    else
      echo "waiting for $service: container not listed yet"
    fi

    sleep 2
  done

  echo "Timed out waiting for $service" >&2
  docker compose --env-file "$STACK_ENV_FILE" -f "$COMPOSE_FILE" ps >&2 || true
  exit 1
}

wait_service postgres
wait_service mysql
wait_service surrealdb
wait_service redpanda
wait_service trino healthy
