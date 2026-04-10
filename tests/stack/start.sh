#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${OVERSYNC_TEST_STACK_COMPOSE_FILE:-tests/stack/docker-compose.yml}"
STACK_ENV_FILE="${OVERSYNC_TEST_STACK_ENV_FILE:-${TMPDIR:-/tmp}/oversync-test-stack-${GITHUB_RUN_ID:-local}-${GITHUB_JOB:-dev}.env}"

allocate_free_ports() {
  local count="$1"
  python3 - "$count" <<'PY'
import socket
import sys

count = int(sys.argv[1])
sockets = []
ports = []

try:
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        sockets.append(s)
        ports.append(str(s.getsockname()[1]))
    print(" ".join(ports))
finally:
    for s in sockets:
        s.close()
PY
}

persist_env() {
  local key="$1"
  local value="$2"
  export "$key=$value"
  printf '%s=%s\n' "$key" "$value" >> "$STACK_ENV_FILE"
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf '%s=%s\n' "$key" "$value" >> "$GITHUB_ENV"
  fi
}

: > "$STACK_ENV_FILE"
persist_env "OVERSYNC_TEST_STACK_ENV_FILE" "$STACK_ENV_FILE"

if [[ -n "${GITHUB_ENV:-}" ]]; then
  project_suffix="${GITHUB_RUN_ID:-local}-${GITHUB_JOB:-dev}-${GITHUB_RUN_ATTEMPT:-0}"
  project_suffix="${project_suffix//[^a-zA-Z0-9_-]/-}"
  persist_env "COMPOSE_PROJECT_NAME" "oversync-${project_suffix}"

  read -r postgres_port mysql_port surreal_port kafka_port kafka_proxy_port trino_port <<<"$(allocate_free_ports 6)"

  persist_env "OVERSYNC_TEST_POSTGRES_HOST_PORT" "$postgres_port"
  persist_env "OVERSYNC_TEST_MYSQL_HOST_PORT" "$mysql_port"
  persist_env "OVERSYNC_TEST_SURREAL_HOST_PORT" "$surreal_port"
  persist_env "OVERSYNC_TEST_KAFKA_HOST_PORT" "$kafka_port"
  persist_env "OVERSYNC_TEST_KAFKA_PROXY_HOST_PORT" "$kafka_proxy_port"
  persist_env "OVERSYNC_TEST_TRINO_HOST_PORT" "$trino_port"

  persist_env "OVERSYNC_TEST_POSTGRES_DSN" "postgres://postgres:postgres@127.0.0.1:${postgres_port}/postgres"
  persist_env "OVERSYNC_TEST_MYSQL_DSN" "mysql://root:root@127.0.0.1:${mysql_port}/test"
  persist_env "OVERSYNC_TEST_SURREAL_URL" "http://127.0.0.1:${surreal_port}"
  persist_env "OVERSYNC_TEST_KAFKA_BROKER" "127.0.0.1:${kafka_port}"
  persist_env "OVERSYNC_TEST_TRINO_URL" "http://127.0.0.1:${trino_port}"
else
  persist_env "COMPOSE_PROJECT_NAME" "${COMPOSE_PROJECT_NAME:-stack}"
fi

docker compose --env-file "$STACK_ENV_FILE" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
docker compose --env-file "$STACK_ENV_FILE" -f "$COMPOSE_FILE" up -d
