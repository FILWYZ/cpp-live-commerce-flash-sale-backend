#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compose_file="${project_dir}/deploy/compose/docker-compose.yml"
project_name="${ECOMMERCE_DEMO_PROJECT:-live-commerce-internship-demo}"
export ECOMMERCE_GATEWAY_PUBLISHED_PORT="${ECOMMERCE_GATEWAY_PUBLISHED_PORT:-18080}"
export ECOMMERCE_REDIS_PUBLISHED_PORT="${ECOMMERCE_REDIS_PUBLISHED_PORT:-16379}"
export ECOMMERCE_MYSQL_PUBLISHED_PORT="${ECOMMERCE_MYSQL_PUBLISHED_PORT:-13316}"
export ECOMMERCE_REDPANDA_PUBLISHED_PORT="${ECOMMERCE_REDPANDA_PUBLISHED_PORT:-19093}"
export DEMO_BASE_URL="${DEMO_BASE_URL:-http://127.0.0.1:${ECOMMERCE_GATEWAY_PUBLISHED_PORT}}"

compose() {
  docker compose -p "${project_name}" -f "${compose_file}" "$@"
}

case "${1:-demo}" in
  demo)
    compose up --build -d --wait --wait-timeout 180
    python3 "${project_dir}/deploy/scripts/demo_flow.py"
    ;;
  start)
    compose up --build -d --wait --wait-timeout 180
    printf 'Demo stack is ready at %s (project=%s)\n' "${DEMO_BASE_URL}" "${project_name}"
    ;;
  stop)
    compose stop
    ;;
  down)
    compose down
    ;;
  *)
    printf 'usage: %s [demo|start|stop|down]\n' "$0" >&2
    exit 2
    ;;
esac
