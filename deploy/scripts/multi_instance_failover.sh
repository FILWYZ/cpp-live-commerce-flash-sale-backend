#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/../.." && pwd)
compose_file="${project_dir}/deploy/compose/docker-compose.multi.yml"
base_url="${BASE_URL:-http://localhost:8080}"
second_url="${SECOND_URL:-http://localhost:8081}"

docker compose -f "${compose_file}" up -d --build
trap 'docker compose -f "${compose_file}" down' EXIT

for url in "${base_url}" "${second_url}"; do
  for attempt in $(seq 1 60); do
    if curl --fail --silent "${url}/ready" >/dev/null; then break; fi
    if [ "${attempt}" -eq 60 ]; then echo "gateway did not become healthy: ${url}" >&2; exit 1; fi
    sleep 2
  done
done

username="failover-user-$(date +%s)-$$"
curl --fail --silent -X POST "${base_url}/users/register" \
  -d "username=${username}&password=failover-password" >/dev/null
login=$(curl --fail --silent -X POST "${base_url}/users/login" \
  -d "username=${username}&password=failover-password")
token=$(printf '%s' "${login}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "${token}" ]

assert_session_on_gateway() {
  status=$(curl --silent --output /dev/null --write-out '%{http_code}' \
    -H "Authorization: Bearer ${token}" "${second_url}/orders?order_id=missing")
  if [ "${status}" != "404" ]; then
    echo "expected authenticated missing-order response 404, got ${status}" >&2
    exit 1
  fi
}

assert_session_on_gateway
docker compose -f "${compose_file}" stop gateway-1
curl --fail --silent "${second_url}/ready" >/dev/null
assert_session_on_gateway
echo "multi-instance Redis session and gateway failover passed"
