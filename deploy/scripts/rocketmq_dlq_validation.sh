#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
base_compose="${project_dir}/deploy/compose/docker-compose.yml"
rocketmq_compose="${project_dir}/deploy/compose/docker-compose.rocketmq.yml"
project_name="${ECOMMERCE_DLQ_PROJECT:-internship-rocketmq-dlq-$(date +%Y%m%d%H%M%S)}"
gateway_port="${ECOMMERCE_GATEWAY_PUBLISHED_PORT:-18094}"
consumer_group="${ECOMMERCE_ROCKETMQ_CONSUMER_GROUP:-flashsale-command-consumers}"
dlq_topic="${ECOMMERCE_ROCKETMQ_DLQ_TOPIC:-flashsale.dlq}"
report="${DLQ_REPORT:-${project_dir}/benchmarks/results/rocketmq-app-dlq-${project_name}.json}"

export ECOMMERCE_GATEWAY_PUBLISHED_PORT="${gateway_port}"
export ECOMMERCE_REDIS_PUBLISHED_PORT="${ECOMMERCE_REDIS_PUBLISHED_PORT:-16394}"
export ECOMMERCE_MYSQL_PUBLISHED_PORT="${ECOMMERCE_MYSQL_PUBLISHED_PORT:-13330}"
export ECOMMERCE_REDPANDA_PUBLISHED_PORT="${ECOMMERCE_REDPANDA_PUBLISHED_PORT:-19107}"
export ECOMMERCE_DEMO_STOCK=10
export ECOMMERCE_DEMO_CAMPAIGN_LIMIT=10
export ECOMMERCE_FLASH_SALE_QUEUE=rocketmq
export ECOMMERCE_ROCKETMQ_CONSUMER_GROUP="${consumer_group}"
export ECOMMERCE_ROCKETMQ_DLQ_TOPIC="${dlq_topic}"
export ECOMMERCE_ROCKETMQ_INVISIBLE_MS="${ECOMMERCE_ROCKETMQ_INVISIBLE_MS:-120000}"
export ECOMMERCE_BENCH_PROJECT="${project_name}"

compose() {
  docker compose -p "${project_name}" -f "${base_compose}" -f "${rocketmq_compose}" --profile rocketmq "$@"
}

fail() {
  # Restore the dependency before preserving the isolated stack for inspection.
  compose start mysql >/dev/null 2>&1 || true
  echo "RocketMQ application DLQ validation failed; isolated stack ${project_name} is left running for inspection." >&2
  exit 1
}

mkdir -p "$(dirname -- "${report}")"
compose up --build -d --wait --wait-timeout 900 || fail
gateway="${project_name}-gateway-1"

# Keep the command in RocketMQ while the worker cannot persist it. The worker
# exhausts its five local attempts, writes the original command to the normal
# application DLQ topic, then ACKs the original delivery.
compose stop mysql || fail
producer_output=$(docker exec -e ECOMMERCE_ROCKETMQ_TEST_ENDPOINT=rocketmq-proxy:8081 \
  -e ECOMMERCE_ROCKETMQ_TEST_TOPIC=flashsale.command \
  -e ECOMMERCE_ROCKETMQ_TEST_GROUP="${consumer_group}" \
  -e ECOMMERCE_ROCKETMQ_TEST_SKU=sku-1 \
  -e ECOMMERCE_ROCKETMQ_PUBLISH_ONLY=1 "${gateway}" /app/rocketmq_smoke_test) || fail
printf '%s\n' "${producer_output}"

dead_lettered=0
stats='{}'
attempt=0
while [ "${attempt}" -lt 120 ]; do
  attempt=$((attempt + 1))
  stats=$(curl -sS --max-time 5 "http://127.0.0.1:${gateway_port}/debug/stats" || true)
  dead_lettered=$(printf '%s' "${stats}" | python3 -c \
    'import json,sys; print(json.load(sys.stdin).get("flash_sale_consumer_dead_lettered",0))' 2>/dev/null || printf '0')
  [ "${dead_lettered}" -ge 1 ] && break
  sleep 1
done
[ "${dead_lettered}" -ge 1 ] || fail

compose start mysql || fail
compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -proot-password --wait=60 >/dev/null || fail
ready=0
attempt=0
while [ "${attempt}" -lt 60 ]; do
  attempt=$((attempt + 1))
  ready_status=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 \
    "http://127.0.0.1:${gateway_port}/ready" || true)
  if [ "${ready_status}" = 200 ]; then
    ready=1
    break
  fi
  sleep 1
done
[ "${ready}" -eq 1 ] || fail

dlq_before=$(curl -sS --max-time 40 -H 'x-admin-token: dev-admin-token' \
  "http://127.0.0.1:${gateway_port}/admin/dlq?limit=10" || true)
if ! printf '%s' "${dlq_before}" | python3 -c \
  'import json,sys; d=json.load(sys.stdin); sys.exit(0 if len(d.get("messages", [])) >= 1 else 1)' 2>/dev/null; then
  printf '%s\n' "${dlq_before}" >"${report}"
  fail
fi

replay_response=$(curl -sS --max-time 40 -X POST -H 'x-admin-token: dev-admin-token' \
  -H 'Content-Type: application/x-www-form-urlencoded' -d 'max_messages=10' \
  "http://127.0.0.1:${gateway_port}/admin/dlq/replay" || true)
if ! printf '%s' "${replay_response}" | python3 -c \
  'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("replayed",0) >= 1 else 1)' 2>/dev/null; then
  printf '{"project":"%s","dead_lettered":%s,"dlq_before_replay":%s,"replay":%s}\n' \
    "${project_name}" "${dead_lettered}" "${dlq_before}" "${replay_response:-null}" >"${report}"
  fail
fi

processed=0
attempt=0
while [ "${attempt}" -lt 60 ]; do
  attempt=$((attempt + 1))
  stats=$(curl -sS --max-time 5 "http://127.0.0.1:${gateway_port}/debug/stats" || true)
  processed=$(printf '%s' "${stats}" | python3 -c \
    'import json,sys; print(json.load(sys.stdin).get("flash_sale_consumer_processed",0))' 2>/dev/null || printf '0')
  [ "${processed}" -ge 1 ] && break
  sleep 1
done
dlq_after=$(curl -sS --max-time 40 -H 'x-admin-token: dev-admin-token' \
  "http://127.0.0.1:${gateway_port}/admin/dlq?limit=10" || true)
printf '{"project":"%s","consumer_group":"%s","dlq_topic":"%s","application_retries":5,"mysql_recovery_without_gateway_restart":true,"dead_lettered":%s,"dlq_before_replay":%s,"replay":%s,"processed_after_replay":%s,"dlq_after_replay":%s}\n' \
  "${project_name}" "${consumer_group}" "${dlq_topic}" "${dead_lettered}" \
  "${dlq_before}" "${replay_response}" "${processed}" "${dlq_after}" >"${report}"

if [ "${processed}" -lt 1 ] || ! printf '%s' "${dlq_after}" | python3 -c \
  'import json,sys; d=json.load(sys.stdin); sys.exit(0 if len(d.get("messages", [])) == 0 else 1)' 2>/dev/null; then
  fail
fi

compose down
cat "${report}"
echo "RocketMQ application DLQ validation report saved to ${report}"
