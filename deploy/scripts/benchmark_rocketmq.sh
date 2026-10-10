#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
base_compose="${project_dir}/deploy/compose/docker-compose.yml"
rocketmq_compose="${project_dir}/deploy/compose/docker-compose.rocketmq.yml"
project_name="${ECOMMERCE_BENCH_PROJECT:-internship-rocketmq-bench-$(date +%Y%m%d%H%M%S)}"
gateway_port="${ECOMMERCE_GATEWAY_PUBLISHED_PORT:-18081}"
requests="${BENCH_REQUESTS:-200}"
concurrency="${BENCH_CONCURRENCY:-100}"
rate="${BENCH_RATE:-0}"
stock="${ECOMMERCE_DEMO_STOCK:-100}"
report="${BENCH_REPORT:-${project_dir}/benchmarks/results/rocketmq-single-node.json}"

export ECOMMERCE_GATEWAY_PUBLISHED_PORT="${gateway_port}"
export ECOMMERCE_REDIS_PUBLISHED_PORT="${ECOMMERCE_REDIS_PUBLISHED_PORT:-16380}"
export ECOMMERCE_MYSQL_PUBLISHED_PORT="${ECOMMERCE_MYSQL_PUBLISHED_PORT:-13317}"
export ECOMMERCE_REDPANDA_PUBLISHED_PORT="${ECOMMERCE_REDPANDA_PUBLISHED_PORT:-19094}"
export ECOMMERCE_DEMO_STOCK="${stock}"
export ECOMMERCE_DEMO_CAMPAIGN_LIMIT="${stock}"
export ECOMMERCE_MYSQL_POOL_SIZE="${ECOMMERCE_MYSQL_POOL_SIZE:-16}"
export ECOMMERCE_HTTP_WORKERS="${ECOMMERCE_HTTP_WORKERS:-8}"
export ECOMMERCE_FLASH_CONSUMER_WORKERS="${ECOMMERCE_FLASH_CONSUMER_WORKERS:-4}"
export ECOMMERCE_FLASH_SALE_QUEUE=rocketmq
export ECOMMERCE_BENCH_PROJECT="${project_name}"
export DEMO_BASE_URL="http://127.0.0.1:${gateway_port}"

compose() {
  docker compose -p "${project_name}" -f "${base_compose}" -f "${rocketmq_compose}" --profile rocketmq "$@"
}

mkdir -p "$(dirname -- "${report}")"
compose up --build -d --wait --wait-timeout 900
set +e
python3 "${project_dir}/deploy/scripts/http_flash_sale_benchmark.py" \
  --base-url "${DEMO_BASE_URL}" --requests "${requests}" --concurrency "${concurrency}" \
  --rate "${rate}" \
  >"${report}" 2>&1
result=$?
cat "${report}"
set -e
if [ "${result}" -eq 0 ]; then
  compose down
  printf 'Benchmark report saved to %s\n' "${report}"
else
  printf 'Benchmark failed; stack %s is left running for inspection. Report: %s\n' "${project_name}" "${report}" >&2
fi
exit "${result}"
