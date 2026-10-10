#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compose_file="${project_dir}/deploy/compose/docker-compose.yml"
build_dir="${BUILD_DIR:-${project_dir}/build-external}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is required" >&2
  exit 1
fi

docker compose -f "${compose_file}" up -d --wait --wait-timeout 180 redis mysql redpanda

# Give this validation run an isolated database with the repository's current
# schema. The Compose MySQL volume may predate newer tables in schema.sql.
docker compose -f "${compose_file}" exec -T mysql mysql -uroot -proot-password -e \
  'DROP DATABASE IF EXISTS ecommerce_validation; CREATE DATABASE ecommerce_validation; GRANT ALL PRIVILEGES ON ecommerce_validation.* TO '\''ecommerce'\''@'\''%'\'';'
docker compose -f "${compose_file}" exec -T mysql mysql -uroot -proot-password ecommerce_validation \
  < "${project_dir}/src/storage/mysql/schema.sql"

cmake -S "${project_dir}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DENABLE_EXTERNAL_TESTS=ON -DENABLE_REDIS=ON -DENABLE_KAFKA=ON -DENABLE_MYSQL=ON
for target in redis_concurrency_test external_smoke_test kafka_flash_sale_test mysql_transaction_test; do
  cmake --build "${build_dir}" --target "${target}" --parallel
done
cmake --build "${build_dir}" --parallel

export ECOMMERCE_REDIS_TEST_HOST=127.0.0.1
export ECOMMERCE_REDIS_TEST_PORT=6379
export ECOMMERCE_KAFKA_TEST_BROKERS=127.0.0.1:19092
export ECOMMERCE_MYSQL_TEST_HOST=127.0.0.1
export ECOMMERCE_MYSQL_TEST_PORT=13306
export ECOMMERCE_MYSQL_TEST_USER=ecommerce
export ECOMMERCE_MYSQL_TEST_PASSWORD=ecommerce
export ECOMMERCE_MYSQL_TEST_DATABASE=ecommerce_validation

ctest --test-dir "${build_dir}" --output-on-failure
