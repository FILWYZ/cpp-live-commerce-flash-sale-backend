#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compose_file="${project_dir}/deploy/compose/docker-compose.yml"
build_dir="${BUILD_DIR:-${project_dir}/build-external}"
project_name="${ECOMMERCE_EXTERNAL_PROJECT:-live-commerce-external-$(date +%Y%m%d%H%M%S)}"

export ECOMMERCE_REDIS_PUBLISHED_PORT="${ECOMMERCE_REDIS_PUBLISHED_PORT:-16379}"
export ECOMMERCE_MYSQL_PUBLISHED_PORT="${ECOMMERCE_MYSQL_PUBLISHED_PORT:-13316}"
export ECOMMERCE_RABBITMQ_PUBLISHED_PORT="${ECOMMERCE_RABBITMQ_PUBLISHED_PORT:-5673}"
export ECOMMERCE_RABBITMQ_MANAGEMENT_PUBLISHED_PORT="${ECOMMERCE_RABBITMQ_MANAGEMENT_PUBLISHED_PORT:-15673}"

compose() { docker compose -p "${project_name}" -f "${compose_file}" "$@"; }
cleanup() { compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is required" >&2
  exit 1
fi

compose build api-server
compose up -d --wait --wait-timeout 180 redis mysql rabbitmq

# Give this validation run an isolated database with the repository's current
# schema. The Compose MySQL volume may predate newer tables in schema.sql.
compose exec -T mysql mysql -uroot -proot-password -e \
  'DROP DATABASE IF EXISTS ecommerce_validation; CREATE DATABASE ecommerce_validation; GRANT ALL PRIVILEGES ON ecommerce_validation.* TO '\''ecommerce'\''@'\''%'\'';'
compose exec -T mysql mysql -uroot -proot-password ecommerce_validation \
  < "${project_dir}/src/storage/mysql/schema.sql"

cmake -S "${project_dir}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DENABLE_EXTERNAL_TESTS=ON -DENABLE_REDIS=ON -DENABLE_MYSQL=ON -DENABLE_RABBITMQ=ON
for target in redis_concurrency_test mysql_transaction_test; do
  if [ ! -f "${build_dir}/build.ninja" ] && [ ! -f "${build_dir}/Makefile" ]; then continue; fi
  cmake --build "${build_dir}" --target "${target}" --parallel
done
cmake --build "${build_dir}" --parallel

export ECOMMERCE_REDIS_TEST_HOST=127.0.0.1
export ECOMMERCE_REDIS_TEST_PORT="${ECOMMERCE_REDIS_PUBLISHED_PORT}"
export ECOMMERCE_MYSQL_TEST_HOST=127.0.0.1
export ECOMMERCE_MYSQL_TEST_PORT="${ECOMMERCE_MYSQL_PUBLISHED_PORT}"
export ECOMMERCE_MYSQL_TEST_USER=ecommerce
export ECOMMERCE_MYSQL_TEST_PASSWORD=ecommerce
export ECOMMERCE_MYSQL_TEST_DATABASE=ecommerce_validation

ctest --test-dir "${build_dir}" --output-on-failure
compose run --rm --no-deps --entrypoint /app/rabbitmq_smoke_test \
  -e ECOMMERCE_RABBITMQ_HOST=rabbitmq -e ECOMMERCE_RABBITMQ_PORT=5672 api-server
