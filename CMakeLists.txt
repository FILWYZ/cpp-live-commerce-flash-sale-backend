cmake_minimum_required(VERSION 3.20)
project(cpp_ecommerce_platform LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_REDIS "Build hiredis Redis adapter when hiredis is installed" ON)
option(ENABLE_KAFKA "Build librdkafka adapter when librdkafka is installed" ON)
option(ENABLE_ROCKETMQ "Build Apache RocketMQ 5.x C++ SDK adapter when installed" ON)
option(ENABLE_MYSQL "Build MySQL transaction repository when mysqlclient is installed" ON)
option(ENABLE_EXTERNAL_TESTS "Build tests that require running Redis and Kafka" OFF)

add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_17)
target_compile_options(project_options INTERFACE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic>
)

if(ENABLE_ASAN)
    target_compile_options(project_options INTERFACE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(project_options INTERFACE -fsanitize=address)
endif()

if(ENABLE_TSAN)
    target_compile_options(project_options INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(project_options INTERFACE -fsanitize=thread)
endif()

if(ENABLE_UBSAN)
    target_compile_options(project_options INTERFACE -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(project_options INTERFACE -fsanitize=undefined)
endif()

enable_testing()

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_library(ecommerce_security
    src/common/security/password_hasher.cc
    src/common/security/webhook_signature.cc
)
target_include_directories(ecommerce_security PUBLIC src)
target_link_libraries(ecommerce_security PUBLIC project_options OpenSSL::Crypto)

add_library(ecommerce_network
    src/network/buffer.cc
    src/network/reactor/channel.cc
    src/network/reactor/event_loop.cc
    src/network/reactor/tcp_acceptor.cc
)
target_include_directories(ecommerce_network PUBLIC src)
target_link_libraries(ecommerce_network PUBLIC project_options Threads::Threads)

add_library(ecommerce_gateway
    src/gateway/http/http_parser.cc
    src/gateway/http/http_response.cc
    src/gateway/http/http_server.cc
    src/gateway/router.cc
)
target_include_directories(ecommerce_gateway PUBLIC src)
target_link_libraries(ecommerce_gateway PUBLIC project_options ecommerce_network)

add_library(ecommerce_storage
    src/storage/wal/wal.cc
    src/storage/snapshot/snapshot_store.cc
    src/storage/local_engine/local_kv_engine.cc
    src/storage/local_engine/snapshot_worker.cc
)
target_include_directories(ecommerce_storage PUBLIC src)
target_link_libraries(ecommerce_storage PUBLIC project_options Threads::Threads)

add_library(ecommerce_observability
    src/observability/metrics/metrics_registry.cc
    src/observability/logging/structured_logger.cc
)
target_include_directories(ecommerce_observability PUBLIC src)
target_link_libraries(ecommerce_observability PUBLIC project_options Threads::Threads)

add_library(ecommerce_business
    src/business/inventory/inventory_service.cc
    src/business/inventory/inventory_gateway.cc
    src/business/inventory/inventory_reconciler.cc
    src/business/product/product_service.cc
    src/business/promotion/promotion_service.cc
    src/business/promotion/flash_sale_gate.cc
    src/business/flash_sale/flash_sale_order_service.cc
    src/business/flash_sale/flash_sale_order_consumer.cc
    src/business/flash_sale/flash_sale_request_status_store.cc
    src/business/flash_sale/local_flash_sale_order_repository.cc
    src/business/order/order_service.cc
    src/business/user/user_service.cc
)
target_include_directories(ecommerce_business PUBLIC src)
target_link_libraries(ecommerce_business PUBLIC project_options ecommerce_cache ecommerce_messaging ecommerce_storage ecommerce_security ecommerce_kv)

if(ENABLE_MYSQL)
    find_path(MYSQL_INCLUDE_DIR mysql/mysql.h
              PATHS
              /opt/homebrew/opt/mysql-client/include
              /opt/homebrew/opt/mariadb-connector-c/include
              /usr/local/opt/mysql-client/include
              /usr/local/opt/mariadb-connector-c/include
              /usr/include /usr/local/include /opt/homebrew/include
              PATH_SUFFIXES mariadb)
    find_library(MYSQL_LIBRARY NAMES mysqlclient mariadb
                 PATHS
                 /opt/homebrew/opt/mysql-client/lib
                 /opt/homebrew/opt/mariadb-connector-c/lib
                 /usr/local/opt/mysql-client/lib
                 /usr/local/opt/mariadb-connector-c/lib
                 /usr/lib /usr/local/lib /opt/homebrew/lib
                 /usr/lib/aarch64-linux-gnu /usr/lib/x86_64-linux-gnu)
    if(MYSQL_INCLUDE_DIR AND MYSQL_LIBRARY)
        add_library(ecommerce_mysql
            src/storage/mysql/mysql_connection_pool.cc
            src/storage/mysql/mysql_transaction_store.cc)
        target_include_directories(ecommerce_mysql PUBLIC src ${MYSQL_INCLUDE_DIR})
        target_link_libraries(ecommerce_mysql PUBLIC project_options ecommerce_business ecommerce_observability ${MYSQL_LIBRARY})
        target_compile_definitions(ecommerce_mysql PUBLIC ECOMMERCE_HAS_MYSQL=1)
    else()
        message(STATUS "mysqlclient not found; MySQL transaction repository is disabled")
    endif()
endif()

add_library(ecommerce_kv
    src/kv/kv_store.cc
)
target_include_directories(ecommerce_kv PUBLIC src)
target_link_libraries(ecommerce_kv PUBLIC project_options)

if(ENABLE_REDIS)
    find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h PATHS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(HIREDIS_LIBRARY hiredis PATHS /opt/homebrew/lib /usr/local/lib /usr/lib /usr/lib/aarch64-linux-gnu /usr/lib/x86_64-linux-gnu)
    if(HIREDIS_INCLUDE_DIR AND HIREDIS_LIBRARY)
        add_library(ecommerce_redis
            src/kv/redis_kv_store.cc
            src/kv/redis_flash_sale_gate.cc
            src/kv/redis_flash_sale_waiting_room.cc
            src/kv/redis_inventory_fast_path.cc
            src/kv/redis_inventory_gateway.cc
            src/kv/redis_order_queue.cc)
        target_sources(ecommerce_redis PRIVATE src/traffic/redis_token_bucket.cc)
        target_include_directories(ecommerce_redis PUBLIC src)
        target_include_directories(ecommerce_redis SYSTEM PUBLIC ${HIREDIS_INCLUDE_DIR})
        target_link_libraries(ecommerce_redis PUBLIC project_options ecommerce_kv ${HIREDIS_LIBRARY})
        target_compile_definitions(ecommerce_redis PUBLIC ECOMMERCE_HAS_REDIS=1)
    else()
        message(STATUS "hiredis not found; Redis adapter is disabled")
    endif()
endif()

add_library(ecommerce_cache
    src/cache/sharded_cache.cc
)
target_include_directories(ecommerce_cache PUBLIC src)
target_link_libraries(ecommerce_cache PUBLIC project_options ecommerce_kv Threads::Threads)

add_library(ecommerce_messaging
    src/messaging/in_memory_broker.cc
    src/messaging/async_outbox_dispatcher.cc
    src/messaging/order_message.cc
    src/messaging/order_queue.cc
)
target_include_directories(ecommerce_messaging PUBLIC src)
target_link_libraries(ecommerce_messaging PUBLIC project_options Threads::Threads)

if(ENABLE_KAFKA)
    find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h PATHS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(RDKAFKA_LIBRARY rdkafka PATHS /opt/homebrew/lib /usr/local/lib /usr/lib /usr/lib/aarch64-linux-gnu /usr/lib/x86_64-linux-gnu)
    if(RDKAFKA_INCLUDE_DIR AND RDKAFKA_LIBRARY)
        add_library(ecommerce_kafka
    src/messaging/kafka_client.cc
    src/messaging/kafka_dlq_operator.cc
            src/messaging/kafka_order_queue.cc
            src/messaging/kafka_flash_sale_consumer.cc)
        target_include_directories(ecommerce_kafka PUBLIC src)
        target_include_directories(ecommerce_kafka SYSTEM PUBLIC ${RDKAFKA_INCLUDE_DIR})
        target_link_libraries(ecommerce_kafka PUBLIC project_options ecommerce_messaging ${RDKAFKA_LIBRARY})
        target_compile_definitions(ecommerce_kafka PUBLIC ECOMMERCE_HAS_KAFKA=1)
        if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
            target_compile_options(ecommerce_kafka PRIVATE -Wno-gnu-statement-expression -Wno-gnu-statement-expression-from-macro-expansion)
        endif()
    else()
        message(STATUS "librdkafka not found; Kafka adapter is disabled")
    endif()
endif()

if(ENABLE_ROCKETMQ)
    find_package(rocketmq CONFIG QUIET)
    set(ROCKETMQ_SDK_TARGET "")
    if(TARGET rocketmq::rocketmq_shared)
        set(ROCKETMQ_SDK_TARGET rocketmq::rocketmq_shared)
    elseif(TARGET rocketmq)
        set(ROCKETMQ_SDK_TARGET rocketmq)
    elseif(TARGET rocketmq_shared)
        set(ROCKETMQ_SDK_TARGET rocketmq_shared)
    endif()
    if(ROCKETMQ_SDK_TARGET)
        add_library(ecommerce_rocketmq src/messaging/rocketmq_order_queue.cc)
        target_include_directories(ecommerce_rocketmq PUBLIC src)
        target_link_libraries(ecommerce_rocketmq PUBLIC project_options ecommerce_messaging ${ROCKETMQ_SDK_TARGET})
        target_compile_definitions(ecommerce_rocketmq PUBLIC ECOMMERCE_HAS_ROCKETMQ=1)
    else()
        message(STATUS "Apache RocketMQ C++ SDK not found; RocketMQ adapter is disabled")
    endif()
endif()

add_executable(first_stage_test tests/unit/first_stage_test.cc)
target_link_libraries(first_stage_test PRIVATE project_options ecommerce_gateway ecommerce_business ecommerce_storage ecommerce_observability)
add_test(NAME first_stage_test COMMAND first_stage_test)

add_executable(storage_recovery_test tests/integration/storage_recovery_test.cc)
target_link_libraries(storage_recovery_test PRIVATE project_options ecommerce_business)
add_test(NAME storage_recovery_test COMMAND storage_recovery_test)

add_executable(commerce_concurrency_test tests/integration/commerce_concurrency_test.cc)
target_link_libraries(commerce_concurrency_test PRIVATE project_options ecommerce_business)
add_test(NAME commerce_concurrency_test COMMAND commerce_concurrency_test)

add_executable(security_outbox_test tests/integration/security_outbox_test.cc)
target_link_libraries(security_outbox_test PRIVATE project_options ecommerce_business ecommerce_messaging ecommerce_storage ecommerce_security)
add_test(NAME security_outbox_test COMMAND security_outbox_test)

add_executable(flash_sale_pipeline_test tests/integration/flash_sale_pipeline_test.cc)
target_link_libraries(flash_sale_pipeline_test PRIVATE project_options ecommerce_business)
add_test(NAME flash_sale_pipeline_test COMMAND flash_sale_pipeline_test)

add_executable(ecommerce_api_demo apps/ecommerce_api_main.cc)
target_link_libraries(ecommerce_api_demo PRIVATE project_options ecommerce_gateway ecommerce_observability)
if(TARGET ecommerce_mysql)
    target_link_libraries(ecommerce_api_demo PRIVATE ecommerce_mysql)
    target_compile_definitions(ecommerce_api_demo PRIVATE ECOMMERCE_HAS_MYSQL=1)
else()
    target_link_libraries(ecommerce_api_demo PRIVATE ecommerce_business)
endif()
if(TARGET ecommerce_redis)
    target_link_libraries(ecommerce_api_demo PRIVATE ecommerce_redis)
    target_compile_definitions(ecommerce_api_demo PRIVATE ECOMMERCE_HAS_REDIS=1)
endif()
if(TARGET ecommerce_kafka)
    target_link_libraries(ecommerce_api_demo PRIVATE ecommerce_kafka)
    target_compile_definitions(ecommerce_api_demo PRIVATE ECOMMERCE_HAS_KAFKA=1)
endif()
if(TARGET ecommerce_rocketmq)
    target_link_libraries(ecommerce_api_demo PRIVATE ecommerce_rocketmq)
    target_compile_definitions(ecommerce_api_demo PRIVATE ECOMMERCE_HAS_ROCKETMQ=1)

    add_executable(rocketmq_smoke_test tools/rocketmq_smoke_test.cc)
    target_link_libraries(rocketmq_smoke_test PRIVATE project_options ecommerce_rocketmq)
endif()

add_executable(http_load_generator tools/http_load_generator.cc)
target_link_libraries(http_load_generator PRIVATE project_options Threads::Threads)

add_executable(http_business_load_generator tools/http_business_load_generator.cc)
target_link_libraries(http_business_load_generator PRIVATE project_options Threads::Threads)

add_executable(inventory_benchmark benchmarks/flash_sale/inventory_benchmark.cc)
target_link_libraries(inventory_benchmark PRIVATE project_options ecommerce_business ecommerce_messaging ecommerce_cache ecommerce_kv Threads::Threads)

add_executable(flash_sale_soak_test tools/flash_sale_soak_test.cc)
target_link_libraries(flash_sale_soak_test PRIVATE project_options ecommerce_business)

if(ENABLE_EXTERNAL_TESTS AND TARGET ecommerce_redis)
    add_executable(redis_concurrency_test tools/redis_concurrency_test.cc)
    target_link_libraries(redis_concurrency_test PRIVATE project_options ecommerce_redis ecommerce_messaging)
    add_test(NAME redis_concurrency_test COMMAND redis_concurrency_test)
endif()

if(ENABLE_EXTERNAL_TESTS AND TARGET ecommerce_redis AND TARGET ecommerce_kafka)
    add_executable(external_smoke_test tools/external_smoke_test.cc)
    target_link_libraries(external_smoke_test PRIVATE project_options ecommerce_redis ecommerce_kafka ecommerce_messaging)
    add_test(NAME external_smoke_test COMMAND external_smoke_test)
    add_executable(kafka_flash_sale_test tools/kafka_flash_sale_test.cc)
    target_link_libraries(kafka_flash_sale_test PRIVATE project_options ecommerce_kafka ecommerce_business)
    add_test(NAME kafka_flash_sale_test COMMAND kafka_flash_sale_test)
endif()

if(TARGET ecommerce_mysql)
    add_executable(mysql_transaction_test tests/integration/mysql_transaction_test.cc)
    target_link_libraries(mysql_transaction_test PRIVATE project_options ecommerce_mysql)
    add_test(NAME mysql_transaction_test COMMAND mysql_transaction_test)
    add_executable(mysql_capacity_test tools/mysql_capacity_test.cc)
    target_link_libraries(mysql_capacity_test PRIVATE project_options ecommerce_mysql Threads::Threads)
endif()

# 当前仓库聚焦可运行、可测试的单机后端交易主链路；Redis/Kafka/MySQL 为可选外部适配。
