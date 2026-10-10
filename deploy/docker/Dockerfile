FROM ubuntu:24.04 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake libssl-dev libhiredis-dev librabbitmq-dev default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake -S . -B /tmp/build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /tmp/build --target api_server order_consumer reconciler ecommerce_api_demo rabbitmq_smoke_test --parallel

FROM ubuntu:24.04 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 libssl3 libhiredis1.1.0 librabbitmq4 libmysqlclient21 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /tmp/build/api_server /app/api_server
COPY --from=build /tmp/build/order_consumer /app/order_consumer
COPY --from=build /tmp/build/reconciler /app/reconciler
COPY --from=build /tmp/build/ecommerce_api_demo /app/ecommerce_api_demo
COPY --from=build /tmp/build/rabbitmq_smoke_test /app/rabbitmq_smoke_test
COPY configs /app/configs

EXPOSE 8080
ENTRYPOINT ["/app/api_server", "8080"]
