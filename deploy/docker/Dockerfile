FROM ubuntu:24.04 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake libssl-dev libhiredis-dev librdkafka-dev default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake -S . -B /tmp/build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /tmp/build --target ecommerce_api_demo --parallel

FROM ubuntu:24.04 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 libssl3 libhiredis1.1.0 librdkafka1 libmysqlclient21 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /tmp/build/ecommerce_api_demo /app/ecommerce_api_demo
COPY configs /app/configs

EXPOSE 8080
ENTRYPOINT ["/app/ecommerce_api_demo", "8080"]
