# ============================================================
# Stage 1: Build
# ============================================================
FROM hexpm/elixir:1.19.5-erlang-28.4.1-ubuntu-noble-20260217 AS builder

# Install system deps + Rust
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential git \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /app

# Install hex + rebar
RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV=prod
ENV HEX_HTTP_TIMEOUT=120
ENV HEX_HTTP_CONCURRENCY=2

# Copy mix files (all apps needed for umbrella resolution)
COPY mix.exs mix.lock ./
COPY apps/ferricstore/mix.exs apps/ferricstore/mix.exs
COPY apps/ferricstore_server/mix.exs apps/ferricstore_server/mix.exs
COPY config/config.exs config/prod.exs config/runtime.exs config/

# Copy source for the standalone Docker image
COPY apps/ferricstore/native apps/ferricstore/native
COPY apps/ferricstore/lib apps/ferricstore/lib
COPY apps/ferricstore/src apps/ferricstore/src
COPY apps/ferricstore/priv apps/ferricstore/priv
COPY apps/ferricstore_server/native apps/ferricstore_server/native
COPY apps/ferricstore_server/lib apps/ferricstore_server/lib
COPY rel rel

RUN mix deps.unlock --unused && mix deps.get --only prod

# Compile everything (deps + app code)
RUN mix compile

# Build release
RUN mix release ferricstore

# ============================================================
# Stage 2: Runtime
# ============================================================
FROM ubuntu:noble-20260217

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 libncurses6 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/_build/prod/rel/ferricstore ./

# Create data directory
RUN mkdir -p /data

ENV FERRICSTORE_DATA_DIR=/data
ENV FERRICSTORE_PORT=6379
ENV FERRICSTORE_HEALTH_PORT=6380
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV ELIXIR_ERL_OPTIONS="+fnu"

EXPOSE 6379 6380

CMD ["bin/ferricstore", "start"]
