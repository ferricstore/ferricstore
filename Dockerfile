# ============================================================
# Stage 1: Rust toolchain
# ============================================================
ARG RUST_VERSION=1.94.0
FROM rust:${RUST_VERSION}-slim-bookworm@sha256:a86cada82e36ebd7a9bffed7548792c55a952fdb20718eea9278a936bcb76e62 AS rust-toolchain

# ============================================================
# Stage 2: Build
# ============================================================
FROM hexpm/elixir:1.19.5-erlang-28.4.1-ubuntu-noble-20260217@sha256:a0ee05779f7231b1f679ce540b63741e0ec56b181947ff00556b13370ad080f8 AS builder

# Install system deps + Rust
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates build-essential git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH="/usr/local/cargo/bin:$PATH"

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
COPY apps/ferricstore/priv/flow_query apps/ferricstore/priv/flow_query
COPY apps/ferricstore_server/native apps/ferricstore_server/native
COPY apps/ferricstore_server/lib apps/ferricstore_server/lib
COPY rel rel

RUN mix deps.unlock --unused && mix deps.get --only prod

# Compile everything (deps + app code)
RUN mix compile

# Build release
RUN mix release ferricstore

# ============================================================
# Stage 3: Runtime
# ============================================================
FROM ubuntu:noble-20260217@sha256:186072bba1b2f436cbb91ef2567abca677337cfc786c86e107d25b7072feef0c

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 libncurses6 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 ferricstore \
    && useradd --system --uid 10001 --gid ferricstore --home-dir /app --shell /usr/sbin/nologin ferricstore

WORKDIR /app

COPY --from=builder --chown=ferricstore:ferricstore /app/_build/prod/rel/ferricstore ./

# Create data directory
RUN mkdir -p /data && chown ferricstore:ferricstore /app /data

ENV FERRICSTORE_DATA_DIR=/data
ENV FERRICSTORE_NATIVE_PORT=6388
ENV FERRICSTORE_HEALTH_PORT=6380
ENV FERRICSTORE_HEALTH_PROBE_PORT=6381
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV ELIXIR_ERL_OPTIONS="+fnu"

EXPOSE 6388 6380 6381

USER ferricstore

CMD ["bin/ferricstore", "start"]
