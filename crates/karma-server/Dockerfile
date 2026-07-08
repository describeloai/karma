# karma-server — the Karma SQL engine as a container (sibling of the ml-runner).
# Build context = the Karma workspace root:  docker build -f crates/karma-server/Dockerfile .
#
# Stateless → scale horizontally. Reads the Iceberg warehouse on R2 directly (its own
# pruning); asks the ml-runner only for data-file paths (cheap metadata call).

# ── build ──────────────────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder
WORKDIR /app
# protoc is required by karma-sql (datafusion-substrait build.rs). `libprotobuf-dev`
# ships the well-known includes (google/protobuf/*.proto) that substrait's .proto files
# import — `protobuf-compiler` alone does NOT provide them on Debian bookworm.
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p karma-server

# ── runtime ────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/karma-server /usr/local/bin/karma-server
# Config via env (see src/main.rs): ML_RUNNER_URL, ML_RUNNER_TOKEN, LAKEHOUSE_S3_*, KARMA_BIND.
ENV KARMA_BIND=0.0.0.0:8088
EXPOSE 8088
CMD ["karma-server"]
