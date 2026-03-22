# ── Build stage ────────────────────────────────────────────
FROM rust:1.94-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --features cli

# ── Runtime stage ─────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/oversync /usr/local/bin/oversync
COPY --from=builder /build/surql/ /app/surql/

WORKDIR /app

ENV OVERSYNC_BIND=0.0.0.0:4200
ENV OVERSYNC_LOG_LEVEL=info
ENV OVERSYNC_SURREALDB_URL=http://surrealdb:8000
ENV OVERSYNC_SURREALDB_NS=oversync
ENV OVERSYNC_SURREALDB_DB=sync

EXPOSE 4200

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:4200/health || exit 1

ENTRYPOINT ["oversync"]
