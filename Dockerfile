# ── Build stage ────────────────────────────────────────────
FROM rust:1.94-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release --features cli

# ── Runtime stage ─────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/overrealdb/oversync"
LABEL org.opencontainers.image.description="oversync - a lightweight data sync engine"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /build/target/release/oversync /usr/local/bin/oversync
COPY --from=builder /build/surql/ /app/surql/
COPY --from=builder /usr/lib/*-linux-gnu/libz.so.1 /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu/libssl.so.3 /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu/libcrypto.so.3 /usr/lib/

WORKDIR /app

ENV OVERSYNC_BIND=0.0.0.0:4200
ENV OVERSYNC_LOG_LEVEL=info
ENV OVERSYNC_SURREALDB_URL=http://surrealdb:8000
ENV OVERSYNC_SURREALDB_NS=oversync
ENV OVERSYNC_SURREALDB_DB=sync

EXPOSE 4200

USER nonroot

ENTRYPOINT ["oversync"]
