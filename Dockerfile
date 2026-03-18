FROM rust:1.94 AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --features cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/oversync /usr/local/bin/oversync
COPY --from=builder /build/surql/ /app/surql/
WORKDIR /app
ENTRYPOINT ["oversync"]
CMD ["--config", "oversync.toml"]
