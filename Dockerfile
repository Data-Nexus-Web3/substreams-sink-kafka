FROM --platform=$BUILDPLATFORM golang:1.24-bullseye AS build

WORKDIR /src

ARG TARGETOS TARGETARCH VERSION=dev

# Install cross-compilation toolchains and librdkafka for both architectures
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential pkg-config ca-certificates \
      gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
      librdkafka-dev libssl-dev zlib1g-dev && \
    if [ "$TARGETARCH" = "arm64" ]; then \
      dpkg --add-architecture arm64 && \
      apt-get update && \
      apt-get install -y --no-install-recommends \
        librdkafka-dev:arm64 libssl-dev:arm64 zlib1g-dev:arm64; \
    fi && \
    rm -rf /var/lib/apt/lists/*

ENV CGO_ENABLED=1

# Set cross-compiler for ARM64 builds
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      echo "CC=aarch64-linux-gnu-gcc" >> /etc/environment; \
    fi

# Leverage layer caching for dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY . .

# Debug: show source tree in CI to confirm context
RUN ls -la /src && ls -la /src/cmd || true && ls -la /src/cmd/substreams-sink-kafka || true && find /src -maxdepth 3 -name '*.go' -print || true

# Build binary from module root (fork- and path-safe)
WORKDIR /src
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      CC=aarch64-linux-gnu-gcc GOOS=$TARGETOS GOARCH=$TARGETARCH \
        go build -ldflags "-X main.version=$VERSION" -o /app/substreams-sink-kafka ./cmd/substreams-sink-kafka; \
    else \
      GOOS=$TARGETOS GOARCH=$TARGETARCH \
        go build -ldflags "-X main.version=$VERSION" -o /app/substreams-sink-kafka ./cmd/substreams-sink-kafka; \
    fi

FROM ubuntu:24.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    ca-certificates vim strace htop lsof curl jq libssl3 librdkafka1 && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

COPY --from=build /app/substreams-sink-kafka /app/substreams-sink-kafka

ENV PATH=/app:$PATH

ENTRYPOINT ["/app/substreams-sink-kafka"]