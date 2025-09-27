FROM --platform=$BUILDPLATFORM golang:1.24-bullseye AS build

WORKDIR /src

ARG TARGETOS TARGETARCH VERSION=dev

# Tooling and native deps for confluent-kafka-go (librdkafka)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential pkg-config librdkafka-dev libssl-dev zlib1g-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV CGO_ENABLED=1

# Leverage layer caching for dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY . .

# Debug: show source tree in CI to confirm context
RUN ls -la /src && ls -la /src/cmd || true && ls -la /src/cmd/substreams-sink-kafka || true && find /src -maxdepth 3 -name '*.go' -print || true

# Build binary from module root (fork- and path-safe)
WORKDIR /src
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-X main.version=$VERSION" -o /app/substreams-sink-kafka ./cmd/substreams-sink-kafka

FROM ubuntu:24.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    ca-certificates vim strace htop lsof curl jq libssl3 librdkafka1 && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

COPY --from=build /app/substreams-sink-kafka /app/substreams-sink-kafka

ENV PATH=/app:$PATH

ENTRYPOINT ["/app/substreams-sink-kafka"]