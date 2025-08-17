FROM --platform=$BUILDPLATFORM golang:1.24-bullseye AS build

WORKDIR /src

ARG TARGETOS TARGETARCH VERSION=dev

RUN --mount=target=. \
      --mount=type=cache,target=/root/.cache/go-build \
      --mount=type=cache,target=/go/pkg \
      GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-X \"main.version=$VERSION\"" -o /app/substreams-sink-kafka ./cmd/substreams-sink-kafka

FROM ubuntu:24.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    ca-certificates vim strace htop lsof curl jq libssl3 && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

COPY --from=build /app/substreams-sink-kafka /app/substreams-sink-kafka

ENV PATH "/app:$PATH"

ENTRYPOINT ["/app/substreams-sink-kafka"]