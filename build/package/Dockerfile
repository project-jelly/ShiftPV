FROM golang:1.26.8 AS build-base

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY src ./src

FROM build-base AS build-controller
ARG CONTROLLER_VERSION=dev
RUN export GOOS="${TARGETOS:-$(go env GOOS)}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    && CGO_ENABLED=0 go build \
    -trimpath -ldflags="-s -w -X main.version=${CONTROLLER_VERSION}" \
    -o /out/shiftpv-controller ./src/cmd/controller \
    && CGO_ENABLED=0 go build \
    -trimpath -ldflags="-s -w" \
    -o /out/shiftpv-uninstall-guard ./src/cmd/uninstall-guard
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/shiftpv-volume-helper ./src/cmd/volume-helper

FROM build-base AS build-node
ARG NODE_VERSION=dev
RUN export GOOS="${TARGETOS:-$(go env GOOS)}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    && CGO_ENABLED=0 go build \
    -trimpath -ldflags="-s -w -X main.version=${NODE_VERSION}" \
    -o /out/shiftpv-node ./src/cmd/node

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base AS controller
RUN apt-get update \
    && apt-get install -y --no-install-recommends rsync \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build-controller /out/shiftpv-controller /shiftpv-controller
COPY --from=build-controller /out/shiftpv-uninstall-guard /shiftpv-uninstall-guard
COPY --from=build-controller /out/shiftpv-volume-helper /shiftpv-volume-helper
USER 65532:65532
ENTRYPOINT ["/shiftpv-controller"]

FROM runtime-base AS node
RUN apt-get update \
    && apt-get install -y --no-install-recommends mount \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build-node /out/shiftpv-node /shiftpv-node
USER 0:0
ENTRYPOINT ["/shiftpv-node"]

# Local kind tests use one loaded image while release builds keep controller
# and node runtimes independent.
FROM runtime-base AS combined
RUN apt-get update \
    && apt-get install -y --no-install-recommends mount rsync \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build-controller /out/shiftpv-controller /shiftpv-controller
COPY --from=build-controller /out/shiftpv-uninstall-guard /shiftpv-uninstall-guard
COPY --from=build-controller /out/shiftpv-volume-helper /shiftpv-volume-helper
COPY --from=build-node /out/shiftpv-node /shiftpv-node
USER 65532:65532
ENTRYPOINT ["/shiftpv-controller"]
