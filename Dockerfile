# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvnode ./cmd/kvnode && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvctl ./cmd/kvctl
# Pre-create the data directory so named volumes inherit nonroot ownership.
RUN mkdir -p /data && chown 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvnode /out/kvctl /usr/local/bin/
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 7000
ENTRYPOINT ["kvnode"]
