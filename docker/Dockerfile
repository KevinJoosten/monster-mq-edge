# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION

WORKDIR /src
RUN apk add --no-cache ca-certificates

COPY . .
RUN mkdir -p /out/cfg-data && \
    case "${TARGETARCH}${TARGETVARIANT}" in \
      armv6) export GOARM=6 ;; \
      armv7) export GOARM=7 ;; \
    esac && \
    VERSION_VAL="${VERSION}" && \
    if [ -z "${VERSION_VAL}" ] && [ -f version.txt ]; then \
      VERSION_VAL=$(cat version.txt | tr -d '\n' | tr -d '\r'); \
    fi && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
      go build -trimpath \
        -ldflags="-s -w -X monstermq.io/edge/internal/version.Version=${VERSION_VAL:-dev}" \
        -o /out/monstermq-edge ./cmd/monstermq-edge

FROM alpine:3

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/cfg-data /cfg-data

COPY --from=build /out/monstermq-edge /monstermq-edge
COPY docker/config.yaml /etc/monstermq/config.yaml
COPY docker/entrypoint.sh /entrypoint.sh

EXPOSE 1883 1884 8080

ENTRYPOINT ["/entrypoint.sh"]
