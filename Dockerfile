# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION

WORKDIR /src
COPY go.mod go.sum ./
COPY mochi-mqtt-server/go.mod mochi-mqtt-server/go.sum ./mochi-mqtt-server/
RUN go mod download
COPY . .

ENV CGO_ENABLED=0
RUN VERSION_VAL="${VERSION}" && \
    if [ -z "${VERSION_VAL}" ] && [ -f version.txt ]; then \
      VERSION_VAL=$(cat version.txt | tr -d '\n' | tr -d '\r'); \
    fi && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags="-s -w -X monstermq.io/edge/internal/version.Version=${VERSION_VAL:-dev}" \
      -o /out/monstermq-edge ./cmd/monstermq-edge

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/monstermq-edge /monstermq-edge
COPY --from=build /src/config.yaml.example /etc/monstermq/config.yaml
EXPOSE 1883 1884 8080 8883 8884
USER nonroot:nonroot
ENTRYPOINT ["/monstermq-edge"]
CMD ["-config", "/etc/monstermq/config.yaml"]
