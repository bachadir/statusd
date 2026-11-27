# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/statusd ./cmd/statusd && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/fakeprom ./cmd/fakeprom && \
    mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/statusd /usr/local/bin/statusd
COPY --from=build /out/fakeprom /usr/local/bin/fakeprom
COPY catalog /etc/statusd/catalog
# Named volumes inherit this ownership, so the nonroot user can write raft and SQLite data.
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME /data
USER nonroot
EXPOSE 8080 8081 8443 7000
ENTRYPOINT ["/usr/local/bin/statusd"]
CMD ["-config", "/etc/statusd/statusd.yaml", "-log-json"]
