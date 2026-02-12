FROM golang:1.24.13-alpine3.23 AS base
WORKDIR /src
RUN apk add --no-cache ca-certificates

FROM base AS gotidy
CMD ["go", "mod", "tidy"]

FROM base AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY src/go.mod ./
COPY src/go.sum ./
RUN go mod download
COPY src/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /out/clickhouse-dash

FROM scratch AS production
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/clickhouse-dash /clickhouse-dash
EXPOSE 8080
ENV PORT=8080
HEALTHCHECK --interval=5s\
            --timeout=1s \
            --start-interval=500ms \
            --start-period=5s \
            --retries=1 \
            CMD ["/clickhouse-dash", "--health"]
ENTRYPOINT ["/clickhouse-dash"]