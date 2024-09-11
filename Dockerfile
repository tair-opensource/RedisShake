FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o redis-shake ./cmd/redis-shake

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/redis-shake .
COPY shake_sync_env.toml .
COPY shake_scan_env.toml .
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/sh", "entrypoint.sh"]