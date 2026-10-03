FROM golang:1.22-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN go build -o /out/coordinator ./cmd/coordinator && \
    go build -o /out/worker      ./cmd/worker && \
    go build -o /out/web         ./cmd/web

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /out/ ./
COPY web ./web
EXPOSE 50051 9090 8080
