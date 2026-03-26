# Stage 1: build
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /ingest-server ./ingest/cmd/server

# Stage 2: minimal runtime image
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /ingest-server /usr/local/bin/ingest-server

EXPOSE 4317

ENTRYPOINT ["ingest-server"]
