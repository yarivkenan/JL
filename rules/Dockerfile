FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /rules-engine ./cmd/rules

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /rules-engine /rules-engine

ENTRYPOINT ["/rules-engine"]
