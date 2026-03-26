FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /consumer ./storage/cmd/consumer

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /consumer /consumer

ENTRYPOINT ["/consumer"]
