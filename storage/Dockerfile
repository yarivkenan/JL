FROM golang:1.26-alpine AS builder

ARG CMD=consumer

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /app-binary ./storage/cmd/${CMD}

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app-binary /app

ENTRYPOINT ["/app"]
