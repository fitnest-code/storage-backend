# Build Stage
FROM golang:1.26-alpine AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

# Install build tools & CA certificates
RUN apk add --no-cache git ca-certificates

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static Go binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o storage-backend ./cmd/server

# Final Runtime Stage
FROM alpine:3.20

WORKDIR /app

# Add runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy binary from builder
COPY --from=builder /app/storage-backend /app/storage-backend
COPY --from=builder /app/src/locales /app/src/locales

# Create required directories and set ownership
RUN mkdir -p /app/local_storage /app/temp_uploads && \
    chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080 9090

ENTRYPOINT ["/app/storage-backend"]
