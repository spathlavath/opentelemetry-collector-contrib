# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies including gcc for CGO
RUN apk add --no-cache \
    git \
    make \
    bash \
    curl \
    gcc \
    musl-dev \
    libc-dev

# Set working directory to the repo root
WORKDIR /workspace

# Copy the entire repository
COPY ../../.. .

RUN make otelcontribcol

# Build the collector using the standard make command
# This will:
# 1. Install builder tool to .tools/builder
# 2. Run builder with builder-config.yaml
# 3. Generate code in cmd/otelcontribcol/
# 4. Build the binary

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libc6-compat

# Create a non-root user
RUN addgroup -S otel && adduser -S otel -G otel

# Copy the binary from builder
# The binary is created in cmd/otelcontribcol/otelcontribcol
COPY --from=builder /workspace/bin /usr/local/bin/

# Copy the config file
COPY --from=builder /workspace/receiver/newrelicmysqlreceiver/test/otel-config.yaml /etc/otel/config.yaml

# CMD ["sleep", "1h"]
# Set ownership
RUN chown -R otel:otel /usr/local/bin/otelcontribcol_linux_arm64 /etc/otel

# Switch to non-root user
USER otel
# Expose standard OTLP ports (optional, for receiving data)
EXPOSE 4317 4318 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
CMD wget --no-verbose --tries=1 --spider http://localhost:8888/ || exit 1

# Run the collector
ENTRYPOINT ["/usr/local/bin/otelcontribcol_linux_arm64"]
CMD ["--config=/etc/otel/config.yaml"]