# Generated Dockerfile: alpine-temurin21
# Base: docker.io/library/eclipse-temurin:21-jre-alpine
# JVM: temurin
# Kafka: 3.7.0

FROM docker.io/library/eclipse-temurin:21-jre-alpine AS builder

# Install build deps (if needed for downloading Kafka)

RUN apk add --no-cache wget ca-certificates


# Download precompiled Kafka
WORKDIR /tmp
RUN wget -q https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz && \
    tar -xzf kafka_2.13-3.7.0.tgz && \
    mv kafka_2.13-3.7.0 /kafka

# Runtime stage
FROM docker.io/library/eclipse-temurin:21-jre-alpine



# Copy Kafka from builder
COPY --from=builder /kafka /opt/kafka

# Create kafka user
RUN useradd -r -m -u 1001 kafka || adduser -D -u 1001 kafka && \
    mkdir -p /var/lib/kafka/data && \
    chown -R kafka:kafka /opt/kafka /var/lib/kafka

# Set environment
ENV PATH=/opt/kafka/bin:$PATH
ENV KAFKA_HEAP_OPTS="-Xms128m -Xmx128m"

WORKDIR /opt/kafka
USER kafka

CMD ["/bin/bash"]

# Expose standard Kafka ports
EXPOSE 9092 9093

# Metadata
LABEL org.opencontainers.image.title="Kafka 3.7.0 (alpine-temurin21)" \
      org.opencontainers.image.description="Minimal Kafka broker - temurin on docker.io/library/eclipse-temurin:21-jre-alpine" \
      org.opencontainers.image.source="https://github.com/jmcgill-public/kuule-topology"