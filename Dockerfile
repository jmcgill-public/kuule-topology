# KRaft-based Kafka + Spring Boot Runtime
# Purpose: Enterprise event bridge with CloudEvents support
# Base: Eclipse Temurin OpenJDK 21 on Ubuntu 22.04

FROM docker.io/library/eclipse-temurin:21-jdk-jammy

# Build arguments
ARG KAFKA_VERSION=3.7.0
ARG SCALA_VERSION=2.13

# Install build dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    unzip \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Create kafka user and directories
RUN useradd -r -m -u 1001 kafka && \
    mkdir -p /opt/kafka /var/lib/kafka/data /var/log/kafka && \
    chown -R kafka:kafka /opt/kafka /var/lib/kafka /var/log/kafka

WORKDIR /opt

# Download precompiled Kafka binaries
RUN wget -q https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION}/* /opt/kafka/ && \
    rm -rf kafka_${SCALA_VERSION}-${KAFKA_VERSION}*

# Set up Kafka environment
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

# Create config directory for KRaft
RUN mkdir -p /opt/kafka/config/kraft

# Set working directory
WORKDIR /opt/kafka

# Switch to kafka user
USER kafka

# Default command (override with docker run arguments)
CMD ["/bin/bash"]

# Expose ports
# 9092: Kafka broker
# 9093: Kafka controller (KRaft)
# 8080: Spring Boot app (CloudEvents bridge)
EXPOSE 9092 9093 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD nc -z localhost 9092 || exit 1
