#!/bin/bash
# Build script for alpine-temurin21

set -e

IMAGE_NAME="kafka-alpine-temurin21:3.7.0"

echo "🐳 Building $IMAGE_NAME..."
podman build -t "$IMAGE_NAME" .

echo "✅ Built: $IMAGE_NAME"
podman images "$IMAGE_NAME" --format "Size: {{.Size}}"
