#!/bin/bash
# Build script for arch-temurin21

set -e

IMAGE_NAME="kafka-arch-temurin21:3.7.0"

echo "🐳 Building $IMAGE_NAME..."
podman build -t "$IMAGE_NAME" .

echo "✅ Built: $IMAGE_NAME"
podman images "$IMAGE_NAME" --format "Size: {{.Size}}"
