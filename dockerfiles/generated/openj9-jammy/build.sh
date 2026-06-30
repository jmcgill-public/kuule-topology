#!/bin/bash
# Build script for openj9-jammy

set -e

IMAGE_NAME="kafka-openj9-jammy:3.7.0"

echo "🐳 Building $IMAGE_NAME..."
podman build -t "$IMAGE_NAME" .

echo "✅ Built: $IMAGE_NAME"
podman images "$IMAGE_NAME" --format "Size: {{.Size}}"
