#!/bin/bash
# Build script for arch-openj9

set -e

IMAGE_NAME="kafka-arch-openj9:3.7.0"

echo "🐳 Building $IMAGE_NAME..."
podman build -t "$IMAGE_NAME" .

echo "✅ Built: $IMAGE_NAME"
podman images "$IMAGE_NAME" --format "Size: {{.Size}}"
