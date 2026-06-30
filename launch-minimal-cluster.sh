#!/bin/bash
# Launch minimal 2-broker + 1-controller KRaft cluster
# Optimized for drive space

set -e

NETWORK="kuule-net"
IMAGE="kafka-minimal:3.7.0"
CONFIG_DIR="$(pwd)/kraft-configs"

# Ensure network exists
podman network exists "$NETWORK" || podman network create "$NETWORK"

echo "🚀 Launching minimal KRaft cluster..."

# Generate cluster UUID (reuse if exists)
CLUSTER_UUID_FILE="/tmp/kuule-cluster-uuid"
if [ ! -f "$CLUSTER_UUID_FILE" ]; then
    CLUSTER_UUID=$(uuidgen | tr -d '-' | head -c 22)
    echo "$CLUSTER_UUID" > "$CLUSTER_UUID_FILE"
fi
CLUSTER_UUID=$(cat "$CLUSTER_UUID_FILE")
echo "   Cluster UUID: $CLUSTER_UUID"

# Controller (Hiljaisuus)
echo "📍 Starting controller at (0,0,0)..."
podman run -d \
    --name kuule-hex-0-0-0 \
    --network "$NETWORK" \
    -e KAFKA_HEAP_OPTS="-Xms128m -Xmx128m" \
    -v "$CONFIG_DIR/controller.properties:/opt/kafka/config/server.properties:ro" \
    "$IMAGE" \
    /bin/bash -c "
        /opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_UUID -c /opt/kafka/config/server.properties --ignore-formatted && \
        /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
    "

echo "   Waiting for controller..."
sleep 5

# Broker 1 at (1,0,-1)
echo "📍 Starting broker-1 at (1,0,-1)..."
podman run -d \
    --name kuule-hex-1-0-n1 \
    --network "$NETWORK" \
    -e KAFKA_HEAP_OPTS="-Xms128m -Xmx128m" \
    -v "$CONFIG_DIR/broker-1.properties:/opt/kafka/config/server.properties:ro" \
    "$IMAGE" \
    /bin/bash -c "
        /opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_UUID -c /opt/kafka/config/server.properties --ignore-formatted && \
        /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
    "

# Broker 2 at (0,1,-1)
echo "📍 Starting broker-2 at (0,1,-1)..."
podman run -d \
    --name kuule-hex-0-1-n1 \
    --network "$NETWORK" \
    -e KAFKA_HEAP_OPTS="-Xms128m -Xmx128m" \
    -v "$CONFIG_DIR/broker-2.properties:/opt/kafka/config/server.properties:ro" \
    "$IMAGE" \
    /bin/bash -c "
        /opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_UUID -c /opt/kafka/config/server.properties --ignore-formatted && \
        /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
    "

echo ""
echo "✅ Cluster launched"
echo ""
echo "Containers:"
podman ps --filter "name=kuule-hex-*" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "Check logs:"
echo "  podman logs -f kuule-hex-0-0-0    # Controller"
echo "  podman logs -f kuule-hex-1-0-n1   # Broker 1"
echo "  podman logs -f kuule-hex-0-1-n1   # Broker 2"
echo ""
echo "Resource usage:"
echo "  podman stats --no-stream kuule-hex-*"
