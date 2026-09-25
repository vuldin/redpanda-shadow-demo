#!/bin/bash

set -e

echo "==================================="
echo "Reverse Shadow Link Setup Script"
echo "(redpanda-source becomes a shadow destination too)"
echo "==================================="

echo "Creating back-events topic on shadow cluster..."
rpk topic create back-events -X brokers=redpanda-shadow:9092 -X admin.hosts=redpanda-shadow:9644 --partitions 2 --replicas 1 2>/dev/null || echo "  back-events already exists"

echo ""
echo "Creating reverse shadow link from source cluster..."
cd /config
if rpk shadow create --config-file shadow-link-reverse.yaml --no-confirm -X brokers=redpanda-source:9092 -X admin.hosts=redpanda-source:9644; then
  echo ""
  echo "Reverse shadow link created successfully!"
else
  echo ""
  echo "Reverse shadow link already exists or creation failed - checking status..."
fi

echo ""
echo "Verifying reverse shadow link status..."
rpk shadow status reverse-shadow-link -X brokers=redpanda-source:9092 -X admin.hosts=redpanda-source:9644

echo ""
echo "==================================="
echo "Both directions now active:"
echo "  demo-shadow-link:    redpanda-source -> redpanda-shadow  (demo-* topics)"
echo "  reverse-shadow-link: redpanda-shadow -> redpanda-source  (back-* topics)"
echo "==================================="
