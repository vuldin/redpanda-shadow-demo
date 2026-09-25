#!/bin/bash

set -e

SRC="-X brokers=redpanda-source:9092 -X admin.hosts=redpanda-source:9644"
SHD="-X brokers=redpanda-shadow:9092 -X admin.hosts=redpanda-shadow:9644"

echo "==================================="
echo "Overlapping-topic experiment"
echo "==================================="
echo "This assumes 'make setup' + 'make setup-reverse' already ran, so both"
echo "demo-shadow-link (dest=shadow) and reverse-shadow-link (dest=source) exist."
echo ""

echo "--- Step 1: create a name-colliding topic natively on BOTH clusters ---"
echo "(simulates two teams independently picking the same topic name)"
rpk topic create clash-orders $SRC --partitions 1 --replicas 1 2>/dev/null || echo "  clash-orders already exists on source"
rpk topic create clash-orders $SHD --partitions 1 --replicas 1 2>/dev/null || echo "  clash-orders already exists on shadow"
echo '{"origin":"source-native"}' | rpk topic produce clash-orders $SRC
echo '{"origin":"shadow-native"}' | rpk topic produce clash-orders $SHD

echo ""
echo "--- Step 2: force-delete both links (fails over existing shadow topics into regular writable topics) ---"
rpk shadow delete demo-shadow-link --force $SHD
rpk shadow delete reverse-shadow-link --force $SRC

echo ""
echo "--- Step 3: recreate BOTH links with wildcard '*' filters (naive 'mirror everything both ways') ---"
rpk shadow create --config-file /config/shadow-link-wildcard.yaml --no-confirm $SHD
rpk shadow create --config-file /config/shadow-link-reverse-wildcard.yaml --no-confirm $SRC

echo ""
echo "Waiting ~25s for sync intervals to run..."
sleep 25

echo ""
echo "--- Step 4: status - every pre-existing topic now exists natively on both sides ---"
echo "Expect: STATE ACTIVE on both links, but 'No topics are being shadowed.'"
echo ""
echo ">>> demo-shadow-link (dest=shadow):"
rpk shadow status demo-shadow-link $SHD
echo ""
echo ">>> reverse-shadow-link (dest=source):"
rpk shadow status reverse-shadow-link $SRC

echo ""
echo "--- Step 5: create a genuinely NEW topic (fresh-alpha) on source only ---"
rpk topic create fresh-alpha $SRC --partitions 1 --replicas 1 2>/dev/null || true
echo '{"id":1}' | rpk topic produce fresh-alpha $SRC
echo "Waiting ~25s for sync intervals..."
sleep 25

echo ""
echo "--- Step 6: status - fresh-alpha should shadow forward, and NOT loop back ---"
echo ""
echo ">>> demo-shadow-link (dest=shadow) - should now show fresh-alpha ACTIVE, lag 0:"
rpk shadow status demo-shadow-link $SHD
echo ""
echo ">>> reverse-shadow-link (dest=source) - should STILL show nothing shadowed (no loop-back):"
rpk shadow status reverse-shadow-link $SRC

echo ""
echo "==================================="
echo "Experiment complete. Findings: see docs/findings.md"
echo "==================================="
