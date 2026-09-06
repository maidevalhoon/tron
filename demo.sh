#!/bin/bash

# Clean up
rm -rf node1_dir node2_dir
rm -rf .sync_cache
mkdir -p node1_dir node2_dir

# Create initial file
echo "Hello World" > node1_dir/test.txt
cp node1_dir/test.txt node2_dir/test.txt

# Start Node 1
./sync-node -port 7946 -http 8080 -watch ./node1_dir > node1.log 2>&1 &
NODE1_PID=$!

# Give it a second to start
sleep 2

# Start Node 2 and join Node 1
./sync-node -port 7947 -http 8081 -watch ./node2_dir -peers 127.0.0.1:7946 > node2.log 2>&1 &
NODE2_PID=$!

sleep 2

echo "Both nodes started. Editing file on Node 1 (1-byte edit)..."
# 1-byte edit
echo "Hello World!" > node1_dir/test.txt

# Wait for sync to happen
sleep 3

echo "=== Node 1 Logs ==="
cat node1.log
echo ""
echo "=== Node 2 Logs ==="
cat node2.log
echo ""

echo "Killing nodes..."
kill $NODE1_PID $NODE2_PID
