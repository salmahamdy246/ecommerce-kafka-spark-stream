#!/bin/bash

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$BASE_DIR/scripts/pids"

if [ -f "$PID_DIR/producer.pid" ]; then
    P_PID=$(cat "$PID_DIR/producer.pid")
    kill -15 "$P_PID" 2>/dev/null && rm -f "$PID_DIR/producer.pid"
fi

if [ -f "$PID_DIR/spark.pid" ]; then
    S_PID=$(cat "$PID_DIR/spark.pid")
    kill -15 "$S_PID" 2>/dev/null && rm -f "$PID_DIR/spark.pid"
fi
