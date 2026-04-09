#!/usr/bin/env bash
set -euo pipefail

TOPIC_DEMO1="corner-case-acks-all"
TOPIC_DEMO2="corner-case-acks-1"
PARTITIONS=1
REPLICATION=3

msg() { echo "[demo] $*"; }

# Banner function for visual separation
banner() {
  echo ""
  echo "========================================"
  echo "$1"
  echo "========================================"
  echo ""
}

# Interactive pause helper. Set DEMO_PAUSE=0 to disable pauses.
pause() {
  if [ "${DEMO_PAUSE:-1}" = "1" ] && [ -t 0 ]; then
    read -r -p "${1:-[demo] Press Enter to continue...}"
  fi
}

ensure_compose() {
  if ! command -v docker &>/dev/null; then
    echo "Docker is required but not found." >&2
    exit 1
  fi
}

wait_for_broker() {
  local broker="$1"
  # Try listing topics to verify broker is up
  for i in {1..60}; do
    if docker compose exec -T "$broker" bash -lc "kafka-topics --bootstrap-server $broker:9092 --list" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Broker $broker did not become ready in time" >&2
  exit 1
}

# Parameterized topic creation
create_topic_with_config() {
  local topic="$1"
  local min_isr="$2"
  msg "Creating topic $topic (rf=$REPLICATION, p=$PARTITIONS, min.isr=$min_isr)"
  docker compose exec -T kafka-1 bash -lc \
    "kafka-topics --bootstrap-server kafka-1:9092 --create --if-not-exists --topic $topic --replication-factor $REPLICATION --partitions $PARTITIONS"
  docker compose exec -T kafka-1 bash -lc \
    "kafka-configs --bootstrap-server kafka-1:9092 --alter --entity-type topics --entity-name $topic --add-config min.insync.replicas=$min_isr"
}

# Produce with acks=all
produce_with_acks_all() {
  local topic="$1"
  local payload="$2"
  msg "Producing message with acks=all to $topic (payload: $payload)"
  docker compose run --rm --entrypoint sh kcat -c \
    "printf '%s\n' '$payload' | kcat -P -b kafka-1:9092 -t $topic -X request.required.acks=-1 -X message.timeout.ms=5000 -X retries=0 -X request.timeout.ms=3000 -X socket.timeout.ms=3000"
}

# Produce with acks=1 (parameterized)
produce_with_acks1_parameterized() {
  local topic="$1"
  local payload="$2"
  msg "Producing message with acks=1 to $topic (payload: $payload)"
  docker compose run --rm --entrypoint sh kcat -c \
    "printf '%s\n' '$payload' | kcat -P -b kafka-1:9092 -t $topic -X request.required.acks=1 -X message.timeout.ms=5000 -X retries=0 -X request.timeout.ms=3000 -X socket.timeout.ms=3000"
}

# Reset cluster between demos
reset_cluster() {
  msg "Resetting cluster: ensuring all brokers are running..."
  docker compose start kafka-1 kafka-2 kafka-3
  wait_for_broker kafka-1
  wait_for_broker kafka-2
  wait_for_broker kafka-3
  sleep 3
  msg "Cluster ready for next demo"
}

# Consume from topic
consume_topic() {
  local topic="$1"
  docker compose run --no-deps --rm --entrypoint sh kcat -c \
    "kcat -C -b kafka-1:9092,kafka-2:9092,kafka-3:9092 -t $topic -o beginning -e -q -c 100" || true
}

describe_topic() {
  local broker="${1:-kafka-1}"
  local topic="${2}"
  docker compose exec -T "$broker" bash -lc \
    "kafka-topics --bootstrap-server $broker:9092 --describe --topic $topic"
}

stop_followers() {
  local topic="$1"
  msg "Stopping followers kafka-2 and kafka-3 to force ISR=1"
  docker compose stop kafka-2 kafka-3
  sleep 3
  msg "Topic state after stopping followers:"
  describe_topic kafka-1 "$topic"
}

kill_leader() {
  msg "Stopping current leader kafka-1"
  docker compose stop kafka-1
}

start_follower_and_observe_offline() {
  local topic="$1"
  msg "Starting kafka-2 and kafka-3 (clean election: partition should remain offline)"
  docker compose start kafka-2 kafka-3
  sleep 5
  msg "Topic state after starting kafka-2 and kafka-3:"
  docker compose exec -T kafka-2 bash -lc \
    "kafka-topics --bootstrap-server kafka-2:9092 --describe --topic $topic" || true
  msg "Attempting to consume from kafka-2 (expected unavailable) - Type Ctrl+C to stop if it hangs:"
  docker compose run --no-deps --rm --entrypoint sh kcat -c \
    "kcat -C -b kafka-2:9092 -t $topic -o beginning -e -q -c 10 -X request.timeout.ms=3000 -X socket.timeout.ms=3000 -X fetch.wait.max.ms=500" || echo "Failed to consume as expected (partition offline)"
  describe_topic kafka-2 "$topic"
}

run_demo_acks_all() {
  banner "DEMO 1: acks=all with min.isr=1"
  msg "Configuration: RF=3, min.isr=1, acks=all"
  msg "This demo shows that acks=all doesn't guarantee durability when min.isr=1"

  create_topic_with_config "$TOPIC_DEMO1" 1
  pause "[demo1] Topic created with min.isr=1. Press Enter to view topic state..."

  msg "Initial topic state:"
  describe_topic kafka-1 "$TOPIC_DEMO1"
  pause "[demo1] Press Enter to stop kafka-2 and kafka-3 (force ISR=1)..."

  stop_followers "$TOPIC_DEMO1"
  pause "[demo1] ISR=1 (only leader). Press Enter to produce with acks=all..."

  PAYLOAD="DEMO1-ACKS-ALL-MESSAGE"
  produce_with_acks_all "$TOPIC_DEMO1" "$PAYLOAD"
  msg "✓ Producer succeeded! Message acknowledged despite only 1 replica in ISR"
  msg "  (acks=all satisfied because ISR=1, so 'all' = just the leader)"

  pause "[demo1] Message produced. Press Enter to stop leader kafka-1..."
  kill_leader

  pause "[demo1] Leader stopped. Press Enter to start kafka-2 and kafka-3..."
  start_follower_and_observe_offline "$TOPIC_DEMO1"

  pause "[demo1] Partition is offline. Press Enter to restore kafka-1..."
  msg "Restoring original leader kafka-1"
  docker compose start kafka-1
  wait_for_broker kafka-1
  sleep 5

  msg "Consuming from $TOPIC_DEMO1:"
  OUT=$(consume_topic "$TOPIC_DEMO1")
  echo "$OUT"

  if echo "$OUT" | grep -q "$PAYLOAD"; then
    msg "✓ $PAYLOAD found after leader restore"
  else
    msg "✗ $PAYLOAD not found"
  fi

  msg ""
  msg "Demo 1 Result: acks=all provided acknowledgment, but partition became unavailable"
  msg "when the only in-sync replica (the leader) failed."
}

run_demo_acks_1() {
  banner "DEMO 2: acks=1 with min.isr=1"
  msg "Configuration: RF=3, min.isr=1, acks=1"
  msg "This demo shows that acks=1 has the same corner case as acks=all when min.isr=1"

  create_topic_with_config "$TOPIC_DEMO2" 1
  pause "[demo2] Topic created with min.isr=1. Press Enter to view topic state..."

  msg "Initial topic state:"
  describe_topic kafka-1 "$TOPIC_DEMO2"
  pause "[demo2] Press Enter to stop kafka-2 and kafka-3 (force ISR=1)..."

  stop_followers "$TOPIC_DEMO2"
  pause "[demo2] ISR=1 (only leader). Press Enter to produce with acks=1..."

  PAYLOAD="DEMO2-ACKS-1-MESSAGE"
  produce_with_acks1_parameterized "$TOPIC_DEMO2" "$PAYLOAD"
  msg "✓ Producer succeeded! Message acknowledged with acks=1"

  pause "[demo2] Message produced. Press Enter to stop leader kafka-1..."
  kill_leader

  pause "[demo2] Leader stopped. Press Enter to start kafka-2 and kafka-3..."
  start_follower_and_observe_offline "$TOPIC_DEMO2"

  pause "[demo2] Partition is offline. Press Enter to restore kafka-1..."
  msg "Restoring original leader kafka-1"
  docker compose start kafka-1
  wait_for_broker kafka-1
  sleep 5

  msg "Consuming from $TOPIC_DEMO2:"
  OUT=$(consume_topic "$TOPIC_DEMO2")
  echo "$OUT"

  if echo "$OUT" | grep -q "$PAYLOAD"; then
    msg "✓ $PAYLOAD found after leader restore"
  else
    msg "✗ $PAYLOAD not found"
  fi

  msg ""
  msg "Demo 2 Result: acks=1 provided acknowledgment, but partition became unavailable"
  msg "when the only in-sync replica (the leader) failed - same as Demo 1."
}

main() {
  ensure_compose
  msg "Kafka Corner Case Demonstration"
  msg "This script runs two demos showing the same availability corner case"
  msg ""

  msg "Bringing up cluster (3 brokers + ZooKeeper)"
  docker compose up -d
  pause "[demo] Cluster starting. Press Enter to wait for brokers..."

  msg "Waiting for brokers to be ready"
  wait_for_broker kafka-1
  wait_for_broker kafka-2
  wait_for_broker kafka-3
  msg "All brokers ready"

  # Run Demo 1: acks=all with min.isr=1
  pause "[demo] Press Enter to start Demo 1 (acks=all with min.isr=1)..."
  run_demo_acks_all

  # Reset cluster between demos
  pause "[demo] Demo 1 complete. Press Enter to reset cluster for Demo 2..."
  reset_cluster

  # Run Demo 2: acks=1 with min.isr=1
  pause "[demo] Press Enter to start Demo 2 (acks=1 with min.isr=1)..."
  run_demo_acks_1

  # Summary
  banner "SUMMARY: Both Demos Show Same Corner Case"
  msg "Both demos use min.isr=1, but different acks settings:"
  msg ""
  msg "DEMO 1 (acks=all, min.isr=1):"
  msg "  → Leader acknowledges as 'all' when ISR=1"
  msg "  → Message not replicated to other brokers"
  msg "  → Leader fails → partition offline"
  msg ""
  msg "DEMO 2 (acks=1, min.isr=1):"
  msg "  → Leader acknowledges with acks=1"
  msg "  → Message not replicated to other brokers"
  msg "  → Leader fails → partition offline"
  msg ""
  msg "Key Insight: With min.isr=1, BOTH acks=all and acks=1 result in"
  msg "acknowledged-but-not-replicated messages that cause partition"
  msg "unavailability under clean elections."
  msg ""
  msg "Prevention: Use acks=all AND min.isr≥2 AND monitor ISR health"
  msg ""
  msg "Cleanup: docker compose down -v"
}

main "$@"
