#!/usr/bin/env bash
set -euo pipefail

TOPIC="acks1-loss"
PARTITIONS=1
REPLICATION=3

msg() { echo "[demo] $*"; }

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

produce_additional_on_k2_k3() {
    local p2="LOSS-CANDIDATE-2"
    msg "Attempting to produce two more messages on kafka-2 (expected to fail under clean election)"
    docker compose run --no-deps --rm --entrypoint sh kcat -c \
  "printf '%s\n' '$p2' | kcat -P -b kafka-2:9092 -t $TOPIC -X request.required.acks=1 -X message.timeout.ms=5000 -X retries=0 -X request.timeout.ms=3000 -X socket.timeout.ms=3000" || echo "Failed to produce LOSS-CANDIDATE-2"
}

create_topic() {
  msg "Creating topic $TOPIC (rf=$REPLICATION, p=$PARTITIONS)"
  docker compose exec -T kafka-1 bash -lc \
    "kafka-topics --bootstrap-server kafka-1:9092 --create --if-not-exists --topic $TOPIC --replication-factor $REPLICATION --partitions $PARTITIONS"
  docker compose exec -T kafka-1 bash -lc \
    "kafka-configs --bootstrap-server kafka-1:9092 --alter --entity-type topics --entity-name $TOPIC --add-config min.insync.replicas=2" || true
}

describe_topic() {
  local broker="${1:-kafka-1}"
  docker compose exec -T "$broker" bash -lc \
    "kafka-topics --bootstrap-server $broker:9092 --describe --topic $TOPIC"
}

stop_followers() {
  msg "Stopping followers kafka-2 and kafka-3 to force ISR=1"
  docker compose stop kafka-2 kafka-3
  
  msg "Topic state after stopping followers:"
  describe_topic kafka-1
}

produce_with_acks1() {
  local payload="$1"
  msg "Producing one message with acks=1 to current leader (payload: $payload)"
  docker compose run --rm --entrypoint sh kcat -c \
    "printf '%s\n' '$payload' | kcat -P -b kafka-1:9092 -t $TOPIC -X request.required.acks=1 -X message.timeout.ms=5000 -X retries=0 -X request.timeout.ms=3000 -X socket.timeout.ms=3000"
  
}

kill_leader() {
  msg "Stopping current leader kafka-1"
  docker compose stop kafka-1
}

start_follower_and_observe_offline() {
  msg "Starting kafka-2 and kafka-3 (clean election: partition should remain offline)"
  docker compose start kafka-2 kafka-3
  sleep 5
  msg "Topic state after starting kafka-2 and kafka-3:"
  docker compose exec -T kafka-2 bash -lc \
    "kafka-topics --bootstrap-server kafka-2:9092 --describe --topic $TOPIC" || true
  msg "Attempting to consume from kafka-2 (expected unavailable)"
  docker compose run --no-deps --rm --entrypoint sh kcat -c \
    "kcat -C -b kafka-2:9092 -t $TOPIC -o beginning -e -q -c 10 -X request.timeout.ms=3000 -X socket.timeout.ms=3000 -X fetch.wait.max.ms=500" || echo "Fail to consume as expected (partition offline)"
}

main() {
  ensure_compose
  msg "Bringing up cluster"
  docker compose up -d
  pause "[demo] Cluster is up. Press Enter to wait for brokers..."
  msg "Waiting for brokers to be ready"
  wait_for_broker kafka-1
  wait_for_broker kafka-2
  wait_for_broker kafka-3

  create_topic
  msg "Initial topic state:"
  describe_topic kafka-1
  pause "[demo] Topic created. Press Enter to stop followers (force ISR=1)..."

  stop_followers
  pause "[demo] Followers stopped (ISR=1). Press Enter to produce with acks=1 and stop leader..."

  # Produce a uniquely identifiable message and immediately kill the leader
  PAYLOAD="LOSS-CANDIDATE-1"
  produce_with_acks1 "$PAYLOAD"
  pause "[demo] Message produced to leader. Press Enter to stop the leader broker..."
    
  kill_leader
  pause "[demo] Leader stopped. Press Enter to start followers and observe offline partition..."

  start_follower_and_observe_offline
  pause "[demo] kafka-2 and kafka-3 started. Press Enter to check topic state..."

  describe_topic kafka-2
  pause "[demo] Topic described. Press Enter to attempt producing additional messages on kafka-2 or kafka-3..."

  produce_additional_on_k2_k3
  pause "[demo] Press Enter to restore original leader..."

  # Restore original leader before checking availability
  docker compose start kafka-1
  sleep 5

  msg "Checking availability after restoring the original leader"
  OUT=$(docker compose run --no-deps --rm --entrypoint sh kcat -c \
    "kcat -C -b kafka-1:9092,kafka-2:9092,kafka-3:9092 -t $TOPIC -o beginning -e -q -c 100" || true)
  echo "$OUT"

  if echo "$OUT" | grep -q "LOSS-CANDIDATE-1"; then
    msg "LOSS-CANDIDATE-1 FOUND after leader restore."
  else
    msg "LOSS-CANDIDATE-1 NOT FOUND (expected under clean election when producing to follower without leader)."
  fi

  if echo "$OUT" | grep -q "LOSS-CANDIDATE-2"; then
    msg "LOSS-CANDIDATE-2 FOUND after leader restore."
  else
    msg "LOSS-CANDIDATE-2 NOT FOUND (expected under clean election when producing to follower without leader)."
  fi

  msg "Cleanup tip: docker compose down -v (removes data)"
}

main "$@"
