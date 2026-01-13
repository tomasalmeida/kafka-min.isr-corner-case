# Kafka acks=1 with RF=1: Unavailability Demo

This repo explains and demonstrates why a topic (or partition) can become unavailable under clean leader elections when data is written with acks=1.

Key idea:
- With `acks=1`, the leader acknowledges as soon as it writes locally. If ISR=1 (no other replica in sync) and the leader stops, there is no eligible in-sync replica to elect as the new leader under clean elections, so the partition becomes offline. Producers and consumers then experience timeouts. When the original leader returns, availability is restored and previously acknowledged records typically remain.

## Prerequisites
- Docker and Docker Compose installed
- macOS or Linux

## Quick start

```bash
cd kafka-acks-1-corner-case
chmod +x scripts/run_demo.sh
./scripts/run_demo.sh
```

The script is interactive and will pause at key steps. To run non-interactively, set an environment variable to skip pauses:

```bash
DEMO_PAUSE=0 ./scripts/run_demo.sh
```

## What the demo does (clean election)
- Starts ZooKeeper and 3 Kafka brokers
- Creates the `acks1-loss` topic (RF=3, P=1) and sets `min.insync.replicas=2`
- Stops `kafka-2` and `kafka-3` to force ISR=1
- Produces `LOSS-CANDIDATE-1` with `acks=1` to the leader
- Stops `kafka-1` (the leader), making the partition unavailable (no in-sync replica to elect)
- Starts `kafka-2` (and `kafka-3`) and attempts to consume — this shows the partition offline under clean election
- Attempts to produce `LOSS-CANDIDATE-2` to `kafka-2` — expected to fail without a leader
- Restarts `kafka-1` and consumes from the beginning — the previously acknowledged message typically reappears

Expected results (clean election)
- `LOSS-CANDIDATE-1`: Present after `kafka-1` is restored
- `LOSS-CANDIDATE-2`: Not present; produces to `kafka-2` fail without a leader
- While the leader is down and ISR is empty, the partition is unavailable

### Optional: Data loss variant (unclean leader election)
If you enable unclean leader elections, an out-of-sync replica may be elected leader. In that case, the previously acknowledged message may be missing (data loss):

1. Edit `docker-compose.yml` and add to all brokers:
   - `KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "true"`
2. Recreate the cluster (removes previous data):
```bash
docker compose down -v
docker compose up -d
```
3. Run the same script:
```bash
./scripts/run_demo.sh
```

## Fast-fail kcat tips
When the topic/partition is unavailable, kcat may wait on metadata and broker request timeouts. You can reduce wait times by setting lower timeouts:

Consumer example (faster fail on unavailable leader):
```bash
docker compose run --no-deps --rm --entrypoint sh kcat -c \
  "kcat -C -b kafka-2:9092 -t acks1-loss -o beginning -e -q -c 100 -X metadata.request.timeout.ms=3000 -X request.timeout.ms=3000 -X socket.timeout.ms=3000 -X fetch.wait.max.ms=500"
```

Producer example (fail quickly if delivery isn’t possible):
```bash
MSG="LOSS-CANDIDATE-2"
docker compose run --no-deps --rm --entrypoint sh kcat -c \
  "printf '%s\n' \"$MSG\" | kcat -P -b kafka-2:9092 -t acks1-loss -X request.required.acks=1 -X message.timeout.ms=5000 -X retries=0 -X request.timeout.ms=3000 -X socket.timeout.ms=3000"
```

Notes on options:
- `metadata.request.timeout.ms`: Limit time waiting for cluster metadata
- `request.timeout.ms`: Lower per-request timeout to brokers
- `socket.timeout.ms`: Reduce broker socket operation timeout
- `fetch.wait.max.ms`: Limit consumer fetch wait
- `message.timeout.ms` and `retries=0` (producer): Fail fast when delivery is not possible

## Cleanup
```bash
docker compose down -v
```

## Disclaimer
This project is a demo for educational purposes only. It is not guaranteed or warranted, is not affiliated with nor part of any company’s codebase, and is provided as-is without warranties. Use at your own risk. Do not use this in production.
