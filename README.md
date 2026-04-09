# Kafka Acknowledged-But-Not-Replicated Corner Case

This repository demonstrates how acknowledged messages can leave a partition unavailable under clean leader elections, using two different producer configurations that both exhibit the same failure pattern.

## The Corner Case

When a message is acknowledged by the leader but not replicated to other brokers, and the leader then fails, the partition becomes unavailable under clean elections.

This repository demonstrates **two scenarios** where this occurs:

### Demo 1: acks=all with min.isr=1
- Producer uses `acks=all` (strongest durability setting)
- Topic configured with `min.isr=1`
- When ISR shrinks to 1 (only leader), `acks=all` is satisfied by just the leader
- If leader fails, partition goes offline (no in-sync replica to elect)

### Demo 2: acks=1 with min.isr=1
- Producer uses `acks=1` (only leader acknowledgment required)
- Topic configured with `min.isr=1` (same as Demo 1)
- When ISR shrinks to 1 (only leader), `acks=1` gets acknowledgment from leader
- If leader fails, partition goes offline (same result as Demo 1)

## Configuration Comparison

| Aspect | Demo 1 | Demo 2 |
|--------|--------|--------|
| acks | all (-1) | 1 |
| min.isr | 1 | 1 |
| RF | 3 | 3 |
| ISR when producing | 1 | 1 |
| Why production succeeds | acks=all satisfied by ISR=1 | acks=1 requires only leader |
| Result when leader fails | Partition offline | Partition offline |

## Key Insight

Both demos prove the same point: **with min.isr=1, both acks=all and acks=1 fail identically**.

- **Demo 1**: `acks=all` sounds safe, but with `min.isr=1`, it only guarantees one copy
- **Demo 2**: `acks=1` with `min.isr=1` has the same result — message on one broker only

The comparison shows that **the acks setting doesn't matter when min.isr is too low**. In both cases:
1. Producer receives acknowledgment
2. Message exists only on the leader
3. Leader fails
4. No in-sync replica available for clean election
5. Partition becomes unavailable
6. Availability restored when original leader returns

## Prerequisites
- Docker and Docker Compose installed
- macOS or Linux

## Quick Start

```bash
cd kafka-acks-1-corner-case
chmod +x scripts/run_demo.sh
./scripts/run_demo.sh
```

The script is interactive and will pause at key steps. To run non-interactively, set an environment variable to skip pauses:

```bash
DEMO_PAUSE=0 ./scripts/run_demo.sh
```

## What Each Demo Does

### Demo 1: acks=all with min.isr=1 (Clean Election)
- Starts ZooKeeper and 3 Kafka brokers
- Creates `corner-case-acks-all` topic (RF=3, P=1) with `min.insync.replicas=1`
- Stops `kafka-2` and `kafka-3` to force ISR=1
- Produces `DEMO1-ACKS-ALL-MESSAGE` with `acks=all` to the leader
- Stops `kafka-1` (the leader), making the partition unavailable (no in-sync replica to elect)
- Starts `kafka-2` and `kafka-3` — partition remains offline under clean election
- Restarts `kafka-1` and consumes from the beginning — the previously acknowledged message reappears

**Expected results:**
- `DEMO1-ACKS-ALL-MESSAGE`: Present after `kafka-1` is restored
- While the leader is down and ISR is empty, the partition is unavailable

### Demo 2: acks=1 with min.isr=1 (Clean Election)
- Creates `corner-case-acks-1` topic (RF=3, P=1) with `min.insync.replicas=1`
- Stops `kafka-2` and `kafka-3` to force ISR=1
- Produces `DEMO2-ACKS-1-MESSAGE` with `acks=1` to the leader
- Stops `kafka-1` (the leader), making the partition unavailable
- Starts `kafka-2` and `kafka-3` — partition remains offline (identical to Demo 1)
- Restarts `kafka-1` and consumes from the beginning — the previously acknowledged message reappears

**Expected results:**
- `DEMO2-ACKS-1-MESSAGE`: Present after `kafka-1` is restored
- Same unavailability pattern as Demo 1

## Prevention

To avoid this corner case in production:

1. **Use acks=all AND min.isr ≥ 2** (both settings required!)
2. **Monitor ISR health**: Alert when ISR drops below replication factor
3. **Consider higher min.isr**: For critical data, use `min.isr=2` or higher
4. **Proper RF sizing**: RF=3 minimum for production topics
5. **Block on under-replicated**: Consider blocking producers when ISR is too low

## Optional: Data Loss Variant (Unclean Leader Election)
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

## Fast-Fail kcat Tips
When the topic/partition is unavailable, kcat may wait on metadata and broker request timeouts. You can reduce wait times by setting lower timeouts:

Consumer example (faster fail on unavailable leader):
```bash
docker compose run --no-deps --rm --entrypoint sh kcat -c \
  "kcat -C -b kafka-2:9092 -t corner-case-acks-all -o beginning -e -q -c 100 -X metadata.request.timeout.ms=3000 -X request.timeout.ms=3000 -X socket.timeout.ms=3000 -X fetch.wait.max.ms=500"
```

Producer example (fail quickly if delivery isn't possible):
```bash
MSG="TEST-MESSAGE"
docker compose run --no-deps --rm --entrypoint sh kcat -c \
  "printf '%s\n' \"$MSG\" | kcat -P -b kafka-2:9092 -t corner-case-acks-all -X request.required.acks=1 -X message.timeout.ms=5000 -X retries=0 -X request.timeout.ms=3000 -X socket.timeout.ms=3000"
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
This project is a demo for educational purposes only. It is not guaranteed or warranted, is not affiliated with nor part of any company's codebase, and is provided as-is without warranties. Use at your own risk. Do not use this in production.
