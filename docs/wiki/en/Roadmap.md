# Roadmap

**中文：** [zh/Roadmap.md](../zh/Roadmap.md)

## Phase-1 (this repo — done)

- [x] Thin Multi-Raft on openraft + openraft-multi
- [x] File persistence + restart recovery
- [x] Multi-process gRPC demo (≥10 groups)
- [x] `acceptance.sh` / `chaos.sh` / porcupine
- [x] Local Jepsen (counter + kill nemesis)
- [x] Consistency Contract + `read_linearizable`

## Phase-1.5 / library hardening

- [x] Learner standby, membership, and lab catalog/checksum/ad generation — see [historical spec](../../specs/2026-07-20-standby-async-snapshot-design.md)
- [ ] Live Standby restore P0/P2: contained; any future protocol needs independently Accepted complete metadata/Vote/membership, atomic capture, full `LogId`, and `install_full_snapshot` — [historical parity spec](../../specs/2026-07-20-aeron-standby-parity-design.md)
- [x] Aeron Standby premium parity **P1**: `promote_standby` / `demote_to_standby` transition
- [ ] Live daisy/catalog/HTTP restore: contained pending the independently Accepted complete protocol
- [x] Aeron Standby premium parity **P3**: `read_stale` / Standby service offload
- [x] Standby chaos (C40–C44) + Jepsen with optional `STANDBY=1`

## Phase-2 (downstream app)

- [ ] Optional Leader RMQ consume → `propose`
- [ ] Pluggable matching engine FSM + idempotency keys
- [ ] Production metrics (propose latency, lag, leadership changes)
- [ ] Durable snapshot/restart P0 remains open; Disabled plus normal OpenRaft recovery is the v1 contract

## Explicit non-goals (near term)

- Region split/merge, PD, dynamic membership
- Replacing RMQ sequencing with Raft (path A)
- Follower LeaseRead as the default production read
