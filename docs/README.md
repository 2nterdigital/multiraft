# Documentation index

**中文：** [README.zh-CN.md](./README.zh-CN.md)

Self-contained docs for the **multiraft** GitHub repository (no links outside this tree required to navigate). Every doc below has an English + Chinese (`.zh-CN.md`) pair.

> **2026-08-22 containment:** historical Standby P0/P2 live-restore material is retained as history. v1 supports Disabled plus normal OpenRaft recovery, learner, and membership behavior; `STANDBY=1` catalog/checksum/ad output is lab-only and is not a restore provider.

## Start here

| Doc (EN) | 中文 | Audience |
|----------|------|----------|
| [spotlight/2026-07-hotpath-sync1.md](./spotlight/2026-07-hotpath-sync1.md) | [spotlight/2026-07-hotpath-sync1.zh-CN.md](./spotlight/2026-07-hotpath-sync1.zh-CN.md) | Technical highlights — hot path and sync=1 |
| [wiki/en/Home.md](./wiki/en/Home.md) | [wiki/zh/Home.md](./wiki/zh/Home.md) | Wiki (getting started / FAQ / roadmap) |
| [../README.md](../README.md) | [../README.zh-CN.md](../README.zh-CN.md) | Repository README |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | [ARCHITECTURE.zh-CN.md](./ARCHITECTURE.zh-CN.md) | Contributors — crate boundaries & contract |
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | Consistency Contract, porcupine, Jepsen |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | Chaos / failover coverage |
| [upstream.md](./upstream.md) | [upstream.zh-CN.md](./upstream.zh-CN.md) | openraft pin & upgrade notes |
| [slot-acceptance.md](./slot-acceptance.md) | [slot-acceptance.zh-CN.md](./slot-acceptance.zh-CN.md) | Local three-process RF3 fixed-Slot acceptance runner |
| [evidence/2026-10-16-slot-foundation.md](./evidence/2026-10-16-slot-foundation.md) | [evidence/2026-10-16-slot-foundation.zh-CN.md](./evidence/2026-10-16-slot-foundation.zh-CN.md) | First verification evidence, preserved failures and production gates |

## Designs (specs)

| Spec (EN) | 中文 | Topic |
|-----------|------|-------|
| [2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) | [2026-07-18-multiraft-design.zh-CN.md](./specs/2026-07-18-multiraft-design.zh-CN.md) | Thin Multi-Raft for matching HA |
| [2026-07-20-standby-async-snapshot-design.md](./specs/2026-07-20-standby-async-snapshot-design.md) | [2026-07-20-standby-async-snapshot-design.zh-CN.md](./specs/2026-07-20-standby-async-snapshot-design.zh-CN.md) | Historical async-snapshot design; live restore contained |
| [2026-07-20-aeron-standby-parity-design.md](./specs/2026-07-20-aeron-standby-parity-design.md) | [2026-07-20-aeron-standby-parity-design.zh-CN.md](./specs/2026-07-20-aeron-standby-parity-design.zh-CN.md) | Historical Standby P0–P3 mapping; live P0/P2 restore contained |
| [2026-07-21-aeron-inspired-hotpath-design.md](./specs/2026-07-21-aeron-inspired-hotpath-design.md) | [2026-07-21-aeron-inspired-hotpath-design.zh-CN.md](./specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) | Hot path + design philosophy (M1–M4) |
| [2026-07-22-sync1-disk-pipeline-merge.md](./specs/2026-07-22-sync1-disk-pipeline-merge.md) | [2026-07-22-sync1-disk-pipeline-merge.zh-CN.md](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) | M4: deep pipeline / sync=1 group-commit / replication batch + knees |
| [2026-07-22-aeron-next-borrow.md](./specs/2026-07-22-aeron-next-borrow.md) | [2026-07-22-aeron-next-borrow.zh-CN.md](./specs/2026-07-22-aeron-next-borrow.zh-CN.md) | Next Aeron borrow backlog (N1–N3) |
| [2026-10-16-slot-multiraft-feasibility.md](./specs/2026-10-16-slot-multiraft-feasibility.md) | [2026-10-16-slot-multiraft-feasibility.zh-CN.md](./specs/2026-10-16-slot-multiraft-feasibility.zh-CN.md) | Slot MultiRaft feasibility and confirmed implementation scope |
| [2026-10-17-slot-admit-first-implementation.md](./plans/2026-10-17-slot-admit-first-implementation.md) | [2026-10-17-slot-admit-first-implementation.zh-CN.md](./plans/2026-10-17-slot-admit-first-implementation.zh-CN.md) | Highest current execution priority: real Admit → persist/delivery → necessary fairness/hot isolation; T0–T7, M1–M3; no automatic deployment |
| [2026-10-16-slot-multiraft-foundation.md](./specs/2026-10-16-slot-multiraft-foundation.md) | [2026-10-16-slot-multiraft-foundation.zh-CN.md](./specs/2026-10-16-slot-multiraft-foundation.zh-CN.md) | Fixed Slot MultiRaft foundation: contracts, routing, recovery and boundaries |
| [2026-10-16-slot-channel-fair-admission.md](./specs/2026-10-16-slot-channel-fair-admission.md) | [2026-10-16-slot-channel-fair-admission.zh-CN.md](./specs/2026-10-16-slot-channel-fair-admission.zh-CN.md) | Bounded Channel fair admission, mixed-load validation and future hot isolation (planned) |
| [2026-10-18-disk-backed-fsm-mode.md](./specs/2026-10-18-disk-backed-fsm-mode.md) | [2026-10-18-disk-backed-fsm-mode.zh-CN.md](./specs/2026-10-18-disk-backed-fsm-mode.zh-CN.md) | Disk-backed FSM mode: per-Slot FSM RocksDB, applied index in the same batch, restart recovery |
| [2026-10-19-disk-fsm-streaming-snapshot.md](./specs/2026-10-19-disk-fsm-streaming-snapshot.md) | [2026-10-19-disk-fsm-streaming-snapshot.zh-CN.md](./specs/2026-10-19-disk-fsm-streaming-snapshot.zh-CN.md) | Disk-mode streaming snapshots: per-Slot checkpoint, chunked gRPC transfer, verified staging, crash-safe swap, compaction; no 64 MiB cap |

## Compare / positioning

| Doc (EN) | 中文 | Topic |
|----------|------|-------|
| [compare/aeron-commercial.md](./compare/aeron-commercial.md) | [compare/aeron-commercial.zh-CN.md](./compare/aeron-commercial.zh-CN.md) | multiraft vs Aeron Cluster / Standby Premium |
| [perf.md](./perf.md) | [perf.zh-CN.md](./perf.zh-CN.md) | Measured TPS ceilings & bench recipes |
| [perf-single-symbol.md](./perf-single-symbol.md) | [perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md) | Single-symbol recommended config & sync=1 knees |
| [perf-multi-group.md](./perf-multi-group.md) | [perf-multi-group.zh-CN.md](./perf-multi-group.zh-CN.md) | Multi-group wall TPS (mem / sync=0 / sync=1) |

## Plans

| Plan (EN) | 中文 | Topic |
|-----------|------|-------|
| [2026-07-18-multiraft.md](./plans/2026-07-18-multiraft.md) | [2026-07-18-multiraft.zh-CN.md](./plans/2026-07-18-multiraft.zh-CN.md) | Phase-1 library + demo |
| [2026-07-18-multiraft-grpc.md](./plans/2026-07-18-multiraft-grpc.md) | [2026-07-18-multiraft-grpc.zh-CN.md](./plans/2026-07-18-multiraft-grpc.zh-CN.md) | Cross-process gRPC (Phase-1.5) |
| [2026-10-16-slot-multiraft-foundation.md](./plans/2026-10-16-slot-multiraft-foundation.md) | [2026-10-16-slot-multiraft-foundation.zh-CN.md](./plans/2026-10-16-slot-multiraft-foundation.zh-CN.md) | Authorized implementation steps and source-bound validation evidence |
| [2026-10-16-slot-channel-fair-admission.md](./plans/2026-10-16-slot-channel-fair-admission.md) | [2026-10-16-slot-channel-fair-admission.zh-CN.md](./plans/2026-10-16-slot-channel-fair-admission.zh-CN.md) | G0–G6/H1 detailed package retained; execution dependencies overridden by Admit-first plan; not implemented or run |

## Operations

| Doc (EN) | 中文 | Topic |
|----------|------|-------|
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | Local Jepsen + porcupine CI gate |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | Kill leader / rolling / double-kill |
| [../jepsen/multiraft/README.md](../jepsen/multiraft/README.md) | [../jepsen/multiraft/README.zh-CN.md](../jepsen/multiraft/README.zh-CN.md) | Jepsen suite layout |
| [../scripts/](../scripts/) | — | `acceptance.sh`, `chaos.sh`, `run_jepsen.sh`, `test_all.sh` |

## Meta

| Doc (EN) | 中文 |
|----------|------|
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | [../CONTRIBUTING.zh-CN.md](../CONTRIBUTING.zh-CN.md) |
| [../SUPPORT.md](../SUPPORT.md) | [../SUPPORT.zh-CN.md](../SUPPORT.zh-CN.md) |
| [../SECURITY.md](../SECURITY.md) | [../SECURITY.zh-CN.md](../SECURITY.zh-CN.md) |
