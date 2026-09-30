# Pre-migration native disk fixture

Produced before this migration by MultiRaft commit
`fe832257b0c744faf93f9c7a4ab1da63d37d6a46`, using its original Cargo.lock and
OpenRaft `0.10.0-alpha.30`. The lock SHA-256 and every exact on-disk file hash are
in `manifest.json`; `producer.rs` is the producer source, retained for provenance,
not rerun by tests. No new runtime API participated in fixture creation.

Group 7, Node 1, CounterFsm: ten `+1` writes, observed native durable compaction,
then a committed `+5` tail. The native snapshot contains value 10; persisted
snapshot plus log recover value 15. This intentionally exercises both persisted
snapshot restoration and committed tail replay. FileLogSyncLevel::Data;
NativeDurable with retain_log_entries=2. Command, snapshot and store formats are
unchanged by the migration.

Consumers verify each hash, copy only these five data files to disposable
`.tmp/issue-167/consumers/` directories, then recover and mutate the copies via
NodeOwner and RuntimeHandle. The fixture remains unchanged. This is independent
library compatibility evidence; actual Ech0 Store/business recovery is verified
by the parent integration ticket separately.
