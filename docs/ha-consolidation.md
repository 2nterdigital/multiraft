# Owned HA runtime and normative APIs

This integration implements the business-neutral library capabilities required by [Ech0 specification #164](https://github.com/2nterdigital/ech0-delivery/issues/164). The consumer capability inventory and acceptance boundaries are recorded in [the pre-implementation audit](https://github.com/2nterdigital/ech0-delivery/blob/codex/issue-164-ha-consolidation/docs/evidence/issue-164-ha-capability-audit.md).

Library baseline: `fe832257b0c744faf93f9c7a4ab1da63d37d6a46`. Existing proposals with effects, ReadIndex, control sampling/transfer/layout, Group observations and native snapshot maintenance are reused or upgraded. The library owns the listener, workers, observations, shared confirmation, maintenance and cancellation cleanup. Public consumers use a unique Node owner and weak request handles. They do not supplement the runtime with raw Raft, metrics or trigger operations.

The runtime composes the existing facade. Feature files isolate Group construction, membership, application operations and task/lifecycle ownership. Store remains below the runtime and supplies native persistence and opaque FSM resource-release observation. Application byte contents, business schema, service routing and balancing policy remain in the consumer.

Initial state: implementation in progress. Verification will include an independent simple FSM consumer, actual old-source disk/snapshot fixtures, deterministic concurrency/deadline/closure checks and real RF3 control/recovery. This document will be updated with delivered API paths and results. No consensus, election or replication algorithm or timing change is part of this integration.
