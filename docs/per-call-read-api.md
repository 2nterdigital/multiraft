# Per-call ReadIndex

Ech0's accepted latest-dev decision (dev d72e121, issue164/173) removes framework
shared confirmation from both consumers and library. Each owned runtime read
calls native `ensure_linearizable(ReadPolicy::ReadIndex)` once, then performs its
own local FSM query. There is no sharing mode, barrier,4+1 queue, round registry
or independent framework read task. Native Raft lifecycle remains owned.

One absolute caller deadline covers admission, native confirmation and FSM query;
the convenience facade retains its10-second budget. The synchronous query must
be bounded. Dropping one caller drops only its future; another read has its own
confirmation. Runtime shutdown still fences/drains admission and native shutdown,
joins transport work and waits for actual FSM destruction. Weak handles close.

`ReadObserver` reports this invocation's Admission/ReadIndex/StateMachine stages;
observer panic cannot change results. Native Closed, Storage/Panicked, deadline,
NotLeader/hint and quorum responding voter identities remain typed. Per-peer RPC
causes are not invented. `TryReadError::Application` keeps the caller's own error.
No lease/stale authority substitute, read retry, write redispatch or timing change
is introduced. Former sharing/error vocabulary and tests are historical evidence,
not current capability claims. Source privacy excludes payload from observations.

Independent public consumer tests are `tests/per_call_read.rs`: acknowledged-write
visibility, caller cancellation followed by another read, expired deadline,
majority loss, weak closure, fallible query, observer isolation and actual FSM
release. `tests/proposal_failure.rs` separately covers opaque native write errors.
