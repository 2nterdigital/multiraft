# Owned startup preference implementation contract

Implements [Ech0 specification #193](https://github.com/2nterdigital/ech0-delivery/issues/193), library slice [#194](https://github.com/2nterdigital/ech0-delivery/issues/194), following the reviewed [design](https://github.com/2nterdigital/ech0-delivery/blob/4a1f553a3c3be2b765381cbf3ed197b674f190f6/docs/architecture/startup-campaign-preference.md).

The new capability is opt-in and business neutral. It atomically admits a startup batch only on an empty owner, constructs/registers every local Group before preference/recovery waits, and relies on provider/native provenance for pristine eligibility. Persisted state follows native recovery; unknown namespaces fail closed. No Raft election algorithm or timing input changes.

Each Group deadline begins at its own registration completion. The Ech0 contract uses Cg+10s, charging subsequent constructors, preference grace, initialize and native wait; application validation remains afterward. Explicit 500ms grace is the first candidate. Existing single-Group native-wait and absolute-deadline APIs retain their contracts.

Accepted work and cleanup belong to the existing owner through waiter cancellation, Drop and shutdown. Public outcomes preserve phase, raw initialization disposition, dispatch uncertainty and actual resource release; initialization replies do not prove quorum/commit or later leader causality. Independent public consumers validate failures and cancellation without production test probes.

Formal comparative runs remain separately authorized. This implementation does not change Ech0's default balancing or write retry contracts.
