# Slot transport overrides

`SlotNodeConfig::transport` defaults to `None`: native Raft and application RPC
continue dialing and binding the numeric addresses from the immutable layout.

Set it to `Some(SlotTransportConfig { nodes, raft_bind, application_rpc_bind })`
to provide an independent transport catalog. Each `SlotTransportNode` contains
`node_id`, `raft: String`, and `application_rpc: String`. The catalog must name
exactly every layout node once, including this process. Entries are host:port,
for example `node-1.namespace.svc.cluster.local:21001` or `[::1]:21001`.
URLs, paths, userinfo, whitespace, zero/out-of-range ports, unspecified dial IPs,
and addresses longer than 260 bytes are rejected. DNS labels are bounded to
63 bytes and names to 253 bytes. Bind addresses remain numeric `SocketAddr`s
and may use wildcard IPs; both bind ports must be nonzero.

Validation is synchronous and does not resolve DNS. Tonic retains the hostname
and resolves it when connecting or reconnecting. Bind addresses never replace
remote dial addresses. There is no partial catalog or per-peer numeric fallback.

These overrides are local deployment settings: the layout, its digest, the Slot
manifest, native membership addresses, and the Raft transport fingerprint remain
unchanged. Invalid catalogs are rejected before creating the Slot manifest,
opening native logs, invoking the FSM factory, or binding a listener. Slot owns
these settings and rejects separately supplied `config.native.transport`.
The native facade separately exposes `ClusterConfig::transport` for non-Slot
consumers; its `RaftTransportConfig` also requires complete peer coverage.
