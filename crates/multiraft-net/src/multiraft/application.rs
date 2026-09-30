//! Existing facade behavior, owned by this feature.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    /// Propose application bytes via openraft `client_write`.
    ///
    /// On `Ok`, the write is committed by a quorum and applied (linearizable write
    /// for this group). Non-leader → [`MultiRaftError::NotLeader`].
    /// Timeout / disconnect ⇒ outcome **unknown**; retry with the same idempotency key.
    pub async fn propose(&self, group: u64, data: Vec<u8>) -> Result<ProposeOk, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        Self::client_write_one(&raft, data).await
    }

    /// Propose application bytes and return the opaque application response
    /// produced by the exact committed and applied log entry.
    ///
    /// Like [`Self::propose`], timeout or disconnect leaves the write outcome
    /// unknown. Effects are returned only when this invocation receives a
    /// successful committed-and-applied response.
    pub async fn propose_with_effects(
        &self,
        group: u64,
        data: Vec<u8>,
    ) -> Result<ProposeApplied, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        Self::client_write_one_with_effects(&raft, data).await
    }

    /// Pipeline many proposes: **one Raft entry per payload**, concurrent `client_write`.
    ///
    /// Returns `Ok` only if **all** entries succeed. On any failure (including
    /// [`MultiRaftError::NotLeader`]), returns that error — some entries may already
    /// be committed; callers must use idempotency keys.
    ///
    /// Each `client_write` is polled concurrently via `try_join_all` so N quorum
    /// waits overlap (deep pipeline). openraft `api_batch_*` may still merge
    /// consecutive writes into fatter storage appends.
    pub async fn propose_batch(
        &self,
        group: u64,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<ProposeOk>, MultiRaftError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        futures::future::try_join_all(payloads.into_iter().map(|data| {
            let raft = raft.clone();
            async move { Self::client_write_one(&raft, data).await }
        }))
        .await
    }

    /// One Core API message for many payloads (fatter appends; shallower client
    /// pipeline than [`Self::propose_batch`]).
    pub async fn propose_many(
        &self,
        group: u64,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<ProposeOk>, MultiRaftError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let n = payloads.len();
        let mut stream = raft
            .client_write_many(payloads.into_iter().map(Request::new))
            .await
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("client_write_many: {e}")))?;
        let mut out = Vec::with_capacity(n);
        while let Some(item) = futures::StreamExt::next(&mut stream).await {
            let result = item.map_err(|e| {
                MultiRaftError::Other(anyhow::anyhow!("client_write_many stream: {e}"))
            })?;
            match result {
                Ok(resp) => out.push(ProposeOk {
                    index: resp.log_id.index(),
                    term: resp.log_id.committed_leader_id().term,
                }),
                Err(fwd) => {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
            }
        }
        Ok(out)
    }

    pub(super) async fn client_write_one(
        raft: &Raft<S>,
        data: Vec<u8>,
    ) -> Result<ProposeOk, MultiRaftError> {
        Self::client_write_one_with_effects(raft, data)
            .await
            .map(|applied| ProposeOk {
                index: applied.index,
                term: applied.term,
            })
    }

    pub(super) async fn client_write_one_with_effects(
        raft: &Raft<S>,
        data: Vec<u8>,
    ) -> Result<ProposeApplied, MultiRaftError> {
        match raft.client_write(Request::new(data)).await {
            Ok(resp) => Ok(ProposeApplied {
                index: resp.log_id.index(),
                term: resp.log_id.committed_leader_id().term,
                effects: resp.data.effects,
            }),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                Err(MultiRaftError::Other(anyhow::anyhow!("client_write: {e}")))
            }
        }
    }

    /// Linearizable read: confirm leadership (ReadIndex), then read the local FSM.
    ///
    /// Non-leader → [`MultiRaftError::NotLeader`]. Use this for order-status / truth
    /// reads. For Standby offload / debug local reads, use [`Self::read_stale`] or
    /// [`Self::with_fsm`].
    pub async fn read_linearizable<R>(
        &self,
        group: u64,
        f: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;

        match raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            Ok(_read_log_id) => self.with_fsm(group, f).await.ok_or_else(|| {
                MultiRaftError::Other(anyhow::anyhow!(
                    "read_linearizable: fsm missing for group {group}"
                ))
            }),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                Err(MultiRaftError::Other(anyhow::anyhow!(
                    "ensure_linearizable: {e}"
                )))
            }
        }
    }
}

impl<S: StateMachine> MultiRaft<S> {
    /// Wait until the state machine has recovered at least the persisted commit
    /// point after a restart (no-op when the log was empty).
    pub async fn wait_for_recovery(
        &self,
        group: GroupId,
        timeout: Duration,
    ) -> Result<(), MultiRaftError> {
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        tracing::info!(
            target: "multiraft::recovery",
            operation = "recovery_wait",
            phase = "start",
            node_id = self.node_id,
            group_id = group,
            timeout_ms,
            "waiting for Raft state-machine recovery"
        );
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let metrics = match raft.wait_for_recovery(Some(timeout)).await {
            Ok(metrics) => metrics,
            Err(error) => {
                tracing::error!(
                    target: "multiraft::recovery",
                    operation = "recovery_wait",
                    phase = "error",
                    node_id = self.node_id,
                    group_id = group,
                    timeout_ms,
                    error = %error,
                    error_debug = ?error,
                    "Raft state-machine recovery failed"
                );
                return Err(MultiRaftError::Other(anyhow::anyhow!(
                    "wait_for_recovery node {}, group {}: {error}",
                    self.node_id,
                    group
                )));
            }
        };
        let applied_index = metrics.last_applied.as_ref().map(|log_id| log_id.index());
        let applied_term = metrics
            .last_applied
            .as_ref()
            .map(|log_id| log_id.committed_leader_id().term);
        tracing::info!(
            target: "multiraft::recovery",
            operation = "recovery_wait",
            phase = "complete",
            node_id = self.node_id,
            group_id = group,
            timeout_ms,
            applied_index = ?applied_index,
            applied_term = ?applied_term,
            "Raft state-machine recovery completed"
        );
        Ok(())
    }

    /// Inspect the **local** FSM for `group` without leadership confirmation.
    ///
    /// May be stale relative to the cluster. Prefer [`Self::read_linearizable`] for
    /// application truth reads; keep this for tests / metrics / debug.
    /// For Standby service offload with an applied watermark, use [`Self::read_stale`].
    pub async fn with_fsm<R>(&self, group: GroupId, f: impl FnOnce(&S) -> R) -> Option<R> {
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())?;
        Some(sm.with_fsm(f).await)
    }

    /// Local FSM read for Standby (or other) service offload.
    ///
    /// Requires [`ClusterConfig::enable_stale_queries`]. Returns the value plus this
    /// node's last applied `(index, term)`. **Not** linearizable — callers must
    /// treat the result as eventually consistent / possibly behind the leader.
    pub async fn read_stale<R>(
        &self,
        group: GroupId,
        f: impl FnOnce(&S) -> R,
    ) -> Result<StaleRead<R>, MultiRaftError> {
        if !self.config.enable_stale_queries {
            return Err(MultiRaftError::StaleQueriesDisabled);
        }
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let (applied_index, applied_term) = self.local_applied(group).await.unwrap_or((0, 0));
        let value = sm.with_fsm(f).await;
        Ok(StaleRead {
            value,
            applied_index,
            applied_term,
        })
    }

    /// Last applied log id for `group` from the state-machine store.
    ///
    /// Prefer this over Raft metrics so out-of-band
    /// [`Self::install_durable_snapshot`] watermarks stay consistent with FSM data.
    pub async fn local_applied(&self, group: GroupId) -> Option<(u64, u64)> {
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())?;
        sm.last_applied().await
    }

    /// Whether this node accepts [`Self::read_stale`].
    pub fn stale_queries_enabled(&self) -> bool {
        self.config.enable_stale_queries
    }
}
