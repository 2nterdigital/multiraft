//! Existing facade behavior, owned by this feature.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    /// Create (or idempotently ensure) a local Raft group peer.
    ///
    /// **In-process:** when every `members` node has created the group, membership
    /// is initialized once (racing callers see `NotAllowed` and ignore).
    ///
    /// **gRPC:** each process spawns the local raft then tries `initialize`;
    /// `NotAllowed` is ignored (no cross-process ClusterGlue).
    ///
    /// **Standby:** local node may be absent from `members` (voters only). Spawns
    /// the local raft without calling `initialize`; join via [`Self::add_standby`].
    pub async fn create_group(&self, group: u64, members: &[u64]) -> Result<(), MultiRaftError> {
        self.create_group_with_policy(group, members, InitializePolicy::Allowed)
            .await
    }

    /// [`Self::create_group`] with an explicit [`InitializePolicy`].
    ///
    /// `Allowed` is exactly [`Self::create_group`]. `Forbidden` spawns the local
    /// raft and never calls `initialize` on either transport; the Group can only
    /// be caught up by an existing leader. When the local storage is pristine
    /// (voter role), the rejoin guard is armed before the Group is published: it
    /// drops inbound vote requests and disables local elections until applied
    /// covers the per-term anchor (see [`crate::RejoinGuard`]); a Group-owned
    /// watcher releases it as soon as that holds. Await
    /// [`Self::wait_for_recovery`] before serving: on an empty log it completes
    /// only after the first observed leader commit is applied.
    /// An already-registered Group is left unchanged (its guard is not re-armed).
    pub async fn create_group_with_policy(
        &self,
        group: u64,
        members: &[u64],
        policy: InitializePolicy,
    ) -> Result<(), MultiRaftError> {
        if members.is_empty() {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "create_group requires at least one member"
            )));
        }

        let is_standby = self.config.role == NodeRole::Standby;
        if !is_standby && !members.contains(&self.node_id) {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "local node {} is not in members {:?}",
                self.node_id,
                members
            )));
        }

        let needs_spawn = !self.groups.lock().unwrap().contains_key(&group);
        if needs_spawn {
            let guard = match policy {
                InitializePolicy::Allowed => crate::RejoinGuard::disarmed(),
                InitializePolicy::Forbidden => {
                    self.ensure_node_log().await?;
                    let provenance = self.local_provenance(group)?;
                    self.rejoin_guard_for(group, policy, provenance)
                }
            };
            self.spawn_local_group(group, guard).await?;
        }

        if is_standby || policy == InitializePolicy::Forbidden {
            // Learner: wait for leader `add_learner`; do not initialize.
            return Ok(());
        }

        match &self.net {
            NetBackend::InProcess { glue, .. } => {
                let all_ready = glue.mark_ready(group, self.node_id, members);
                if all_ready && glue.try_claim_init(group) {
                    self.try_initialize(group, members).await?;
                }
            }
            NetBackend::Grpc { .. } => {
                // Cross-process: every node attempts initialize; loser gets NotAllowed.
                self.try_initialize(group, members).await?;
            }
        }

        Ok(())
    }

    /// Whether the local native Raft of `group` holds any initialized state
    /// (membership or log). A Forbidden Group stays uninitialized until a leader
    /// replicates to it.
    pub async fn is_initialized(&self, group: u64) -> Result<bool, MultiRaftError> {
        self.startup_eligible(group).await.map(|eligible| !eligible)
    }

    /// Leader-only: add a Standby as an openraft Learner (`add_learner`, blocking).
    ///
    /// Retries transient "configuration change in progress" errors until membership
    /// from a prior `initialize` / `change_membership` commits (openraft requirement).
    pub async fn add_standby(&self, group: u64, standby_id: u64) -> Result<(), MultiRaftError> {
        retry_on_membership_pending(|| self.add_standby_once(group, standby_id)).await
    }

    pub(super) async fn add_standby_once(
        &self,
        group: u64,
        standby_id: u64,
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let addr = self
            .config
            .peers
            .iter()
            .find(|(n, _)| *n == standby_id)
            .map(|(_, a)| a.to_string())
            .unwrap_or_default();
        let node = BasicNode { addr };
        match raft.add_learner(standby_id, node, true).await {
            Ok(_) => {
                self.standby_throttle.insert(standby_id);
                Ok(())
            }
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                let msg = e.to_string();
                if msg.contains("configuration change") {
                    return Err(transient_membership_err("add_learner", &e));
                }
                Err(MultiRaftError::Other(anyhow::anyhow!("add_learner: {e}")))
            }
        }
    }
}

impl<S: StateMachine> MultiRaft<S> {
    /// Leader-only: promote a Standby learner to voter (`change_membership` AddVoterIds).
    pub async fn promote_standby(&self, group: u64, node_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let membership = raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .clone();
        let is_learner = membership.learner_ids().any(|id| id == node_id);
        let is_voter = membership.voter_ids().any(|id| id == node_id);
        if is_voter {
            self.standby_throttle.remove(node_id);
            return Ok(());
        }
        if !is_learner {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "promote_standby: node {node_id} is not a learner in group {group}"
            )));
        }
        retry_on_membership_pending(|| self.promote_standby_once(group, node_id)).await
    }

    pub(super) async fn promote_standby_once(
        &self,
        group: u64,
        node_id: u64,
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let mut add = BTreeSet::new();
        add.insert(node_id);
        match raft
            .change_membership(ChangeMembers::AddVoterIds(add), true)
            .await
        {
            Ok(_) => {
                self.standby_throttle.remove(node_id);
                Ok(())
            }
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                let msg = e.to_string();
                if msg.contains("configuration change") {
                    return Err(transient_membership_err(
                        "promote_standby change_membership",
                        &e,
                    ));
                }
                Err(MultiRaftError::Other(anyhow::anyhow!(
                    "promote_standby change_membership: {e}"
                )))
            }
        }
    }

    /// Leader-only: demote a voter to Standby learner (`RemoveVoters`, retain=true).
    pub async fn demote_to_standby(&self, group: u64, node_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let membership = raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .clone();
        let is_voter = membership.voter_ids().any(|id| id == node_id);
        if !is_voter {
            self.standby_throttle.insert(node_id);
            return Ok(());
        }
        retry_on_membership_pending(|| self.demote_to_standby_once(group, node_id)).await
    }

    pub(super) async fn demote_to_standby_once(
        &self,
        group: u64,
        node_id: u64,
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let mut remove = BTreeSet::new();
        remove.insert(node_id);
        match raft
            .change_membership(ChangeMembers::RemoveVoters(remove), true)
            .await
        {
            Ok(_) => {
                self.standby_throttle.insert(node_id);
                Ok(())
            }
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                let msg = e.to_string();
                if msg.contains("configuration change") {
                    return Err(transient_membership_err(
                        "demote_to_standby change_membership",
                        &e,
                    ));
                }
                Err(MultiRaftError::Other(anyhow::anyhow!(
                    "demote_to_standby change_membership: {e}"
                )))
            }
        }
    }

    /// Current voter ids from raft metrics (committed membership view may lag slightly).
    pub fn voter_ids(&self, group: u64) -> Option<BTreeSet<NodeId>> {
        let raft = self.raft(group)?;
        Some(
            raft.metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .voter_ids()
                .collect(),
        )
    }

    /// Current learner (Standby) ids from raft metrics.
    pub fn learner_ids(&self, group: u64) -> Option<BTreeSet<NodeId>> {
        let raft = self.raft(group)?;
        Some(
            raft.metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .learner_ids()
                .collect(),
        )
    }
}
