//! Startup and listener assembly; owner cleanup remains shared across startup modes.
use super::{Owners, Shared, SlotNodeConfig, SlotNodeOwner, SlotStateMachine};
use crate::limits::Budget;
use crate::runtime_error::{native, startup_failure};
use crate::{RuntimeError, SlotManifest, SlotStartupDisposition, StrictSlotStartup};
use multiraft_net::{
    application_rpc::{ApplicationRpcConfig, ApplicationRpcOwner},
    GroupConfig, NodeOwner, RuntimeConfig, StartupBatch, StartupGroup, StartupReport,
    StateMachineFactory,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinSet, time::Instant};

/// Bound for confirming native release after a failed startup.
const CLEANUP: Duration = Duration::from_secs(60);

impl<S: SlotStateMachine> SlotNodeOwner<S> {
    /// Verify or create the slot manifest, start the native node, bring up all
    /// local voter slots with one native `start_groups_with_preference` batch
    /// (see [`crate::SlotStartupPlan`]), then start the application RPC listener.
    ///
    /// Returns after local recovery of every voter slot, which is neither
    /// quorum nor leader readiness. The batch's per-group budget is the plan's
    /// `recovery_timeout` clamped to the time left before `deadline`, counted
    /// from each group's native registration.
    ///
    /// # Errors
    ///
    /// Any group failing or timing out fails the whole start with
    /// [`RuntimeError::StartupFailed`] carrying the native cause; everything
    /// already started (groups, listener, database) is released first. No
    /// retry or fallback beyond the native grace is attempted.
    pub async fn start(
        config: SlotNodeConfig,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        Self::start_inner(config, None, factory, deadline).await
    }

    /// Start with explicit creation/recovery authority and a separately versioned peer directory.
    ///
    /// Candidate startup owns both listeners, creates no Groups and refuses business calls.
    /// Genesis is a one-time empty-directory operation. Existing source identities can only
    /// recover; interrupted genesis and old manifest formats require explicit operator recovery.
    /// The caller must authenticate the directory and authorize the data-space creation fact.
    ///
    /// # Errors
    /// Rejects incompatible identities, incomplete creation facts and missing recovery state.
    /// Failed startup retains normal cancellation-independent native/listener cleanup.
    pub async fn start_strict(
        config: SlotNodeConfig,
        startup: StrictSlotStartup,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        Self::start_inner(config, Some(startup), factory, deadline).await
    }

    async fn start_inner(
        config: SlotNodeConfig,
        strict: Option<StrictSlotStartup>,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        if let Some(startup) = &strict {
            startup.validate(&config.layout, config.node_id)?;
            config.validate_strict(&startup.directory.nodes)?;
        } else {
            config.validate()?;
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline);
        }
        let layout = if let Some(startup) = &strict {
            crate::strict_manifest::prepare(
                &config.data_dir,
                &config.layout,
                config.node_id,
                startup,
            )?;
            config.layout.clone()
        } else {
            SlotManifest::open_or_create(&config.data_dir, &config.layout, config.node_id)
                .map_err(|_| RuntimeError::ManifestMismatch)?
                .layout()
                .clone()
        };
        let candidate = strict
            .as_ref()
            .is_some_and(|s| s.disposition == SlotStartupDisposition::Candidate);
        let nodes = strict
            .as_ref()
            .map_or_else(|| layout.nodes().to_vec(), |s| s.directory.nodes.clone());
        let transport_addresses = strict.as_ref().map(|startup| {
            // Validation guaranteed coverage and nonzero binds before creating the manifest.
            let advertised = nodes
                .iter()
                .find(|n| n.node_id == config.node_id)
                .expect("strict validation checked local directory coverage")
                .clone();
            crate::SlotTransportAddresses {
                raft_bind: config
                    .transport
                    .as_ref()
                    .map_or(advertised.raft, |t| t.raft_bind),
                application_rpc_bind: config
                    .transport
                    .as_ref()
                    .map_or(advertised.application_rpc, |t| t.application_rpc_bind),
                advertised,
                directory_version: startup.directory.version,
            }
        });
        let mut groups = Vec::new();
        for assignment in layout
            .config()
            .slots
            .iter()
            .filter(|a| !candidate && a.voters.contains(&config.node_id))
        {
            let group = GroupConfig {
                group_id: layout
                    .group_for_slot(assignment.slot)
                    .map_err(|_| RuntimeError::InvalidConfig)?,
                voters: assignment.voters.clone(),
            };
            groups.push(StartupGroup {
                group,
                preferred_initializer: Some(assignment.preferred_initializer),
            });
        }
        let mut cluster = config.native;
        cluster.peers = nodes.iter().map(|n| (n.node_id, n.raft)).collect();
        cluster.transport = config.transport.as_ref().map(|t| t.native());
        // Source the native compatibility domain from the disk-verified layout.
        cluster.raft_transport_fingerprint = Some(layout.digest().0);
        // Groups are declared by the batch below, never by the owner itself.
        let mut runtime = RuntimeConfig::new(cluster, Vec::new());
        // The Slot budget is the public limit, native admission remains independently bounded.
        runtime.max_inflight = config.limits.global_requests;
        let native_owner = NodeOwner::start(runtime, factory, deadline)
            .await
            .map_err(|e| native(e, false))?;
        let startup = if groups.is_empty() {
            // A router-only node hosts no voter slot and has nothing to recover.
            StartupReport {
                input_digest: Some(layout.digest().0),
                node_id: config.node_id,
                configuration_verified: false,
                groups: Vec::new(),
            }
        } else if strict
            .as_ref()
            .is_some_and(|s| s.disposition == SlotStartupDisposition::RecoverExisting)
        {
            match super::recovery::recover_sources(
                &native_owner.handle(),
                &layout,
                groups,
                config.startup.recovery_timeout,
                deadline,
            )
            .await
            {
                Ok(report) => report,
                Err(error) => {
                    let _ = native_owner.shutdown(Instant::now() + CLEANUP).await;
                    return Err(error);
                }
            }
        } else {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let recovery_timeout = config.startup.recovery_timeout.min(remaining);
            if recovery_timeout.is_zero() {
                let _ = native_owner.shutdown(Instant::now() + CLEANUP).await;
                return Err(RuntimeError::Deadline);
            }
            let batch = StartupBatch {
                // Opaque native correlation; the layout digest identifies the cohort input.
                input_digest: Some(layout.digest().0),
                groups,
                grace: config.startup.bootstrap_grace,
                recovery_timeout,
            };
            // On failure the native batch already fenced and released the owner;
            // shutting it down here only confirms that release before returning.
            match native_owner
                .handle()
                .start_groups_with_preference(batch)
                .await
            {
                Ok(report) => report,
                Err(failure) => {
                    let error = startup_failure(&layout, &failure);
                    let cleanup = native_owner.shutdown(Instant::now() + CLEANUP).await;
                    tracing::warn!(target: "multiraft_slot::startup", node_id = config.node_id,
                        error = %failure, cleanup_confirmed = cleanup.is_ok(),
                        "slot native preference startup failed; owner released");
                    return Err(error);
                }
            }
        };
        let shared = Arc::new(Shared {
            native: native_owner.handle(),
            budget: Budget::new(config.limits, layout.slot_count()),
            layout: layout.clone(),
            node_id: config.node_id,
            rpc: Mutex::new(None),
            owners: Mutex::new(None),
            accepting: AtomicBool::new(true),
            business_enabled: !candidate,
            transport_addresses,
            cleaning: AtomicBool::new(false),
            tasks: Mutex::new(JoinSet::new()),
            done: watch::channel(None).0,
            unknown: AtomicU64::new(0),
            panicked: AtomicBool::new(false),
            runtime: tokio::runtime::Handle::current(),
        });
        let address = nodes
            .iter()
            .find(|n| n.node_id == config.node_id)
            .ok_or(RuntimeError::InvalidConfig)?
            .application_rpc;
        let peers = nodes
            .iter()
            .filter(|n| n.node_id != config.node_id)
            .map(|n| (n.node_id, n.application_rpc))
            .collect();
        let mut rpc_config = ApplicationRpcConfig::new(config.node_id, address, peers);
        if let Some(transport) = config.transport {
            rpc_config.listen_address = transport.application_rpc_bind;
            rpc_config.dial_addresses = Some(
                transport
                    .nodes
                    .into_iter()
                    .filter(|n| n.node_id != config.node_id)
                    .map(|n| (n.node_id, n.application_rpc))
                    .collect(),
            );
        }
        let result = ApplicationRpcOwner::start(
            rpc_config,
            Arc::new(crate::rpc::Handler {
                shared: Arc::downgrade(&shared),
            }),
            deadline,
        )
        .await;
        let (rpc_owner, rpc_handle) = match result {
            Ok(rpc) => rpc,
            Err(_) => {
                let _ = native_owner.shutdown(Instant::now() + CLEANUP).await;
                return Err(RuntimeError::Unavailable);
            }
        };
        *shared.rpc.lock().unwrap() = Some(rpc_handle);
        *shared.owners.lock().unwrap() = Some(Owners {
            native: native_owner,
            rpc: rpc_owner,
        });
        let owner = Self { shared, startup };
        if let Some(strict) = &strict {
            if let Err(error) =
                crate::strict_manifest::complete(&config.data_dir, &layout, config.node_id, strict)
            {
                let _ = owner.shutdown(Instant::now() + CLEANUP).await;
                return Err(error);
            }
        }
        Ok(owner)
    }
}
