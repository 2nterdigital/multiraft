//! Retained construction, policy wait, native recovery and atomic publication.
use super::*;
use futures::{stream::FuturesUnordered, StreamExt};
type Progress = Arc<Mutex<StartupReport>>;

impl<S: StateMachine> RuntimeShared<S> {
    pub(super) async fn run_batch(
        &self,
        batch: StartupBatch,
        progress: Progress,
    ) -> Result<StartupReport, StartupFailure> {
        let digest = crate::multiraft::digest_label(batch.input_digest);
        tracing::info!(target: "multiraft::startup", node_id=self.node.node_id(), startup_digest=digest.as_deref(), digest_known=digest.is_some(), configuration_verified=false, phase="batch_admitted", groups=batch.groups.len(), "owned startup batch; consumer digest is opaque");
        // Native registry is the registration boundary. No initialize/native wait
        // is entered until every local Group (including persisted ones) is there.
        for (index, input) in batch.groups.iter().enumerate() {
            self.check_open(&progress, index)?;
            for earlier in 0..index {
                self.check_budget(&progress, earlier)?;
            }
            let (provenance, registered) = self
                .node
                .register_startup_group(input.group.group_id)
                .await
                .map_err(|e| failure(&progress, index, RuntimeError::Source(e)))?;
            update(&progress, index, |group| {
                group.provenance = Some(provenance);
                group.registered_at = Some(registered);
                group.deadline = Some(registered + batch.recovery_timeout);
                group.phase = StartupPhase::Registered;
            });
            tracing::info!(target:"multiraft::startup",node_id=self.node.node_id(),group_id=input.group.group_id,startup_digest=digest.as_deref(),digest_known=digest.is_some(),phase="registered",startup_provenance=provenance.code(),budget_ms=batch.recovery_timeout.as_millis() as u64,"Group registration anchors startup deadline");
            for earlier in 0..=index {
                self.check_budget(&progress, earlier)?;
            }
        }
        let batch_registered = progress
            .lock()
            .unwrap()
            .groups
            .last()
            .unwrap()
            .registered_at
            .unwrap();
        let mut waits = FuturesUnordered::new();
        for (index, input) in batch.groups.iter().enumerate() {
            waits.push(self.recover_startup_group(
                index,
                input,
                batch.grace,
                batch_registered,
                progress.clone(),
            ));
        }
        let mut first = None;
        // Drain the retained children even after a failure. Native dispatch cannot
        // be retracted; all source facts remain attached to their actual Group.
        while let Some(result) = waits.next().await {
            if let Err(error) = result {
                if first.is_none() {
                    first = Some(error);
                }
            }
            if first.is_some() {
                self.accepting.store(false, Ordering::Release);
                self.closed.send_replace(true);
            }
        }
        if let Some(mut error) = first {
            error.report = Box::new(progress.lock().unwrap().clone());
            return Err(error);
        }
        for (index, input) in batch.groups.iter().enumerate() {
            self.check_open(&progress, index)?;
            update(&progress, index, |group| {
                group.phase = StartupPhase::Validate
            });
            // Application validators are deliberately outside the native budget.
            let mut closed = self.closed.subscribe();
            self.check_open(&progress, index)?;
            tokio::select! {
                biased;
                _ = closed.changed() => return Err(failure(&progress, index, RuntimeError::Closed)),
                result = self.node.validate_recovered(input.group.group_id) =>
                    result.map_err(|e| failure(&progress, index, RuntimeError::Source(e)))?,
            }
            self.node
                .ensure_recovery_running(input.group.group_id)
                .map_err(|e| failure(&progress, index, RuntimeError::Source(e)))?;
        }
        self.check_open(&progress, 0)?;
        let mut ready = self.ready.lock().unwrap();
        for (index, input) in batch.groups.iter().enumerate() {
            ready.insert(input.group.group_id);
            update(&progress, index, |group| group.phase = StartupPhase::Ready);
        }
        Ok(progress.lock().unwrap().clone())
    }
    async fn recover_startup_group(
        &self,
        index: usize,
        input: &StartupGroup,
        grace: Duration,
        batch_registered: Instant,
        progress: Progress,
    ) -> Result<(), StartupFailure> {
        self.check_budget(&progress, index)?;
        let pristine =
            progress.lock().unwrap().groups[index].provenance == Some(StartupProvenance::Pristine);
        if pristine && self.node.startup_voter() {
            let preferred = input.preferred_initializer;
            if preferred.is_some_and(|id| id != self.node.node_id()) {
                update(&progress, index, |group| group.phase = StartupPhase::Grace);
                let deadline = progress.lock().unwrap().groups[index].deadline.unwrap();
                let until = (batch_registered + grace).min(deadline);
                loop {
                    self.check_open(&progress, index)?;
                    self.check_budget(&progress, index)?;
                    if !self
                        .bounded(
                            index,
                            &progress,
                            self.node.startup_eligible(input.group.group_id),
                        )
                        .await?
                    {
                        break;
                    }
                    if Instant::now() >= until {
                        update(&progress, index, |group| group.fallback = true);
                        break;
                    }
                    let mut closed = self.closed.subscribe();
                    tokio::select! {
                        _=closed.changed()=> {self.check_open(&progress,index)?;},
                        _=tokio::time::sleep_until(until.min(Instant::now()+Duration::from_millis(10)))=>{},
                    }
                }
            }
            update(&progress, index, |group| {
                group.phase = StartupPhase::Eligibility
            });
            if self
                .bounded(
                    index,
                    &progress,
                    self.node.startup_eligible(input.group.group_id),
                )
                .await?
            {
                self.check_open(&progress, index)?;
                self.check_budget(&progress, index)?;
                update(&progress, index, |group| {
                    group.phase = StartupPhase::Initialize
                });
                let disposition = self
                    .bounded(index, &progress, async {
                        let input_digest = progress.lock().unwrap().input_digest;
                        update(&progress, index, |group| {
                            group.initialization = InitializeDisposition::Unknown
                        });
                        self.node
                            .startup_initialize(
                                input.group.group_id,
                                &input.group.voters,
                                input_digest,
                            )
                            .await
                    })
                    .await?;
                update(&progress, index, |group| group.initialization = disposition);
            }
        }
        update(&progress, index, |group| {
            group.phase = StartupPhase::NativeWait
        });
        self.check_budget(&progress, index)?;
        let deadline = progress.lock().unwrap().groups[index].deadline.unwrap();
        self.bounded(
            index,
            &progress,
            self.node.wait_for_owned_recovery(
                input.group.group_id,
                deadline.saturating_duration_since(Instant::now()),
            ),
        )
        .await?;
        self.node
            .ensure_recovery_running(input.group.group_id)
            .map_err(|e| failure(&progress, index, RuntimeError::Source(e)))?;
        Ok(())
    }
    async fn bounded<T>(
        &self,
        index: usize,
        progress: &Progress,
        future: impl std::future::Future<Output = Result<T, MultiRaftError>>,
    ) -> Result<T, StartupFailure> {
        let mut closed = self.closed.subscribe();
        self.check_open(progress, index)?;
        self.check_budget(progress, index)?;
        let deadline = progress.lock().unwrap().groups[index].deadline.unwrap();
        tokio::select! {
            biased;
            _=closed.changed()=>Err(failure(progress,index,RuntimeError::Closed)),
            result=tokio::time::timeout_at(deadline,future)=>result.map_err(|_| failure(progress,index,RuntimeError::Deadline{phase:RuntimePhase::GroupStart,outcome_unknown:true}))?
                .map_err(|e|failure(progress,index,RuntimeError::Source(e))),
        }
    }
    fn check_open(&self, progress: &Progress, index: usize) -> Result<(), StartupFailure> {
        if !self.accepting.load(Ordering::Acquire) {
            Err(failure(progress, index, RuntimeError::Closed))
        } else {
            Ok(())
        }
    }
    fn check_budget(&self, progress: &Progress, index: usize) -> Result<(), StartupFailure> {
        let deadline = progress.lock().unwrap().groups[index].deadline;
        if deadline.is_some_and(|d| Instant::now() >= d) {
            Err(failure(
                progress,
                index,
                RuntimeError::Deadline {
                    phase: RuntimePhase::GroupStart,
                    outcome_unknown: false,
                },
            ))
        } else {
            Ok(())
        }
    }
}
fn update(progress: &Progress, index: usize, mutate: impl FnOnce(&mut GroupStartupReport)) {
    mutate(&mut progress.lock().unwrap().groups[index]);
}
fn failure(progress: &Progress, index: usize, source: RuntimeError) -> StartupFailure {
    let report = progress.lock().unwrap().clone();
    let group = &report.groups[index];
    let (id, phase) = (group.group_id, group.phase);
    report.failure(Some(id), phase, source)
}
