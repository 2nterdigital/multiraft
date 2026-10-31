//! Fixed replacement actions keep native executor, seed and terminal capabilities private.
use super::*;
impl<S: StateMachine> ManagedControlledGroup<S> {
    /// Bind immutable replacement arguments and acquire one native executor incarnation.
    pub async fn begin(
        &self,
        specification: ReplacementSpec,
        epoch: u64,
        target: SourceTargetPort,
        deadline: Instant,
    ) -> Result<ManagedOperation<S>, ManagedControlError> {
        let executor = self
            .request(deadline, move |group| async move {
                group
                    .begin_with_port(specification, epoch, &target, deadline)
                    .await
            })
            .await?;
        Ok(ManagedOperation {
            group: self.clone(),
            executor,
        })
    }
    /// Commit the same operation's executor claim before returning its managed incarnation.
    pub async fn claim(
        &self,
        operation: OperationId,
        epoch: u64,
        deadline: Instant,
    ) -> Result<ManagedOperation<S>, ManagedControlError> {
        let executor = self
            .request(deadline, move |group| async move {
                group.claim(operation, epoch, deadline).await
            })
            .await?;
        Ok(ManagedOperation {
            group: self.clone(),
            executor,
        })
    }
}
impl<S: StateMachine> ManagedOperation<S> {
    /// Capture actual current-operation checkpoint bytes and install through the native target port.
    pub async fn seed_target(
        &self,
        target: SourceTargetPort,
        admission: StartupAdmission,
        deadline: Instant,
    ) -> Result<(), ManagedControlError> {
        let executor = self.executor.clone();
        self.group
            .request(deadline, move |group| async move {
                let seed = group.prepare_seed(&executor, deadline).await?;
                target.install_seed(seed, admission, deadline).await
            })
            .await
    }
    /// Add the exact opened learner under the pinned executor and application barrier.
    pub async fn add_learner(
        &self,
        target: SourceTargetPort,
        deadline: Instant,
    ) -> Result<(), ManagedControlError> {
        let executor = self.executor.clone();
        self.group
            .request(deadline, move |group| async move {
                group
                    .add_learner_with_port(&executor, &target, deadline)
                    .await
            })
            .await
    }
    /// Commit the exact old-three/new-three joint membership after actual target catch-up.
    pub async fn enter_joint(
        &self,
        target: SourceTargetPort,
        deadline: Instant,
    ) -> Result<(), ManagedControlError> {
        let executor = self.executor.clone();
        self.group
            .request(deadline, move |group| async move {
                group
                    .enter_joint_with_port(&executor, &target, deadline)
                    .await
            })
            .await
    }
    /// Finalize the exact uniform target membership without implying terminal completion.
    pub async fn finalize_uniform(&self, deadline: Instant) -> Result<(), ManagedControlError> {
        let executor = self.executor.clone();
        self.group
            .request(deadline, move |group| async move {
                group.finalize_uniform(&executor, deadline).await
            })
            .await
    }
    /// Verify durable terminal application and return facts without granting retirement authority.
    pub async fn complete(
        &self,
        target: SourceTargetPort,
        deadline: Instant,
    ) -> Result<TerminalProof, ManagedControlError> {
        let executor = self.executor.clone();
        self.group
            .request(deadline, move |group| async move {
                Ok(group
                    .complete_with_port(&executor, &target, deadline)
                    .await?
                    .proof()
                    .clone())
            })
            .await
    }
}
