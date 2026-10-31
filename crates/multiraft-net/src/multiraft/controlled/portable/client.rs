//! Source validation of encoded replies, plus bounded source-owned seed delivery.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

fn correlation() -> [u8; 16] {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let mut id = [0; 16];
    id[..8].copy_from_slice(&u64::from(std::process::id()).to_be_bytes());
    id[8..].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    id
}
impl SourceTargetPort {
    async fn request(
        &self,
        scope: NativeTargetScope,
        command: NativeTargetCommand,
    ) -> Result<NativeTargetReply, ControlledError> {
        let request = NativeTargetRequest {
            scope: scope.clone(),
            command: command.clone(),
        };
        let bytes = serde_json::to_vec(&request).map_err(std::io::Error::other)?;
        let bytes = self.transport.exchange(bytes).await.map_err(|error| {
            ControlledError::TargetOutcome {
                outcome: error.outcome,
                message: error.message,
            }
        })?;
        if bytes.len() > 6 * multiraft_store::disk_snapshot::MAX_MANIFEST_BYTES {
            return Err(ControlledError::Rejected(
                "native target reply exceeds bound",
            ));
        }
        let reply: NativeTargetReply =
            serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        // Comparing the encoded command also binds chunk sequence/content and the full barrier.
        if reply.scope != scope
            || serde_json::to_value(&reply.command).map_err(std::io::Error::other)?
                != serde_json::to_value(&command).map_err(std::io::Error::other)?
        {
            return Err(ControlledError::Rejected(
                "native target reply scope or command mismatch",
            ));
        }
        match reply.outcome {
            NativeTargetOutcome::Durable | NativeTargetOutcome::InProgress => Ok(reply),
            _ => Err(ControlledError::TargetOutcome {
                outcome: reply.outcome,
                message: reply
                    .error
                    .unwrap_or_else(|| "no durable result".to_owned()),
            }),
        }
    }
    pub(in crate::multiraft::controlled) async fn observe(
        &self,
        configuration: Option<StoredMembership>,
        barrier: Option<LogId>,
    ) -> Result<NativeTargetObservation, ControlledError> {
        let reply = self
            .request(
                self.scope(correlation()),
                NativeTargetCommand::Observe {
                    configuration: configuration.clone(),
                    barrier,
                },
            )
            .await?;
        if reply.outcome != NativeTargetOutcome::Durable {
            return Err(ControlledError::TargetOutcome {
                outcome: reply.outcome,
                message: "target observation is not durable".to_owned(),
            });
        }
        let observation = reply
            .observation
            .ok_or(ControlledError::Rejected("target observation missing"))?;
        if observation.stage == NativeTargetStage::Opened
            && observation
                .record
                .as_ref()
                .is_none_or(|record| record.specification != self.specification)
        {
            return Err(ControlledError::Rejected(
                "opened target operation differs from source",
            ));
        }
        if let Some(configuration) = configuration {
            if observation.stage != NativeTargetStage::Opened
                || observation.configuration.as_ref() != Some(&configuration)
                || observation.applied.is_none_or(|applied| {
                    barrier.is_some_and(|floor| !super::super::seed::covers(applied, floor))
                })
                || observation
                    .record
                    .as_ref()
                    .is_none_or(|record| record.specification != self.specification)
            {
                return Err(ControlledError::Rejected(
                    "target application evidence mismatch",
                ));
            }
        }
        Ok(observation)
    }
    pub(in crate::multiraft::controlled) async fn receipt_at(
        &self,
        configuration: &StoredMembership,
    ) -> Result<TargetOpenReceipt, ControlledError> {
        let barrier = *configuration.log_id();
        let observation = self.observe(Some(configuration.clone()), barrier).await?;
        Ok(TargetOpenReceipt {
            operation: self.specification.operation,
            target_node: self.identity.node,
            generation: self.identity.generation,
            applied: observation
                .applied
                .ok_or(ControlledError::Rejected("target applied log missing"))?,
            receipt: self.identity.consumption.clone(),
        })
    }
    /// Deliver an actual source checkpoint with bounded chunks. Source work survives waiter loss.
    /// Retry/status after uncertain delivery uses the same immutable operation and target identity.
    pub async fn install_seed(
        &self,
        seed: ControlledSeed,
        admission: StartupAdmission,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        if seed.specification != self.specification || seed.fingerprint != self.identity.domain {
            return Err(ControlledError::Rejected(
                "seed and portable target scope differ",
            ));
        }
        let port = self.clone();
        let scope = self.scope(correlation());
        let header = NativeSeedHeader {
            record: seed.record.clone(),
            vote: seed.vote,
            meta: seed.meta.clone(),
            manifest: seed.manifest.clone(),
        };
        // Owned transfer registration precedes transmission; the caller owns only its waiter.
        let input = seed.input.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // The export's registry owns the transfer continuation too, rather than a detached task.
        input.run(async move {
            let result = async {
                let start = port
                    .request(
                        scope.clone(),
                        NativeTargetCommand::StartSeed { header, admission },
                    )
                    .await?;
                if start.outcome == NativeTargetOutcome::Durable {
                    port.validate_install_reply(&start)?;
                    return Ok(());
                }
                let mut chunks = seed.input.open(seed.manifest, port.identity.group).await?;
                let mut sequence = 0;
                loop {
                    let chunk = chunks
                        .recv()
                        .await
                        .ok_or(ControlledError::Rejected("source stream incomplete"))?
                        .map_err(ControlledError::Native)?;
                    let done = chunk.is_none();
                    port.request(
                        scope.clone(),
                        NativeTargetCommand::SeedChunk { sequence, chunk },
                    )
                    .await?;
                    sequence += 1;
                    if done {
                        break;
                    }
                }
                let finish = port.request(scope, NativeTargetCommand::FinishSeed).await?;
                if finish.outcome != NativeTargetOutcome::Durable {
                    return Err(ControlledError::Rejected("target seed not durably opened"));
                }
                port.validate_install_reply(&finish)?;
                Ok(())
            }
            .await;
            let _ = sender.send(result);
        })?;
        tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| ControlledError::Deadline {
                outcome_unknown: true,
            })?
            .map_err(|_| ControlledError::Closed)?
    }
    fn validate_install_reply(&self, reply: &NativeTargetReply) -> Result<(), ControlledError> {
        if reply.observation.as_ref().is_none_or(|observation| {
            observation.stage != NativeTargetStage::Opened
                || observation
                    .record
                    .as_ref()
                    .is_none_or(|record| record.specification != self.specification)
                || observation.applied.is_none()
                || observation.configuration.is_none()
        }) {
            return Err(ControlledError::Rejected(
                "durable seed reply lacks actual opened operation",
            ));
        }
        Ok(())
    }
    /// Read persistent native target status after a lost install reply; never restarts construction.
    pub async fn status(&self) -> Result<NativeTargetObservation, ControlledError> {
        self.observe(None, None).await
    }
}
