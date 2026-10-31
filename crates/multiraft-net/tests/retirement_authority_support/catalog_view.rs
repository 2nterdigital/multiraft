//! Controlled corrupt/unavailable reads of the trusted fixture catalog; mutations are forbidden.
use super::{authority::*, catalog::FileAuthority};
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

pub struct CatalogView {
    pub backend: Arc<FileAuthority>,
    pub mode: AtomicU8,
    pub writes: AtomicUsize,
    pub source: u64,
    pub generation: u64,
}
impl CatalogView {
    fn mutation<T>(
        &self,
    ) -> StartupAuthorityFuture<'_, Result<StartupAuthorityResult<T>, StartupAuthorityError>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(StartupAuthorityError(
                "permission query attempted catalog mutation".to_owned(),
            ))
        })
    }
}
impl StartupAuthority for CatalogView {
    fn admit(
        &self,
        _: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    > {
        self.mutation()
    }
    fn bind_active(
        &self,
        _: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    > {
        self.mutation()
    }
    fn terminal_handoff(
        &self,
        _: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    > {
        self.mutation()
    }
    fn read(
        &self,
        group: u64,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>> {
        Box::pin(async move {
            let mode = self.mode.load(Ordering::SeqCst);
            if mode == 8 {
                return Err(StartupAuthorityError("catalog unavailable".to_owned()));
            }
            if mode == 9 {
                return std::future::pending().await;
            }
            let mut catalog = self.backend.read(group).await?;
            match mode {
                1 => catalog.completed.clear(),
                2 => {
                    catalog.authorized.insert(self.source, self.generation);
                }
                3 => {
                    let proof = &catalog.completed[0];
                    catalog.active_operation = Some(StartupBinding {
                        group_id: group,
                        specification: proof.native_record.specification.clone(),
                        source: self.source,
                        source_generation: self.generation,
                        target: proof.target_opened.clone(),
                    });
                }
                4 => catalog.completed[0].applied_log.index += 1,
                5 => catalog.completed[0].source_generation += 1,
                6 => {
                    let completed = &mut catalog.completed[0];
                    let proof = completed.native_record.terminal.as_mut().unwrap();
                    proof.barrier = proof.target_open.applied;
                    completed.applied_log = proof.barrier;
                }
                7 => catalog.group_id += 1,
                10 => catalog.completed[0].target_opened.consumption_id.push(9),
                11 => {
                    catalog.completed[0].applied_membership = catalog.completed[0]
                        .native_record
                        .specification
                        .expected_start
                        .clone()
                }
                13 => {
                    catalog.membership = catalog.completed[0]
                        .native_record
                        .specification
                        .expected_start
                        .clone()
                }
                14 => {
                    let proof = &catalog.completed[0];
                    catalog.active_operation = Some(StartupBinding {
                        group_id: group,
                        specification: proof.native_record.specification.clone(),
                        source: proof.target_opened.node_id,
                        source_generation: proof.target_opened.generation,
                        target: StartupTargetGeneration {
                            node_id: self.source,
                            generation: self.generation,
                            consumption_id: vec![77],
                        },
                    });
                }
                12 => {
                    let duplicate = catalog.completed[0].clone();
                    catalog.completed.push(duplicate);
                }
                _ => {}
            }
            Ok(catalog)
        })
    }
}
