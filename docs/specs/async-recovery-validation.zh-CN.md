# 异步恢复校验

consumer 可以校验外部恢复证明，而不把网络等待放入确定性 `apply`。库不依赖具体归档或数据库类型；OpenRaft 仍按 workspace 中的精确 revision 固定。

## consumer 合同

需要外部恢复证明的 FSM 实现 `StateMachine::requires_recovery_validation()`，返回 `true`。`recovery_validation(context)` 在短暂 FSM 锁内同步冻结有界、独立拥有的输入，返回可选 `ValidationFuture`；库释放锁后才轮询 future。冻结阶段不能执行网络 IO 或创建脱离 owner 的任务。future 拥有连接、请求和临时资源；丢弃 future 必须取消或释放这些工作。`None` 表示这个具体状态无需外部工作，会跳过外部 validator 准入，即使 consumer 提供的预算正被占用。

`ValidationContext` 包含 Group、应用状态代次、applied `(index, term)` 和来源 `Startup` / `PeerInstall`。consumer 必须冻结当前状态的证明输入，不能在异步等待后再读取可变 FSM。`recovery_validated` 在证明成功后设置同步、本地 readiness 标记，不能等待外部 IO；consumer restore 必要时清除自己的标记。

例如归档 consumer 可以只冻结有界归档水位与证明摘要，在既有 `StateMachine` 实现中加入：

```rust,ignore
fn requires_recovery_validation(&self) -> bool { true }

fn recovery_validation(
    &self,
    context: multiraft_fsm::ValidationContext,
) -> Option<multiraft_fsm::ValidationFuture> {
    // 小型不可变证明请求，不复制整个归档。
    let request = self.archive_proof_request(context);
    let archive = self.archive.clone();
    Some(Box::pin(async move {
        // 请求可取消，并拥有全部远端资源。
        archive.verify(request).await
    }))
}

fn recovery_validated(
    &mut self,
    context: multiraft_fsm::ValidationContext,
) -> Result<(), Self::Error> {
    self.ready_generation = Some(context.generation);
    Ok(())
}
```

`StateMachineFactory::validation_timeout(context)` 设置正数相对 deadline，默认 30 秒。一个 node 的 Groups 默认共享一个外部 validator 准入槽。consumer 可通过 `StateMachineFactory::validation_budget()` 返回自己拥有的共享 `Arc<Semaphore>`；各次调用应返回同一个 semaphore。deadline 同时覆盖准入等待和 future 执行。直接使用 `StateMachineStore` 时，在 clone/register 前配置 `ValidationOptions { deadline, budget: Arc<Semaphore> }`。既有 snapshot capture/send/receive 准入和字节限制继续生效，校验不提高 64 MiB 上限。

## 启动与同伴安装

owned node 启动调用 `begin_recovery_validation`，加载已有 active provider，回放 committed suffix。加载完全匹配的既有合法 active 状态不是激活新同伴候选，不执行 peer proof。随后 `validate_recovery` 冻结实际恢复所得状态代次，异步校验证明，核对代次与关闭状态，执行既有本地 `StateMachineFactory::validate_recovered`，在该同步回调返回后再次核对代次与 owner 状态，最后发布 readiness hook，才准入 Group。

启用校验的底层 `MultiRaft` consumer 先等待 `wait_for_recovery`，再调用 `validate_recovered(group)`，之后才访问业务。proposal 方法在应用 ready 前门控业务派发，同时继续允许 native committed suffix 回放。

直接使用 store 的 consumer 必须在加载/回放前调用 `begin_recovery_validation`，在 committed 回放后调用 `validate_recovery`。恢复阶段允许 native replay，业务读取与新捕获被门控。`try_with_fsm` 对未校验状态返回错误；旧便捷方法 `with_fsm` 要求调用者先保证 ready，否则 panic。

运行中 native peer install 与 apply、另一 install 串行。它暂存 durable bytes，在业务 read/capture 被门控时恢复候选，冻结证明输入，释放 FSM 锁后等待证明，并在激活前核对代次与 owner 关闭状态。成功安装先 durable 激活 provider，再发布 readiness hook、更新 applied/membership，最后返回成功。等待中的应用输入不能改变候选或借用旧证明。

## 失败、取消与资源 owner

拒绝、超时、panic、调用者取消、owner 关闭都不能发布未验证候选。失败 transition 使应用访问进入 fencing，直到销毁/重启。外部证明失败时，既有 active durable provider 与诊断事实保留。库不会通过第二次不可信 restore 尝试回滚已经改变的应用。重启从既有 active provider 与 committed log 恢复。

一个 catalog ownership domain 的部分构造写入与拥有 owner 的 stage handle 合计最多 16 个，同一 generation 的重复 handle 也计入预算。同一 root 的 clone 与独立重新打开的 catalog 共享 ownership 和该上限。启动最多扫描一个 Group native namespace 中的 64 项。只有完整 namespace 与既有 active authority 全部验证成功，才能清理已完全验证、inactive 且没有 owner 的 generation。未知项、不完整 generation 与 `.stage-*` 目录保留，并使恢复失败隔离；不能把它们当成 abandoned work 静默删除。扫描失败时保留诊断字节，不进行 pruning。没有 active authority 时也保留全部代文件，供原生 coverage 检查和精确修复恢复依据；namespace 预算仍生效。

inactive staged generation 有拥有清理职责的 guard。最后一个 owner 丢弃时尝试 durable 删除候选；active generation 和仍有其他 handle 的 generation 受保护。直接调用者可通过 `NativeSnapshotStage::discard()` 获得清理 IO 错误。Drop 记录错误和 cleanup debt；该 debt 阻止该 Group 新 staging，直到同一 Group 完整 provenance scan 验证既有合法 active authority 后才清除，避免重复清理失败继续准入无界候选。

validation permit 随 future 完成/取消释放。native staging/activation 的 blocking work 自己拥有 permit，即使调用者取消仍须等待它收尾。`close_native_intake` 发出取消信号，`wait_native_quiescent` join transition 与 native blocking work。owned runtime 的 `begin_cleanup` 立即停止新准入，并对 pending recovery/install 调用 `cancel_pending_validation`；ready generation 已准入的业务请求继续 drain。清理还等待真正的应用析构并释放 listener。consumer 不能用 `block_on` 或脱离 owner 的任务绕过生命周期。

既有 FSM 默认无需恢复校验、无外部 future、readiness hook 接受。bounded snapshot、durable coverage、purge、`CompletedObserved` 合同保持。外部校验失败不能制造安装完成或新的 purge coverage。

## 验证

`multiraft-store/tests/async_validation.rs` 覆盖成功、read/capture/apply 隔离、不同代次并发安装、拒绝/deadline/调用者 abort/owner close、准入超时、既有 provider 保留与重复失败清理。validator 拥有真实临时文件 lease，从外部观察资源回收。

`multiraft-net/tests/async_recovery_validation.rs` 使用既有 `legacy-native-alpha30` fixture，校验 manifest hash 与来源提交，证明启动只对 snapshot＋committed suffix 实际状态执行一次校验，覆盖拒绝、超时、启动取消和 dynamic Group 校验期间 owned shutdown。通用库测试不证明特定数据库事务协议；consumer 仍须独立验证真实外部证明实现。
