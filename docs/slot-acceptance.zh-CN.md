# 固定 Slot 本机验收

`multiraft-slot-demo` 是 `multiraft-slot` 的测试消费者，使用新数据、不可变布局、
RocksDB 日志及 NativeDurable FSM 快照。计数器和 command-id 表用于证明精确
 effects、业务 key 身份校验与确定性去重；不实现生产 Admit、TiKV、消息顺序协议，
也不认证容量。

在仓库根目录运行：

```sh
mkdir -p .tmp/slot-foundation/tmp
env TMPDIR="$PWD/.tmp/slot-foundation/tmp" \
  CARGO_TARGET_DIR="$PWD/.tmp/slot-foundation/target" \
  cargo build -p multiraft-slot-demo
python3 scripts/slot_acceptance.py
```

macOS 从源码构建 RocksDB 时可能需要将 `LIBCLANG_PATH`、`DYLD_LIBRARY_PATH`
设为 Xcode toolchain 的 `usr/lib`。`--binary PATH` 指定已构建二进制；
`--output-dir PATH` 必须是尚不存在的目录，默认在 `.tmp/slot-foundation/`
创建唯一目录。`--case normal`、`--case recovery` 或 `--case fingerprint` 可单独运行独立场景集。

Python 标准库脚本通过 `Popen` 启动三个真实 OS 进程，分别持有不同 PID、
loopback Raft／应用 RPC／管理端口和数据目录。候选端口同时保留，启动前释放；
若本机其他进程抢占端口，验收明确失败。仅清理本次创建的进程组，先 SIGTERM，
超时才 SIGKILL；切主场景明确使用 SIGKILL。SIGINT／SIGTERM 处理器只记录首个信号，HTTP（socket timeout 最多 10 秒）、poll
和阶段检查点执行协作式停止；KeyboardInterrupt 同样进入必达清理并写入失败收据。
清理期间继续使用不抛异常的处理器，重复信号不会打断清理。最终收据提交为完成点；
子进程回收与证据关闭后，仅在恢复两项原处理器时同时屏蔽 SIGINT／SIGTERM，随后
恢复原 mask，此后信号按原处理器语义处理；新子进程不会继承该 mask。不能拦截 SIGKILL。正常关闭必须 exit 0，demo 同时要求
owner 完成关闭且无剩余请求。脚本不访问实验室、SSH 或其他进程。

验收逐项断言：

- 三节点相同 manifest、RocksDB／NativeDurable、本地多个固定 Slot、follower
  入口转发、精确 effects 和线性读。
- 相同 command-id／intent 重试返回原 effects；修改 intent 得到业务拒绝，值不变。
- leader 上测试专用、有上限的 Condvar apply gate；明确观察 entered 后，检查
  caller Unknown、Slot inflight 仍占用、同 Slot Busy、其他 Slot 继续完成；释放后
  核对恰好一次 apply，无自动重提交。
- 真实 public HTTP socket 提交 gated write，独立 status 确认 entered 后才关闭／RST，
  不读取响应。后台仍占 Slot 额度，释放 gate 后只增加一次业务值，显式相同 ID 重试
  返回原 effects。原始 socket 事件记录进入与断连先后，不称 caller 收到了 Unknown。
- 真实杀死 leader，剩余 quorum 选主并写入，原端口／原数据重启恢复。
- compaction 返回提交后继续轮询每节点每 Slot 的 durable snapshot、purge 和
  CompletedObserved；追加 committed suffix；全部进程关闭／重启后核对值及去重结果。
- 磁盘 manifest 身份错配启动拒绝，原数据保留且可用原布局重新打开。
- 独立新数据 RF3 场景：两个 digest A 节点加一个 digest B 节点。A+A 可提交，
  只剩 A+B 时写不能 Applied，B 未应用 A 的值。恢复 A quorum 后显式用相同
  command-id 重试消歧；Unknown 不能解释为“未提交”。

仅 `--test-controls` 开启 `/gate`。gate 有 45 秒保护上限，管理线程可独立释放；
本测试服务不应暴露为生产服务。`/compact` 只代表提交，`/status` 是本地瞬时事实，
不代表 quorum readiness。本地 `/read` 保留 `result` 字段，并增加与值在同一 FSM
锁内采集的 `applied`（term、node_id、index，或 null）。

所有 HTTP 请求、原始响应／传输错误写入 `requests.jsonl`。布局、每次启动日志、
数据和 `receipt.json` 在成功或失败时均保留。收据包括 PID、完整启动命令、通过
场景、清理结果和失败原因。只有选定场景及清理全部成功才 exit 0；这些是本次
本机正确性证据，不证明吞吐、延迟或生产恢复认证。
