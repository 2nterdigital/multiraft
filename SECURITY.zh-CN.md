# 安全策略

**English：** [SECURITY.md](SECURITY.md)

## 支持版本

| Version | Supported |
|---------|-----------|
| `main` (0.1.x) | Yes |
| Older tags | Best effort |

## 报告漏洞

请**不要**在公开 GitHub Issue 中披露未修复的安全漏洞。

1. 优先使用 GitHub **Security Advisories**：[lanpishu6300/multiraft](https://github.com/lanpishu6300/multiraft/security/advisories/new)（若可用）
2. 或私密联系：**lanpishu6300@gmail.com**，主题 `[SECURITY] multiraft`

请包含：

- 受影响 crate / 组件
- 复现步骤或 PoC（私密）
- 影响评估（鉴权绕过、DoS、数据泄露等）

我们目标在 **72 小时**内确认，并给出修复计划或时间表。

## 范围说明

- Demo Admin HTTP 与 Raft gRPC 面向实验 / 本地集群 — 若脚本默认启用且暴露到不可信网络，视为范围内。
- **Admin HTTP 无鉴权。** `/admin/*`（成员变更 promote/demote、snapshot ads）以及 `/snapshots/*/latest` 必须保持回环监听或置于已鉴权网关之后。`replicate_standby_snapshot` 是历史且已 containment 的端点，在任何 `fetch_url` 或其他拉取效果前返回类型化 unsupported（HTTP 409）。勿将这些路由端口转发到不可信网络。Snapshot SHA-256 只校验完整性，不代表拉取源可信；未来任何重新启用 `replicate_standby_snapshot` 的改动都属安全敏感。
- 2026-08-22 的 Standby containment 是 **P2 威胁边界修正**：C42 及其元数据没有证明完整的恢复所有权，因此实时 HTTP/ad/catalog/daisy 恢复为类型化 unsupported。在单独 Accepted 的完整恢复协议存在前，任何试图重新启用它的路由都应按安全敏感处理。
- 依赖 CVE：优先提交升版 PR 并附简短风险说明（尊重 openraft 精确锁定，除非刻意升版）。
