# MCP 工具真实调用流程

这份流程用于以后由 Agent 重复执行公开的九个 `zcode_subagent_*` 工具。它有两条必须都通过的路径：

1. 直接调用 `zcode-as-subagent` CLI（daemon RPC 的公开 CLI 映射）。
2. 启动新的 `codex exec` 会话，由 Codex 通过 `zcode_as_subagent` MCP 调用同一工具。

## 前置条件

在项目根目录执行：

```sh
zcode-as-subagent init
zcode-as-subagent install-mcp codex
zcode-as-subagent status
```

记录 `target/release` 与 `npm/native/darwin-arm64` 两个 native binary 的 SHA-256，并确认 daemon socket 与固定的 ZCode runtime 可用。真实调用使用专用测试仓库；不要把项目当前工作区作为 build/edit/yolo 的写入目标。

## 公共输入和生命周期

每次 `spawn` 保存返回的 `agent_id`。`write_manifest` 可省略；省略时 build/edit/yolo 使用受保护的工作区范围，也可传仓库相对路径例如 `write_manifest=["src"]` 缩小范围。成功后循环 `poll`，把返回的 `next_revision` 作为下一次 `after_revision`；遇到 pending permission request 只用 `respond` 回复。进入 `TERMINAL` 后调用 `result`，最后调用 `close`。

CLI 请求形状如下（`<method>` 替换为表格中的方法，JSON 从 stdin 或 `--json` 传入）：

```sh
zcode-as-subagent <method> --json '<json-object>'
```

MCP 请求由新 Codex 会话执行：

```sh
codex exec --dangerously-bypass-approvals-and-sandbox --json \
  '只调用 zcode_as_subagent 的 <tool>，使用下面 JSON 参数，并原样返回工具结果。'
```

每条测试都必须保存 CLI 输出、Codex JSONL 输出、binary hash、daemon PID、时间戳和测试仓库前后 Git 状态。不得把 token、完整 prompt 或模型原始推理写入报告。

## 工具覆盖矩阵

| 工具 | 直接 CLI 调用 | Codex MCP 调用 | 通过条件 |
|---|---|---|---|
| `zcode_subagent_status` | `status`，参数 `{}` | 同名工具，`{}` | 返回协议版本、generation，daemon/store/scheduler 为 `READY` |
| `zcode_subagent_spawn` | `spawn`，plan 或带 `write_manifest` 的写模式 | 同名工具 | 返回新 `agent_id`、`submission_disposition=created`、非空 phase |
| `zcode_subagent_poll` | `poll`，`agent_id`、`after_revision`、`timeout_ms<=5000` | 同名工具 | 返回 `next_revision`；可重复长轮询且不丢 pending request |
| `zcode_subagent_list` | `list`，必须提供 `repository`、`phase` 或 `outcome` 范围 | 同名工具 | 只返回范围内任务，cursor 可继续且 limit 有界 |
| `zcode_subagent_send` | `send`，已存在 agent、唯一 `message_id`、非空 content | 同名工具 | 首次 queued/delivered，重复 message_id 为 already_delivered |
| `zcode_subagent_respond` | `respond`，真实 pending request 的 `request_id` 与 allow/deny | 同名工具 | 首次 responded，重复请求幂等，策略覆盖字段准确 |
| `zcode_subagent_cancel` | `cancel`，运行中的 `agent_id` | 同名工具 | 返回 cancel_requested，随后 poll/result 为 CANCELLED 或既定终态 |
| `zcode_subagent_result` | `result`，已终态 agent | 同名工具 | 返回 outcome、final_text、partial；未终态应明确失败 |
| `zcode_subagent_close` | `close`，已完成或取消的 agent | 同名工具 | 返回 closed/resources_reaped；重复调用保持幂等 |

## Case 3 长运行探测流程

Case 3 不再使用“spawn 后立即 cancel”的短路径作为主要验证。对 CLI 和 Codex MCP 各执行一条独立任务：

1. `status`、`list`。
2. `spawn` Case 3，使用 `build` 和 `write_manifest=["src"]`。
3. 持续 `poll`，直到至少一次返回非空 `latest_text_tail`，并且任务已经进入 `RUNNING`；保存每次的 revision、activity 和时间戳。
4. 在仍运行时执行 `list` 和 `send`。`send` 使用唯一 `message_id`，随后用相同 message_id 重复一次，验证幂等结果。
5. 不调用 `respond`，因为没有真实 pending request 时不能伪造 request_id；若 poll 出现 pending request，记录为“未执行 respond，待专门测试”。
6. 立即执行 `cancel`，继续 `poll` 到 `TERMINAL/CANCELLED`，再执行 `result` 和 `close`。
7. 取消后至少观察 10 秒：检查任务对应 runtime 是否仍存在，并查询 `~/.zcode/cli/db/db.sqlite` 的 `model_usage`，确认没有继续新增 token 记录。

每条路径必须记录 `spawn_at`、首次文本时间、`send_at`、重复 send 结果、`cancel_at`、terminal 时间、最后一条 `completed_at` 和 token 差值。若任务在获得文本前就终止，该路径标记为 `TEXT_NOT_OBSERVED`，不能当作长运行测试通过。

## 独立 Respond Case（edit）

使用 `tests/live-agent/non-git-based/respond_case.py` 执行专门的权限请求测试。脚本只使用 `permission_mode=edit` 和 `write_manifest=["src"]`，分别启动 allow 与 deny 两条独立任务；每条任务都必须在 `poll` 观察到真实的 pending、首次 `respond`，再用完全相同的 `request_id` 重复响应验证幂等，最后 `result → close`。没有观察到真实 pending 时脚本失败，不允许伪造 request_id。

```sh
python3 tests/live-agent/non-git-based/respond_case.py --transport cli --repository <absolute-fixture-repo>
python3 tests/live-agent/non-git-based/respond_case.py --transport mcp --repository <absolute-fixture-repo>
```

## 推荐执行顺序

先分别执行 `status`、`list`。创建一个 `plan` 任务验证 `spawn → poll → result → close`；创建一个短生命周期写模式任务验证 `send`、pending `respond` 和重复响应；再创建一个可取消任务验证 `cancel → poll → result → close`。`list` 在每个阶段执行一次，核对终态过滤和 cursor。每个步骤都在两条路径各执行一次，不能用 CLI 结果替代 Codex MCP 结果。

## 失败处理

记录第一层完整错误。`validation: request validation failed` 只表示 facade 校验失败；若带有 scheduler/preparation 详情，按详情定位仓库、快照、权限或持久化问题。没有 `agent_id` 时禁止调用 poll/result。修复后必须重建 native binary、验证 hash、重启 daemon，并从失败工具重新开始整条生命周期。
