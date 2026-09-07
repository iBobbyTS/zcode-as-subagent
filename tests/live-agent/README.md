# zcode-as-subagent live-agent evidence matrix

The committed matrix exercises the public `zcode_subagent_*` catalog and
`build|edit|plan|yolo` permission contract. It is intentionally offline and
never invokes a real model. A missing native daemon/runtime payload is an
`EVIDENCE_GAP`, not a passing runtime claim.

The release gate also checks these explicit negative guarantees: no migration
or old aliases; no automatic download/upgrade; no provider or credential
management; no remote daemon, multi-tenant service, Windows daemon, GUI,
Rosetta, Git/worktree/base_ref/access_mode integration, or second supervisor.

This directory separates committed small-scale tests from local Git-based
Agent fixtures and disposable execution state.

- `non-git-based/` is committed. It contains the small fake-runtime, transport,
  facade and harness tests that do not require a fixture Git repository.
- `git-based/` is local and ignored. It contains complex Agent scenario source
  templates whose workspaces include independent Git repositories.
- `workspace/` is local and ignored. Every test execution must copy its source
scenario here before reset, verification, or result collection.

Source scenarios are immutable inputs. Test code must use
`non-git-based/fixture_workspace.py` to create a unique execution directory and
materialize a scenario. Results, transcripts, logs, stores, temporary Git
repositories, and imported historical evidence stay under `workspace/`.

`non-git-based/real_completion_case.py` is an explicit real-runtime evidence
flow. Run it only when the local daemon and official ZCode runtime are ready:

```sh
python3 tests/live-agent/non-git-based/real_completion_case.py
```

It performs one isolated read-only `plan` lifecycle (`spawn`, repeated `poll`,
`result`, `close`) and emits the observed task/activity/result payloads. The
harness does not classify the outcome or decide goal success; that decision is
left to the executing Agent or human. It exits non-zero only when a transport or
protocol call fails.

The runner records observable runtime facts and safety invariants; it does not
replace the task executor's or human evaluator's judgment of whether a goal was
achieved. A task that achieves its stated goal may be classified as success or
success-with-gap when bounded evidence is incomplete. `FAILED` is reserved for
an outcome that did not achieve the goal. Artifact hashes and repository
identity remain integrity evidence, not a substitute for goal judgment.

## 真实 MCP：完成后追加指令

```sh
python3 tests/live-agent/non-git-based/real_terminal_send_case.py
```

这是显式运行的真实工具调用用例，不加入默认离线矩阵。它读取用户
LaunchAgent plist 中的 socket/database/runtime，记录当前 HEAD、二进制
哈希及服务状态，使用分发的 MCP 二进制通过 stdio 调用官方 ZCode。

流程：复制 `fixtures/terminal-send` → MCP spawn 并要求真实 Read
`initial.txt` → 按 `next_revision` poll → result → 确认 COMPLETED 且未
close → 重启本用例的 MCP 接入进程 → 向同一 agent 发送读取
`followup.txt` 的消息 → 用相同 message_id 重试 → 持续观察 → result
→ 只读采集消息状态及 diagnose → close 并复查。

初始等待默认 120 秒，追加发送后的观察窗口默认 20 秒，可使用
`--timeout-sec` 和 `--observe-sec` 调整测试上限；不更改产品超时。
首次发送报错也保留重试和后续观察。原有 COMPLETED 不能当作追加任务
成功；执行者须核对新一轮真实 Read、消息交付和第二个文件的结果。
若初始任务未完成或需要审批，不能判定为终态发送复现。

每次执行在 `workspace/real-terminal-send-*` 保留 transcript、summary、
诊断和执行副本，包括失败、超时及清理结果。退出码 1 表示调用或测试
流程有错误；退出码 0 仅表示采集完成，业务验收仍需核对证据。
